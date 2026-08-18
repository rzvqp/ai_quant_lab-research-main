"""`N1IncrementalDualClockLoop` -- the final wiring (CEO directive 2026-08-18, RT-N1-0003 `f33e739`):
N1 incremental hydration (isolated `.alpha_n1_venv` subprocess) -> M15 context-refresh -> M5 decision loop
(real Tower N2->N3->N4 -> Router->Eligibility->EV->N6 -> Risk -> broker gate BLOCKED), behind an explicit,
DEFAULT-OFF feature flag.

**Flag semantics, read fresh at every `main()` call, never cached at import time**: `resolve_runtime_mode`
reads `AI_TRADER_RUNTIME_MODE` from the environment. Anything other than the literal string
`"N1_INCREMENTAL_DUAL_CLOCK"` -- unset, empty, misspelled, or any value this module doesn't recognize --
resolves to `"LEGACY_M15"`, fail-closed. `entrypoint.main()`'s own existing body is UNCHANGED; the only
edit there is a single `if` at the very top that calls into `main_incremental_dual_clock()` and returns
when the flag is on, so an accidental Scheduled-Task restart with no explicit env var override always
reaches the exact same legacy code path that has been running since `65798b4`.

**No legacy fallback, anywhere in this file.** Every failure mode in the CEO's own matrix (worker dead/
timeout, corrupt/mismatched snapshot, MT5 unavailable, insufficient history, future/stale context,
restart mid M15/M5, invalid JSON, NaN/Inf, duplicate bar, tower unavailable) resolves to NO_TRADE/
UNAVAILABLE through the ALREADY-TESTED fail-closed paths in `n1_incremental`/`dual_clock` -- this file
composes them, it does not invent new resilience or a path back to `RawAxesBuilder`/legacy telemetry."""

from __future__ import annotations

import dataclasses
import os
import signal
import time
from pathlib import Path
from typing import Callable

from ai_trader.live_signal_source.bar_feed import LiveBarFeed, make_broker_offset
from ai_trader.mandate2_readiness.broker_gate import BrokerOrderSubmissionGate
from ai_trader.mt5_pnl_source.gateway import RealMT5HistoryGateway
from ai_trader.new_brain_bridge.authority import DecisionAuthority, current_authority
from ai_trader.new_brain_bridge.bridge import TowerDependencies
from ai_trader.new_brain_bridge.telemetry import NewBrainTelemetryLog
from ai_trader.new_brain_bridge.tower_client import TowerClient, TowerClientConfig
from ai_trader.new_brain_bridge.tower_launcher import EstablishedSession, TowerWorkerLauncher
from ai_trader.new_brain_live.deps import NewBrainLiveDepsFactory
from ai_trader.new_brain_live.dual_clock.m5_decision_loop import M5DecisionLoop
from ai_trader.new_brain_live.dual_clock.upstream_context import UpstreamContextStore
from ai_trader.new_brain_live.heartbeat import HeartbeatWriter, LiveShadowHeartbeat
from ai_trader.new_brain_live.live_shadow_journal import LiveShadowJournal
from ai_trader.new_brain_live.n1_incremental import artifact_pin
from ai_trader.new_brain_live.n1_incremental.client import N1IncrementalClient
from ai_trader.new_brain_live.n1_incremental.context_refresh_loop_incremental import IncrementalContextRefreshLoop
from ai_trader.new_brain_live.n1_incremental.hydrate import hydrate_n1_incremental
from ai_trader.new_brain_live.n1_incremental.snapshot_store import N1IncrementalSnapshotStore
from ai_trader.new_brain_live.singleton import AlreadyRunningError, SingletonLock
from ai_trader.persistent_state.store import SqliteStateStore
from ai_trader.risk_manager.types import EngineState
from ai_trader.risk_manager_live.circuit_breaker import load_persisted_circuit_state

RUNTIME_MODE_ENV_VAR = "AI_TRADER_RUNTIME_MODE"
RUNTIME_MODE_LEGACY = "LEGACY_M15"
RUNTIME_MODE_INCREMENTAL = "N1_INCREMENTAL_DUAL_CLOCK"

SYMBOL = "XAUUSD"
MT5_TIMEFRAME_M15 = 15
MT5_TIMEFRAME_M5 = 5
BAR_SECONDS_M15 = 15 * 60
BAR_SECONDS_M5 = 5 * 60
POLL_INTERVAL_SECONDS = 30.0
COLD_START_BAR_COUNT_MINIMUM = 6000
"""CEO section 2: "citește din MT5 suficiente bare M15 închise pentru cold rebuild -- minimum 6000" --
enforced explicitly below (`hydrate_n1_incremental`'s own default already matches, this is the pinned
floor a future change to that default may never silently drop below for this runtime mode)."""
_M15_CONTEXT_WATERMARK_SUFFIX = "n1_incremental_context"
_HYDRATION_MAX_ATTEMPTS = 3
_HYDRATION_RETRY_DELAY_SECONDS = 5.0

TOWER_VENV_PYTHON = Path("C:/Users/MEDION GAMING/ve_tower_venv/Scripts/python.exe")


def resolve_runtime_mode(env: dict[str, str] | None = None) -> str:
    """Fail-closed to `LEGACY_M15` for ANY value other than the exact literal `N1_INCREMENTAL_DUAL_
    CLOCK` -- unset, empty, a typo, or a future mode name this version doesn't know about. Never raises."""
    source = os.environ if env is None else env
    raw = source.get(RUNTIME_MODE_ENV_VAR, RUNTIME_MODE_LEGACY)
    return raw if raw == RUNTIME_MODE_INCREMENTAL else RUNTIME_MODE_LEGACY


def n1_version_pin_label() -> str:
    return f"{artifact_pin.PINNED_VERSION}@{artifact_pin.PINNED_WHEEL_SHA256[:8]}"


def _default_sleep(seconds: float) -> None:
    time.sleep(seconds)


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class HydrationOutcome:
    attempted: int
    succeeded: bool
    last_rejection_reason: str | None


def hydrate_with_controlled_retry(
    *, symbol: str, gateway: RealMT5HistoryGateway, state_store: SqliteStateStore,
    context_store: UpstreamContextStore, client: N1IncrementalClient,
    max_attempts: int = _HYDRATION_MAX_ATTEMPTS, retry_delay_seconds: float = _HYDRATION_RETRY_DELAY_SECONDS,
    sleep: Callable[[float], None] = _default_sleep,
) -> HydrationOutcome:
    """CEO section 2: "cold rebuild imposibil -> NO_TRADE și retry controlat, fără context incomplet."
    Never records a partial context: `hydrate_n1_incremental` itself only ever records a context on a
    genuinely complete, non-rejected result -- this wrapper's only job is bounding the retry, not
    weakening that guarantee. If every attempt fails, the context store is left exactly as it started
    (empty, or whatever a prior successful hydration already left there) -- `M5DecisionLoop` already
    treats a missing/stale context as `CONTEXT_STALE` -> NO_TRADE, so no special-case handling is needed
    downstream."""
    last_reason: str | None = None
    for attempt in range(1, max_attempts + 1):
        result = hydrate_n1_incremental(
            symbol=symbol, gateway=gateway, state_store=state_store, context_store=context_store,
            client=client, cold_start_bar_count=COLD_START_BAR_COUNT_MINIMUM,
        )
        if result.context_recorded:
            return HydrationOutcome(attempted=attempt, succeeded=True, last_rejection_reason=None)
        last_reason = result.rejection_reason
        print(
            f"new_brain_live.n1_incremental: hydration attempt {attempt}/{max_attempts} failed "
            f"({last_reason}) -- {'retrying' if attempt < max_attempts else 'giving up, NO_TRADE until next tick'}",
            flush=True,
        )
        if attempt < max_attempts:
            sleep(retry_delay_seconds)
    return HydrationOutcome(attempted=max_attempts, succeeded=False, last_rejection_reason=last_reason)


class N1IncrementalDualClockLoop:
    def __init__(
        self, *, context_refresh: IncrementalContextRefreshLoop, m5_loop: M5DecisionLoop,
        state_store: SqliteStateStore, context_store: UpstreamContextStore,
        snapshot_store: N1IncrementalSnapshotStore, tower: TowerDependencies,
        deps_factory: NewBrainLiveDepsFactory, gate: BrokerOrderSubmissionGate,
        shadow_journal: LiveShadowJournal,
        heartbeat_writer: HeartbeatWriter | None = None, gateway: RealMT5HistoryGateway | None = None,
        authority_check: Callable[[], DecisionAuthority] | None = None,
        poll_interval_seconds: float = POLL_INTERVAL_SECONDS,
    ) -> None:
        if poll_interval_seconds <= 0:
            raise ValueError(f"N1IncrementalDualClockLoop: poll_interval_seconds must be > 0, got {poll_interval_seconds!r}")
        self._context_refresh = context_refresh
        self._m5_loop = m5_loop
        self._state_store = state_store
        self._context_store = context_store
        self._snapshot_store = snapshot_store
        self._tower = tower
        self._deps_factory = deps_factory
        self._gate = gate
        self._shadow_journal = shadow_journal
        """The SAME `LiveShadowJournal` instance passed to `m5_loop`'s own construction -- read here only
        for heartbeat diagnostics (`last_market_event_id`/`last_journal_sequence`/`last_outcome_reason`),
        never written to directly by this class."""
        self._heartbeat_writer = heartbeat_writer
        self._gateway = gateway
        self._authority_check = authority_check
        self._poll_interval_seconds = poll_interval_seconds
        self._stop_requested = False
        self._pid = os.getpid()
        self._process_start_identity = f"{self._pid}:{int(time.time())}"
        from ai_trader.new_brain_live.entrypoint import current_git_commit  # local import: avoid a cycle
        self._runtime_commit = current_git_commit()

    @property
    def stop_requested(self) -> bool:
        return self._stop_requested

    def stop(self) -> None:
        self._stop_requested = True

    def _handle_stop_signal(self, signum: int, frame: object) -> None:
        self.stop()

    def install_default_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self._handle_stop_signal)
        signal.signal(signal.SIGTERM, self._handle_stop_signal)

    def _build_heartbeat(self) -> LiveShadowHeartbeat:
        authority = self._authority_check() if self._authority_check is not None else None
        session = self._tower.client.session if self._tower is not None else None
        last_entry = self._shadow_journal.entries[-1] if self._shadow_journal.entries else None

        mt5_connected = False
        balance: float | None = None
        equity: float | None = None
        open_orders: int | None = None
        open_positions: int | None = None
        try:
            account = self._deps_factory.account()
            balance, equity, mt5_connected = account.balance, account.equity, True
        except Exception:  # noqa: BLE001 -- heartbeat is diagnostic-only, must never crash the loop
            pass
        if self._gateway is not None:
            try:
                orders = self._gateway.orders_get(symbol=SYMBOL)
                positions = self._gateway.positions_get(symbol=SYMBOL)
                open_orders = None if orders is None else len(orders)
                open_positions = None if positions is None else len(positions)
            except Exception:  # noqa: BLE001
                pass

        context = self._context_store.latest()
        snapshot = self._snapshot_store.latest()
        m5_bar = self._m5_loop.last_bar

        return LiveShadowHeartbeat(
            timestamp_utc=int(time.time()), pid=self._pid,
            process_start_identity=self._process_start_identity, runtime_commit=self._runtime_commit,
            authority=("UNKNOWN" if authority is None else authority.value),
            broker_gate_state=("ENABLED" if self._gate.enabled else "DISABLED"),
            tower_worker_session_id=(None if session is None else session.session_id),
            last_closed_bar_id=(None if context is None else context.market_event_id),
            last_market_event_id=(None if last_entry is None else last_entry.market_event_id),
            last_journal_sequence=len(self._shadow_journal.entries),
            last_outcome_reason=(None if last_entry is None else last_entry.terminal_reason_code),
            mt5_connected=mt5_connected, balance=balance, equity=equity,
            open_orders=open_orders, open_positions=open_positions,
            runtime_mode=RUNTIME_MODE_INCREMENTAL, n1_version_pin=n1_version_pin_label(),
            snapshot_identity=(None if snapshot is None else snapshot.identity_fingerprint),
            last_m15_bar_id=(None if context is None else context.market_event_id),
            last_m5_bar_id=(
                None if m5_bar is None else f"{m5_bar.symbol}:M5:{m5_bar.ts_close}"
            ),
            context_timestamp=(None if context is None else context.market_timestamp),
            tower_state=("CONNECTED" if session is not None else "UNAVAILABLE"),
            order_send_calls=0,
        )

    def _write_heartbeat(self) -> None:
        if self._heartbeat_writer is None:
            return
        try:
            self._heartbeat_writer.record(self._build_heartbeat())
        except Exception:  # noqa: BLE001 -- a heartbeat write must never crash the live loop
            pass

    def tick(self) -> bool:
        try:
            circuit_state = load_persisted_circuit_state(self._state_store)
            if circuit_state.state is not EngineState.READY:
                return False
            self._context_refresh.tick()
            self._m5_loop.tick()
            return True
        finally:
            self._write_heartbeat()

    def run_forever(
        self, sleep: Callable[[float], None] = _default_sleep, install_signal_handlers: bool = True,
    ) -> None:
        if install_signal_handlers:
            self.install_default_signal_handlers()
        try:
            while not self._stop_requested:
                self.tick()
                sleep(self._poll_interval_seconds)
        finally:
            self._state_store.close()


def build_incremental_dual_clock_loop(
    gateway: RealMT5HistoryGateway, session: EstablishedSession, state_store: SqliteStateStore,
    state_dir: Path, symbol: str = SYMBOL,
) -> N1IncrementalDualClockLoop:
    """Pure composition, mirroring `entrypoint.build_loop`'s own shape for the legacy path -- the SAME
    `state_store`/`DEFAULT_DB_PATH`, `NewBrainTelemetryLog`, `LiveShadowJournal`, and `HeartbeatWriter` a
    legacy-mode process would use, so a future cutover that switches the flag mid-deployment-lineage
    continues the SAME journal sequence and telemetry log rather than starting a parallel one."""
    context_store = UpstreamContextStore(state_store)
    snapshot_store = N1IncrementalSnapshotStore(state_store)
    n1_client = N1IncrementalClient(
        symbol=symbol, timeframe="M15", bar_interval_seconds=BAR_SECONDS_M15,
        implementation_commit=artifact_pin.PINNED_DELIVERY_COMMIT,
    )

    hydration = hydrate_with_controlled_retry(
        symbol=symbol, gateway=gateway, state_store=state_store, context_store=context_store, client=n1_client,
    )
    print(
        f"new_brain_live.n1_incremental: startup hydration {'OK' if hydration.succeeded else 'FAILED'} "
        f"after {hydration.attempted} attempt(s)"
        + ("" if hydration.succeeded else f" -- {hydration.last_rejection_reason} -- proceeding NO_TRADE"),
        flush=True,
    )

    m15_feed = LiveBarFeed(
        gateway, symbol, MT5_TIMEFRAME_M15, BAR_SECONDS_M15, state_store=state_store,
        broker_offset=make_broker_offset(gateway, symbol), watermark_key_suffix=_M15_CONTEXT_WATERMARK_SUFFIX,
    )
    context_refresh = IncrementalContextRefreshLoop(
        feed=m15_feed, client=n1_client, context_store=context_store, snapshot_store=snapshot_store,
    )

    m5_feed = LiveBarFeed(
        gateway, symbol, MT5_TIMEFRAME_M5, BAR_SECONDS_M5, state_store=state_store,
        broker_offset=make_broker_offset(gateway, symbol),
    )
    tower_client = TowerClient(
        TowerClientConfig(host=session.host, port=session.port, timeout_seconds=15.0), session=session,
    )
    tower = TowerDependencies(client=tower_client, gateway=gateway)
    deps_factory = NewBrainLiveDepsFactory(symbol, gateway, state_dir)
    telemetry_log = NewBrainTelemetryLog(state_store)
    shadow_journal = LiveShadowJournal(state_store)
    gate = BrokerOrderSubmissionGate()
    authority_check = lambda: current_authority(state_store)  # noqa: E731
    m5_loop = M5DecisionLoop(
        feed=m5_feed, context_store=context_store, tower=tower, deps_factory=deps_factory,
        state_store=state_store, telemetry_log=telemetry_log, shadow_journal=shadow_journal,
        authority_check=authority_check, gate=gate,
    )

    heartbeat_writer = HeartbeatWriter(state_store)
    return N1IncrementalDualClockLoop(
        context_refresh=context_refresh, m5_loop=m5_loop, state_store=state_store, context_store=context_store,
        snapshot_store=snapshot_store, tower=tower, deps_factory=deps_factory, gate=gate,
        shadow_journal=shadow_journal, heartbeat_writer=heartbeat_writer, gateway=gateway,
        authority_check=authority_check,
    )


def main_incremental_dual_clock() -> None:
    """Mirrors `entrypoint.main`'s own structure exactly (singleton first, then MT5, then tower, then the
    loop) -- the SAME `SingletonLock` (default name, unchanged), so at most one decisional process ever
    runs system-wide regardless of which runtime mode it was launched under."""
    from ai_trader.new_brain_live.entrypoint import DEFAULT_DB_PATH, DEFAULT_STATE_DIR

    lock = SingletonLock()
    try:
        lock.acquire()
    except AlreadyRunningError as exc:
        print(f"new_brain_live.n1_incremental: ALREADY_RUNNING -- {exc}", flush=True)
        raise SystemExit(0)

    try:
        DEFAULT_STATE_DIR.mkdir(parents=True, exist_ok=True)

        gateway = RealMT5HistoryGateway()
        if not gateway.initialize():
            raise SystemExit(
                f"new_brain_live.n1_incremental: LIVE_SHADOW_STARTUP_FAILED -- MT5 initialize() failed: "
                f"{gateway.last_error()!r}"
            )

        launcher = TowerWorkerLauncher(tower_python=TOWER_VENV_PYTHON)
        session = launcher.launch_and_handshake()
        if not isinstance(session, EstablishedSession):
            gateway.shutdown()
            raise SystemExit(
                f"new_brain_live.n1_incremental: LIVE_SHADOW_STARTUP_FAILED -- tower handshake failed: {session!r}"
            )

        state_store = SqliteStateStore(DEFAULT_DB_PATH)
        try:
            loop = build_incremental_dual_clock_loop(gateway, session, state_store, DEFAULT_STATE_DIR)
            print(
                f"new_brain_live.n1_incremental: N1_INCREMENTAL_DUAL_CLOCK starting -- symbol={SYMBOL} "
                f"tower_version={session.worker_identity.ve_tower_package_version} "
                f"n1_version_pin={n1_version_pin_label()} db={DEFAULT_DB_PATH}",
                flush=True,
            )
            loop.run_forever()
        finally:
            launcher.stop()
            gateway.shutdown()
    finally:
        lock.release()
