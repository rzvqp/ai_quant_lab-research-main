"""`NewBrainLiveLoop`/`build_loop`/`main` -- CEO authorization 2026-08-17 (`AUTORIZEZ PORNIREA LIVE_SHADOW`,
RT-MANDATE2-0003, Red Team commit `b05cbcb`): the continuous, unattended LIVE_SHADOW consumer of
`new_brain_bridge` against real closed MT5 bars and the real, installed `ve_tower` 0.5.2 chain.

**The real flow, per bar**: `LiveBarFeed.poll()` (real, watermarked, gap-detecting) -> `fail_safe.
safe_evaluate_bar(bar, timeframe=M15, axes_builder=..., tower=...)` (real N1 -> Router/Eligibility -> real
IPC v3 -> the isolated worker -> `run_tower_chain` -> real N2/N3/N4 -> official cost model -> EV -> N6) ->
for every `NewBrainOutcome` whose `decision.decision` is `TRADE`/`SHADOW_TRADE_CANDIDATE`: real
`risk_gate.submit_new_brain_candidate` (real Risk Manager) -> real `execution_shadow.
attempt_shadow_execution` with the DEFAULT (`enabled=False`) `BrokerOrderSubmissionGate` -- the broker
barrier is NEVER constructed with `enabled=True` anywhere in this file, and `order_send` is never
imported here, structurally, not merely by convention (`test_new_brain_live_ast_guard.py` proves this).

**`DecisionAuthority` check** (`authority_check`, CEO section 3): consulted fresh every tick, exactly the
`pdh_pdl_demo`/`multi_policy_live`-established discipline (`authority.py`'s own docstring) -- when
`current_authority(state_store)` is NOT `DecisionAuthority.NEW_BRAIN`, this loop treats the cycle as
`LEGACY_SHADOW_TELEMETRY`: it still polls and observes (so a flip back never loses continuity), but never
submits to Risk Manager/Execution Adapter. There is no code path in this file that falls back to
`market_intelligence`/legacy recognition -- the absence is structural, matching `fail_safe.py`'s own
established "no fallback" discipline.

**Circuit breaker** (CEO section 2/§7): `tick()` reads `load_persisted_circuit_state` fresh every call,
exactly `live_loop.LiveSignalLoop`'s own established pattern -- not `READY` skips the cycle entirely, no
bars processed, no candidates evaluated.

**`probability_inputs` in production is `None`, always** (CEO section 4's own explicit prohibition on
`TEST_ONLY_CANONICAL_FIXTURE` in runtime) -- this file never monkeypatches, never imports, never
references `probability_source.load_probability_inputs`'s return value; `bridge.evaluate_bar` calls the
REAL, unmodified `load_probability_inputs`, which the module's own docstring already discloses always
returns `None` today (no validated per-regime outcome-count table exists in this repository). The
overwhelming majority of real events are therefore expected to resolve `NO_TRADE` /
`MISSING_PROBABILITY_INPUTS` -- the CEO's own explicit "acesta este comportamentul corect.\""""

from __future__ import annotations

import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Callable

from ai_trader.live_signal_source.bar_feed import LiveBarFeed, make_broker_offset
from ai_trader.live_signal_source.types import Bar
from ai_trader.mandate2_readiness.broker_gate import BrokerOrderSubmissionGate
from ai_trader.mandate2_readiness.event_identity import NodeTrace
from ai_trader.mt5_pnl_source.gateway import RealMT5HistoryGateway
from ai_trader.new_brain_bridge.authority import DecisionAuthority, current_authority
from ai_trader.new_brain_bridge.bridge import TowerDependencies
from ai_trader.new_brain_bridge.execution_shadow import attempt_shadow_execution
from ai_trader.new_brain_bridge.fail_safe import BrainUnavailableOutcome, safe_evaluate_bar
from ai_trader.new_brain_bridge.raw_axes_builder import RawAxesBuilder
from ai_trader.new_brain_bridge.risk_gate import submit_new_brain_candidate
from ai_trader.new_brain_bridge.telemetry import NewBrainTelemetryLog
from ai_trader.new_brain_bridge.tower_client import TowerClient, TowerClientConfig
from ai_trader.new_brain_bridge.tower_launcher import EstablishedSession, TowerWorkerLauncher
from ai_trader.new_brain_live.deps import NewBrainLiveDepsFactory
from ai_trader.new_brain_live.heartbeat import HeartbeatWriter, LiveShadowHeartbeat
from ai_trader.new_brain_live.live_shadow_journal import LiveShadowCategory, LiveShadowJournal, LiveShadowRecord
from ai_trader.new_brain_live.singleton import AlreadyRunningError, SingletonLock
from ai_trader.persistent_state.store import SqliteStateStore
from ai_trader.risk_manager.types import EngineState
from ai_trader.risk_manager_live.circuit_breaker import load_persisted_circuit_state

SYMBOL = "XAUUSD"
MT5_TIMEFRAME_M15 = 15
BAR_SECONDS_M15 = 15 * 60
POLL_INTERVAL_SECONDS = 30.0
TOWER_VENV_PYTHON = Path("C:/Users/MEDION GAMING/ve_tower_venv/Scripts/python.exe")
DEFAULT_STATE_DIR = Path(__file__).resolve().parents[2] / "new_brain_live_state"
DEFAULT_DB_PATH = DEFAULT_STATE_DIR / "xauusd_m15.db"
_REPO_ROOT = Path(__file__).resolve().parents[2]


def _default_sleep(seconds: float) -> None:
    time.sleep(seconds)


def current_git_commit() -> str:
    """Read fresh from git every call -- never hardcoded, never self-referential (this file's own
    commit can't be known by this file). Fails soft to `"UNKNOWN"`: a heartbeat's diagnostic
    `runtime_commit` field must never crash the live loop over an unavailable `git` binary."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short=7", "HEAD"], cwd=_REPO_ROOT, capture_output=True, text=True,
            timeout=10,
        )
        commit = result.stdout.strip()
        return commit if result.returncode == 0 and commit else "UNKNOWN"
    except Exception:  # noqa: BLE001 -- diagnostic-only, must never raise
        return "UNKNOWN"


class NewBrainLiveLoop:
    """Mirrors `live_loop.LiveSignalLoop`'s own clean-stop/circuit-breaker discipline, adapted for
    `evaluate_bar`'s per-strategy-outcomes-list shape (which `CandidateSignalProducer`'s
    single-`CandidateSignal` contract cannot represent) -- a new, minimal loop, not a fork of the
    existing one."""

    def __init__(
        self, *, feed: LiveBarFeed, axes_builder: RawAxesBuilder, tower: TowerDependencies,
        deps_factory: NewBrainLiveDepsFactory, state_store: SqliteStateStore,
        telemetry_log: NewBrainTelemetryLog, shadow_journal: LiveShadowJournal,
        heartbeat_writer: HeartbeatWriter | None = None,
        gateway: RealMT5HistoryGateway | None = None,
        timeframe: str = "M15",
        poll_interval_seconds: float = POLL_INTERVAL_SECONDS,
        authority_check: Callable[[], DecisionAuthority] | None = None,
        gate: BrokerOrderSubmissionGate = BrokerOrderSubmissionGate(),
    ) -> None:
        if poll_interval_seconds <= 0:
            raise ValueError(f"NewBrainLiveLoop: poll_interval_seconds must be > 0, got {poll_interval_seconds!r}")
        self._feed = feed
        self._axes_builder = axes_builder
        self._tower = tower
        self._deps_factory = deps_factory
        self._state_store = state_store
        self._telemetry_log = telemetry_log
        self._shadow_journal = shadow_journal
        self._heartbeat_writer = heartbeat_writer
        self._gateway = gateway
        self._timeframe = timeframe
        self._poll_interval_seconds = poll_interval_seconds
        self._authority_check = authority_check
        self._gate = gate
        self._stop_requested = False
        self.events_processed = 0
        self._last_bar: Bar | None = None
        self._pid = os.getpid()
        self._process_start_identity = f"{self._pid}:{int(time.time())}"
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

    def _process_bar(self, bar: Bar) -> None:
        self._last_bar = bar
        outcomes = safe_evaluate_bar(bar, timeframe="M15", axes_builder=self._axes_builder, tower=self._tower)
        if isinstance(outcomes, BrainUnavailableOutcome):
            return  # NO_TRADE / BRAIN_UNAVAILABLE -- no legacy fallback exists in this file's body
        self._deps_factory.update_on_bar(bar, self._feed.last_gaps())

        legacy_shadow_only = (
            self._authority_check is not None and self._authority_check() is not DecisionAuthority.NEW_BRAIN
        )

        for outcome in outcomes:
            self.events_processed += 1
            decision = outcome.decision
            terminal_reason = (
                decision.reason_codes[0] if decision is not None and decision.reason_codes else "NO_DECISION"
            )
            if decision is None or decision.decision not in ("TRADE", "SHADOW_TRADE_CANDIDATE"):
                self._telemetry_log.record_outcome(outcome)
                self._shadow_journal.record(LiveShadowRecord(
                    category=LiveShadowCategory.LIVE_SHADOW_NO_TRADE, trace_id=outcome.event_identity.trace_id,
                    market_event_id=outcome.event_identity.market_event_id, strategy_id=outcome.strategy_id,
                    as_of=outcome.event_identity.market_timestamp, terminal_reason_code=terminal_reason,
                    decision=None if decision is None else decision.decision,
                ))
                continue

            if legacy_shadow_only:
                # LEGACY_SHADOW_TELEMETRY: observed, journaled, but never submitted to Risk Manager --
                # the demotion the CEO's own section 3 requires for any process running WITHOUT
                # NEW_BRAIN authority.
                self._telemetry_log.record_outcome(outcome)
                self._shadow_journal.record(LiveShadowRecord(
                    category=LiveShadowCategory.LIVE_SHADOW_EVENT, trace_id=outcome.event_identity.trace_id,
                    market_event_id=outcome.event_identity.market_event_id, strategy_id=outcome.strategy_id,
                    as_of=outcome.event_identity.market_timestamp,
                    terminal_reason_code="LEGACY_SHADOW_TELEMETRY_AUTHORITY_NOT_NEW_BRAIN",
                    decision=decision.decision,
                ))
                continue

            circuit_state = load_persisted_circuit_state(self._state_store)
            risk_decision = submit_new_brain_candidate(
                outcome, account=self._deps_factory.account(), portfolio=self._deps_factory.portfolio(),
                instrument=self._deps_factory.instrument(),
                risk_context=self._deps_factory.risk_context(as_of=outcome.event_identity.market_timestamp),
                risk_config=self._deps_factory.risk_config(), circuit_state=circuit_state,
            )
            shadow_result = attempt_shadow_execution(risk_decision, gate=self._gate)

            risk_trace = NodeTrace(
                trace_id=outcome.event_identity.trace_id, node_name="RiskManager",
                input_fingerprint=outcome.event_identity.trace_id, output=str(risk_decision.approved),
                reason_codes=risk_decision.reason_codes, latency_seconds=0.0, component_version="risk_gate_v1",
            )
            exec_trace = NodeTrace(
                trace_id=outcome.event_identity.trace_id, node_name="ExecutionAdapter",
                input_fingerprint=outcome.event_identity.trace_id,
                output=f"{shadow_result.reached_broker_gate}|{shadow_result.blocked}",
                reason_codes=() if shadow_result.blocked is False else (shadow_result.reason,),
                latency_seconds=0.0, component_version="broker_gate_v1",
            )
            self._telemetry_log.record_outcome(outcome, extra_traces=(risk_trace, exec_trace))

            category = (
                LiveShadowCategory.LIVE_SHADOW_BLOCKED_AT_BROKER if shadow_result.reached_broker_gate
                else LiveShadowCategory.LIVE_SHADOW_CANDIDATE
            )
            self._shadow_journal.record(LiveShadowRecord(
                category=category, trace_id=outcome.event_identity.trace_id,
                market_event_id=outcome.event_identity.market_event_id, strategy_id=outcome.strategy_id,
                as_of=outcome.event_identity.market_timestamp, terminal_reason_code=terminal_reason,
                decision=decision.decision, reached_broker_gate=shadow_result.reached_broker_gate,
                broker_blocked=shadow_result.blocked, order_send_calls=0, orders_created=0, positions_created=0,
            ))

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
                orders = self._gateway.orders_get(symbol=self._axes_builder.symbol)
                positions = self._gateway.positions_get(symbol=self._axes_builder.symbol)
                open_orders = None if orders is None else len(orders)
                open_positions = None if positions is None else len(positions)
            except Exception:  # noqa: BLE001
                pass

        return LiveShadowHeartbeat(
            timestamp_utc=int(time.time()), pid=self._pid,
            process_start_identity=self._process_start_identity, runtime_commit=self._runtime_commit,
            authority=("UNKNOWN" if authority is None else authority.value),
            broker_gate_state=("ENABLED" if self._gate.enabled else "DISABLED"),
            tower_worker_session_id=(None if session is None else session.session_id),
            last_closed_bar_id=(
                None if self._last_bar is None
                else f"{self._last_bar.symbol}:{self._timeframe}:{self._last_bar.ts_close}"
            ),
            last_market_event_id=(None if last_entry is None else last_entry.market_event_id),
            last_journal_sequence=len(self._shadow_journal.entries),
            last_outcome_reason=(None if last_entry is None else last_entry.terminal_reason_code),
            mt5_connected=mt5_connected, balance=balance, equity=equity,
            open_orders=open_orders, open_positions=open_positions,
            runtime_mode="LEGACY_M15",
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
            for bar in self._feed.poll():
                self._process_bar(bar)
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


def build_loop(
    gateway: RealMT5HistoryGateway, session: EstablishedSession, state_store: SqliteStateStore,
    state_dir: Path, symbol: str = SYMBOL,
) -> NewBrainLiveLoop:
    """Pure composition, no new I/O beyond what `gateway`/`session`/`state_store` already do."""
    feed = LiveBarFeed(
        gateway, symbol, MT5_TIMEFRAME_M15, BAR_SECONDS_M15, state_store=state_store,
        broker_offset=make_broker_offset(gateway, symbol),
    )
    axes_builder = RawAxesBuilder(symbol)
    tower_client = TowerClient(
        TowerClientConfig(host=session.host, port=session.port, timeout_seconds=15.0), session=session,
    )
    tower = TowerDependencies(client=tower_client, gateway=gateway)
    # RT-TIME-0001 section A: `wall_clock_provider` defaults to the real `time.time`, called fresh on
    # every tower-chain call -- never captured once here. Bar-fetch anchoring uses `event_as_of`/
    # `data_cutoff` (derived per-call from the bar actually being evaluated), which is the fix for the
    # `TowerDependencies.now`-frozen-at-startup defect `LIVE_SHADOW_TIMEFRAME_AUDIT.md` found.
    deps_factory = NewBrainLiveDepsFactory(symbol, gateway, state_dir)
    telemetry_log = NewBrainTelemetryLog(state_store)
    shadow_journal = LiveShadowJournal(state_store)
    heartbeat_writer = HeartbeatWriter(state_store)
    return NewBrainLiveLoop(
        feed=feed, axes_builder=axes_builder, tower=tower, deps_factory=deps_factory, state_store=state_store,
        telemetry_log=telemetry_log, shadow_journal=shadow_journal, heartbeat_writer=heartbeat_writer,
        gateway=gateway, authority_check=lambda: current_authority(state_store),
    )


def main() -> None:
    """RT-N1-INCREMENTAL-WIRING-0001 (2026-08-18): the ONLY edit in this function for the new runtime
    mode is the `if` immediately below. `resolve_runtime_mode()` reads `AI_TRADER_RUNTIME_MODE` from the
    environment and returns `LEGACY_M15` for anything other than the exact literal `N1_INCREMENTAL_DUAL_
    CLOCK` (unset, empty, a typo) -- so an accidental Scheduled-Task restart with no explicit override
    reaches the exact same body below, unchanged, that has been running since `65798b4`. The import is
    local so merely importing this module never pulls in `n1_incremental` unless the flag is actually on.

    Singleton acquisition happens FIRST, before any MT5/tower I/O -- a second launch must exit
    cleanly (`ALREADY_RUNNING`) without ever touching the terminal or spawning a second tower worker
    (RT-N1-PERSIST-0001 section 1: "maximum o instanta decizionala")."""
    from ai_trader.new_brain_live.n1_incremental.runtime_loop import RUNTIME_MODE_INCREMENTAL, resolve_runtime_mode

    if resolve_runtime_mode() == RUNTIME_MODE_INCREMENTAL:
        from ai_trader.new_brain_live.n1_incremental.runtime_loop import main_incremental_dual_clock
        main_incremental_dual_clock()
        return

    lock = SingletonLock()
    try:
        lock.acquire()
    except AlreadyRunningError as exc:
        print(f"new_brain_live: ALREADY_RUNNING -- {exc}", flush=True)
        raise SystemExit(0)

    try:
        DEFAULT_STATE_DIR.mkdir(parents=True, exist_ok=True)

        gateway = RealMT5HistoryGateway()
        if not gateway.initialize():
            raise SystemExit(f"new_brain_live: LIVE_SHADOW_STARTUP_FAILED -- MT5 initialize() failed: {gateway.last_error()!r}")

        launcher = TowerWorkerLauncher(tower_python=TOWER_VENV_PYTHON)
        session = launcher.launch_and_handshake()
        if not isinstance(session, EstablishedSession):
            gateway.shutdown()
            raise SystemExit(f"new_brain_live: LIVE_SHADOW_STARTUP_FAILED -- tower handshake failed: {session!r}")

        state_store = SqliteStateStore(DEFAULT_DB_PATH)
        try:
            loop = build_loop(gateway, session, state_store, DEFAULT_STATE_DIR)
            print(
                f"new_brain_live: LIVE_SHADOW starting -- symbol={SYMBOL} "
                f"tower_version={session.worker_identity.ve_tower_package_version} db={DEFAULT_DB_PATH}",
                flush=True,
            )
            loop.run_forever()
        finally:
            launcher.stop()
            gateway.shutdown()
    finally:
        lock.release()


if __name__ == "__main__":
    main()
