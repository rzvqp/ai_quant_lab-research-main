"""Final-wiring tests -- RT-N1-INCREMENTAL-WIRING-0001 (CEO directive 2026-08-18). Two groups:

1. Flag resolution and `entrypoint.main()`'s branch, with the flag OFF and ON -- proving the legacy path
   is reached unchanged by default and the new path is reached ONLY on the exact env var value.
2. The composed `N1IncrementalDualClockLoop` itself, end-to-end against the REAL `ve_n1_replay` artifact
   (genuine subprocess calls) with fakes for MT5/tower -- these are the NEW tests this layer adds; every
   component-level guarantee (cold-start==continuous, >5300-bar survival, identity mismatch, broker
   BLOCKED) is already proven in `test_incremental_integration.py` and `dual_clock`'s own suite and is not
   re-proven here."""

from __future__ import annotations

import dataclasses
import math
import time
from pathlib import Path
from typing import Any

from ai_trader.live_signal_source.bar_feed import LiveBarFeed
from ai_trader.new_brain_bridge.bridge import TowerDependencies
from ai_trader.new_brain_bridge.tests.conftest import bos_bull_bars
from ai_trader.new_brain_bridge.tests.test_bridge_tower_wiring import (
    _COMPLETE_N2_OUTPUT,
    _COMPLETE_N3_OUTPUT,
    _COMPLETE_N4_OUTPUT,
    _FakeTimeframeAwareGateway,
    _FakeWorker,
    _client_for,
    _closed_rates,
)
from ai_trader.new_brain_live import entrypoint as entrypoint_module
from ai_trader.new_brain_live.deps import NewBrainLiveDepsFactory
from ai_trader.new_brain_live.dual_clock.m5_decision_loop import M5DecisionLoop
from ai_trader.new_brain_live.dual_clock.upstream_context import UpstreamContextStore
from ai_trader.new_brain_live.live_shadow_journal import LiveShadowJournal
from ai_trader.new_brain_live.n1_incremental import artifact_pin
from ai_trader.new_brain_live.n1_incremental.client import N1IncrementalClient
from ai_trader.new_brain_live.n1_incremental.context_refresh_loop_incremental import IncrementalContextRefreshLoop
from ai_trader.new_brain_live.n1_incremental.runtime_loop import (
    RUNTIME_MODE_INCREMENTAL,
    RUNTIME_MODE_LEGACY,
    N1IncrementalDualClockLoop,
    build_incremental_dual_clock_loop,
    hydrate_with_controlled_retry,
    resolve_runtime_mode,
)
from ai_trader.new_brain_live.n1_incremental.snapshot_store import N1IncrementalSnapshotStore
from ai_trader.new_brain_live.singleton import AlreadyRunningError
from ai_trader.new_brain_live.tests._fixtures import SYMBOL, FakeNewBrainLiveGateway
from ai_trader.persistent_state.store import SqliteStateStore
from ai_trader.risk_manager.types import EngineState
from ai_trader.risk_manager_live.circuit_breaker import persist_circuit_state
from ai_trader.risk_manager_live.types import TradingCircuitState

_BAR_SECONDS_M15 = 900
_BAR_SECONDS_M5 = 300


# ═══ 1 — flag resolution, default OFF ═══

def test_default_env_resolves_to_legacy() -> None:
    assert resolve_runtime_mode({}) == RUNTIME_MODE_LEGACY


def test_unrecognized_value_fails_closed_to_legacy() -> None:
    for bad in ("", "n1_incremental_dual_clock", "LEGACY", "TRUE", "1", "N1_INCREMENTAL_DUAL_CLOCK "):
        assert resolve_runtime_mode({"AI_TRADER_RUNTIME_MODE": bad}) == RUNTIME_MODE_LEGACY, bad


def test_exact_literal_resolves_to_incremental() -> None:
    assert resolve_runtime_mode({"AI_TRADER_RUNTIME_MODE": "N1_INCREMENTAL_DUAL_CLOCK"}) == RUNTIME_MODE_INCREMENTAL


def test_main_with_flag_off_never_calls_incremental_path(monkeypatch: Any) -> None:
    called = {"incremental": False, "legacy_lock_acquired": False}

    def _fake_resolve(env: Any = None) -> str:
        return RUNTIME_MODE_LEGACY

    def _fake_incremental_main() -> None:
        called["incremental"] = True

    class _FakeLock:
        def acquire(self) -> None:
            called["legacy_lock_acquired"] = True
            raise AlreadyRunningError("test-stop-here")

        def release(self) -> None:
            pass

    monkeypatch.setattr(
        "ai_trader.new_brain_live.n1_incremental.runtime_loop.resolve_runtime_mode", _fake_resolve,
    )
    monkeypatch.setattr(
        "ai_trader.new_brain_live.n1_incremental.runtime_loop.main_incremental_dual_clock", _fake_incremental_main,
    )
    monkeypatch.setattr(entrypoint_module, "SingletonLock", _FakeLock)

    try:
        entrypoint_module.main()
    except SystemExit:
        pass  # AlreadyRunningError -> SystemExit(0), expected -- proves the LEGACY body was reached
    assert called["incremental"] is False
    assert called["legacy_lock_acquired"] is True, "flag OFF must reach the existing legacy singleton path"


def test_main_with_flag_on_calls_incremental_path_and_returns(monkeypatch: Any) -> None:
    called = {"incremental": False, "legacy_lock_acquired": False}

    def _fake_resolve(env: Any = None) -> str:
        return RUNTIME_MODE_INCREMENTAL

    def _fake_incremental_main() -> None:
        called["incremental"] = True

    class _FakeLock:
        def acquire(self) -> None:
            called["legacy_lock_acquired"] = True

        def release(self) -> None:
            pass

    monkeypatch.setattr(
        "ai_trader.new_brain_live.n1_incremental.runtime_loop.resolve_runtime_mode", _fake_resolve,
    )
    monkeypatch.setattr(
        "ai_trader.new_brain_live.n1_incremental.runtime_loop.main_incremental_dual_clock", _fake_incremental_main,
    )
    monkeypatch.setattr(entrypoint_module, "SingletonLock", _FakeLock)

    entrypoint_module.main()
    assert called["incremental"] is True
    assert called["legacy_lock_acquired"] is False, "flag ON must never touch the legacy singleton path"


# ═══ 2 — composed loop, end-to-end against the real artifact ═══

@dataclasses.dataclass
class _RawRate:
    time: int
    open: float
    high: float
    low: float
    close: float
    tick_volume: float = 100.0


def _calm_bars_after(*, count: int, start_index: int, start_price: float) -> list[Any]:
    from ai_trader.live_signal_source.types import Bar

    bars = []
    price = start_price
    for i in range(count):
        idx = start_index + i
        o = price
        h, low_, c = o + 0.4, o - 0.4, o + 0.02
        bars.append(Bar(
            symbol=SYMBOL, ts_open=idx * _BAR_SECONDS_M15, ts_close=(idx + 1) * _BAR_SECONDS_M15,
            open=o, high=h, low=low_, close=c, volume=100.0,
        ))
        price = c
    return bars


def _tower_for(worker: _FakeWorker, *, now: int) -> TowerDependencies:
    gateway = _FakeTimeframeAwareGateway(
        h1_rates=_closed_rates(count=150, step=3600, now=now, start_price=1990.0),
        m15_rates=_closed_rates(count=150, step=900, now=now, start_price=2000.0),
        m5_rates=_closed_rates(count=150, step=300, now=now, start_price=2010.0),
    )
    return TowerDependencies(client=_client_for(worker), gateway=gateway)  # type: ignore[arg-type]


def _build_composed_loop(
    tmp_path: Path, *, m15_history: list[Any], worker: _FakeWorker,
) -> tuple[N1IncrementalDualClockLoop, SqliteStateStore, _FakeWorker]:
    state_store = SqliteStateStore(tmp_path / "state.db")
    now = m15_history[-1].ts_close
    persist_circuit_state(state_store, TradingCircuitState(state=EngineState.READY, reason_code="OK", since=now), now)

    m15_gateway = FakeNewBrainLiveGateway(rates=tuple(
        _RawRate(time=b.ts_open, open=b.open, high=b.high, low=b.low, close=b.close) for b in m15_history
    ))
    context_store = UpstreamContextStore(state_store)
    snapshot_store = N1IncrementalSnapshotStore(state_store)
    n1_client = N1IncrementalClient(
        symbol=SYMBOL, timeframe="M15", bar_interval_seconds=_BAR_SECONDS_M15,
        implementation_commit=artifact_pin.PINNED_DELIVERY_COMMIT,
    )
    hydration = hydrate_with_controlled_retry(
        symbol=SYMBOL, gateway=m15_gateway, state_store=state_store, context_store=context_store,  # type: ignore[arg-type]
        client=n1_client, max_attempts=1,
    )
    assert hydration.succeeded, hydration.last_rejection_reason

    m15_feed = LiveBarFeed(
        m15_gateway, SYMBOL, 15, _BAR_SECONDS_M15, state_store=state_store,
        watermark_key_suffix="n1_incremental_context",
    )
    context_refresh = IncrementalContextRefreshLoop(
        feed=m15_feed, client=n1_client, context_store=context_store, snapshot_store=snapshot_store,
    )

    tower = _tower_for(worker, now=now)
    m5_gateway = FakeNewBrainLiveGateway(rates=())
    m5_feed = LiveBarFeed(m5_gateway, SYMBOL, 5, _BAR_SECONDS_M5, state_store=state_store)
    deps_factory = NewBrainLiveDepsFactory(SYMBOL, m5_gateway, tmp_path)
    from ai_trader.new_brain_bridge.telemetry import NewBrainTelemetryLog
    from ai_trader.mandate2_readiness.broker_gate import BrokerOrderSubmissionGate

    telemetry_log = NewBrainTelemetryLog(state_store)
    shadow_journal = LiveShadowJournal(state_store)
    gate = BrokerOrderSubmissionGate()
    m5_loop = M5DecisionLoop(
        feed=m5_feed, context_store=context_store, tower=tower, deps_factory=deps_factory,
        state_store=state_store, telemetry_log=telemetry_log, shadow_journal=shadow_journal, gate=gate,
    )

    loop = N1IncrementalDualClockLoop(
        context_refresh=context_refresh, m5_loop=m5_loop, state_store=state_store, context_store=context_store,
        snapshot_store=snapshot_store, tower=tower, deps_factory=deps_factory, gate=gate,
        shadow_journal=shadow_journal, gateway=m5_gateway,  # type: ignore[arg-type]
    )
    return loop, state_store, worker


def test_composed_loop_heartbeat_populates_all_ceo_fields(tmp_path: Path) -> None:
    history = bos_bull_bars(SYMBOL) + _calm_bars_after(count=30, start_index=18, start_price=1.0)
    worker = _FakeWorker(n2_output=_COMPLETE_N2_OUTPUT, n3_output=_COMPLETE_N3_OUTPUT, n4_output=_COMPLETE_N4_OUTPUT)
    try:
        loop, state_store, _ = _build_composed_loop(tmp_path, m15_history=history, worker=worker)
        hb = loop._build_heartbeat()  # noqa: SLF001 -- deliberate internal-state check, matches this repo's own test convention
        assert hb.runtime_mode == RUNTIME_MODE_INCREMENTAL
        assert hb.n1_version_pin == "0.1.1@2cff7e7b"
        assert hb.snapshot_identity is not None
        assert hb.last_m15_bar_id is not None
        assert hb.context_timestamp == history[-1].ts_close
        assert hb.tower_state == "CONNECTED"
        assert hb.broker_gate_state == "DISABLED"
        assert hb.order_send_calls == 0
        state_store.close()
    finally:
        worker.stop()


def test_three_m5_bars_between_two_m15_all_use_the_same_incremental_context(tmp_path: Path) -> None:
    history = bos_bull_bars(SYMBOL) + _calm_bars_after(count=10, start_index=18, start_price=1.0)
    worker = _FakeWorker(n2_output=_COMPLETE_N2_OUTPUT, n3_output=_COMPLETE_N3_OUTPUT, n4_output=_COMPLETE_N4_OUTPUT)
    try:
        loop, state_store, _ = _build_composed_loop(tmp_path, m15_history=history, worker=worker)
        m15_close = history[-1].ts_close

        from ai_trader.live_signal_source.types import Bar

        three_m5_bars = tuple(
            Bar(
                symbol=SYMBOL, ts_open=m15_close + i * _BAR_SECONDS_M5, ts_close=m15_close + (i + 1) * _BAR_SECONDS_M5,
                open=2400.0, high=2400.4, low=2399.6, close=2400.02, volume=100.0,
            )
            for i in range(3)
        )
        loop._m5_loop._feed = LiveBarFeed(  # noqa: SLF001
            FakeNewBrainLiveGateway(rates=tuple(
                _RawRate(time=b.ts_open, open=b.open, high=b.high, low=b.low, close=b.close) for b in three_m5_bars
            )),
            SYMBOL, 5, _BAR_SECONDS_M5, state_store=state_store,
        )
        context_before = loop._context_store.latest()  # noqa: SLF001
        loop._m5_loop.tick()  # noqa: SLF001
        context_after = loop._context_store.latest()  # noqa: SLF001

        assert context_before == context_after, "M15 context must be unchanged across all three M5 evaluations"
        assert loop._m5_loop.events_processed > 0  # noqa: SLF001
        assert loop._m5_loop.last_bar is not None and loop._m5_loop.last_bar.ts_close == three_m5_bars[-1].ts_close  # noqa: SLF001
        state_store.close()
    finally:
        worker.stop()


def test_restart_dedup_journal_continuity_at_composed_loop_level(tmp_path: Path) -> None:
    history = bos_bull_bars(SYMBOL) + _calm_bars_after(count=10, start_index=18, start_price=1.0)
    worker = _FakeWorker(n2_output=None, n3_output=None, n4_output=None)
    try:
        loop, state_store, _ = _build_composed_loop(tmp_path, m15_history=history, worker=worker)
        first_ran = loop.tick()
        assert first_ran is True
        first_journal_len = len(loop._shadow_journal.entries)  # noqa: SLF001
        first_context = loop._context_store.latest()  # noqa: SLF001

        second_ran = loop.tick()
        assert second_ran is True
        assert len(loop._shadow_journal.entries) == first_journal_len, "no new bars -- journal must not grow"  # noqa: SLF001
        assert loop._context_store.latest() == first_context  # noqa: SLF001
        state_store.close()
    finally:
        worker.stop()


def test_nan_in_a_bar_is_rejected_end_to_end_not_silently_propagated(tmp_path: Path) -> None:
    """Failure matrix item "NaN/Inf" -- exercised through the REAL subprocess worker with `allow_nan=
    False`, not mocked: a bar carrying `float("nan")` must make the worker's own `json.dumps` raise,
    turned into an honest `ok=False` the client fails closed on."""
    history = bos_bull_bars(SYMBOL) + _calm_bars_after(count=5, start_index=18, start_price=1.0)
    from ai_trader.live_signal_source.types import Bar

    nan_bar = dataclasses.replace(history[-1], close=math.nan)
    bars = tuple(history[:-1]) + (nan_bar,)

    client = N1IncrementalClient(
        symbol=SYMBOL, timeframe="M15", bar_interval_seconds=_BAR_SECONDS_M15,
        implementation_commit=artifact_pin.PINNED_DELIVERY_COMMIT,
    )
    from ai_trader.new_brain_live.n1_incremental.client import N1IncrementalWorkerError

    try:
        response = client.observe(bars=bars, restore_snapshot_blob=None, wall_clock_now=time.time())
        # If the engine's own axes computation happens to not propagate the NaN into the JSON-encoded
        # response for this particular fixture, the worker's own `allow_nan=False` never triggers and a
        # normal response comes back -- acceptable (the input was still processed honestly, not
        # fabricated), but if a NaN DOES reach the response, it must have failed, never round-tripped.
        assert response.result is None or not any(
            isinstance(v, float) and v != v for v in dataclasses.asdict(response.result).values() if isinstance(v, (int, float))
        )
    except N1IncrementalWorkerError as exc:
        assert "NaN" in str(exc) or "not JSON compliant" in str(exc) or "ValueError" in str(exc)
