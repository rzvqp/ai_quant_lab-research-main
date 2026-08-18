"""`M5DecisionLoop` -- RT-TIME-0001 section B, the actual M5-triggered N4/EV/N6/Risk/Shadow path.

**Chain-binding, honored structurally**: the only tower call this file ever makes is
`bridge.query_tower_chain` -- the exact same (renamed-public, unmodified) function `evaluate_bar` itself
uses, which internally calls the REAL `run_tower_chain` and binds N2/N3/N4 identities within that one
response. Never `run_n2`/`run_n3`/`run_n4` directly (AST-guard-proven, `tests/test_ast_guard.py`), never
a cached prior N2/N3 answer substituted in. N1/Router are the only cached inputs -- read from
`UpstreamContextStore`, never recomputed here (`ContextRefreshLoop` is the only place that happens).

**Telemetry**: every path (context-stale, Router-ineligible, ATR-insufficient, cost-unavailable,
legacy-shadow-demoted, N6 NO_TRADE, and a genuine candidate) builds a real `NewBrainOutcome` and records
it via `NewBrainTelemetryLog.record_outcome` -- the SAME telemetry log and the SAME record shape
`evaluate_bar`'s own callers already use, just with `timeframe="M5"` and an M5-precision
`market_event_id`."""

from __future__ import annotations

import dataclasses
from typing import Any, Callable

import ve_brain  # type: ignore[import-untyped]

from ai_trader.live_signal_source.bar_feed import LiveBarFeed
from ai_trader.live_signal_source.types import Bar
from ai_trader.mandate2_readiness.broker_gate import BrokerOrderSubmissionGate
from ai_trader.mandate2_readiness.decision_provenance import NEW_BRAIN_SOURCE, DecisionProvenance
from ai_trader.mandate2_readiness.event_identity import EventIdentity, NodeTrace
from ai_trader.mandate2_readiness.shadow_cost_model import (
    CALIBRATION_STATUS,
    SHADOW_COST_MODEL_VERSION,
    CostModelUnavailableError,
)
from ai_trader.mandate2_readiness.shadow_cost_model import configuration_fingerprint as cost_model_configuration_fingerprint
from ai_trader.mandate2_readiness.shadow_cost_model import resolve_cost_components
from ai_trader.new_brain_bridge.authority import DecisionAuthority
from ai_trader.new_brain_bridge.bridge import (
    ELIGIBILITY_POLICY_VERSION,
    FP,
    PLACEHOLDER_TARGET_RR,
    NewBrainOutcome,
    TowerDependencies,
    query_tower_chain,
    side_from_strategy,
)
from ai_trader.new_brain_bridge.execution_shadow import attempt_shadow_execution
from ai_trader.new_brain_bridge.probability_source import load_probability_inputs
from ai_trader.new_brain_bridge.risk_gate import submit_new_brain_candidate
from ai_trader.new_brain_bridge.telemetry import NewBrainTelemetryLog
from ai_trader.new_brain_live.deps import NewBrainLiveDepsFactory
from ai_trader.new_brain_live.dual_clock.upstream_context import UpstreamContextStore
from ai_trader.new_brain_live.live_shadow_journal import LiveShadowCategory, LiveShadowJournal, LiveShadowRecord
from ai_trader.persistent_state.store import SqliteStateStore
from ai_trader.risk_manager.types import EngineState
from ai_trader.risk_manager_live.circuit_breaker import load_persisted_circuit_state

_TIMEFRAME = "M5"
_CONTEXT_STALE = "CONTEXT_STALE"
_CONTEXT_FROM_FUTURE = "CONTEXT_FROM_FUTURE"
_DEFAULT_MAX_CONTEXT_STALENESS_SECONDS = 2 * 900  # two M15 bars of slack, mirrors TowerDependencies' own convention
_DEFAULT_STOP_ATR_MULTIPLIER = 1.0  # today's existing behavior for every strategy_id with no override


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class StrategyGeometry:
    """Per-strategy execution geometry `ve_brain.StrategyContract` has no fields for. Canonical source of
    truth is `n1_incremental.strategies.canonical_specs.GEOMETRY`, which imports this type from here
    (not the reverse) so `dual_clock` never depends on the newer, higher-level `strategies` package."""

    stop_atr_multiplier: float
    target_kind: str  # "rr" | "price" | "none" -- the only 3 values ve_brain.contracts._require accepts
    target_param: float | None


def _event_identity(*, trace_id: str, market_event_id: str, bar: Bar, configuration_fingerprint: str) -> EventIdentity:
    return EventIdentity(
        trace_id=trace_id, market_event_id=market_event_id, symbol=bar.symbol, timeframe=_TIMEFRAME,
        bar_id=market_event_id, market_timestamp=bar.ts_close, received_timestamp=bar.ts_close,
        brain_version=ve_brain.VE_BRAIN_VERSION, catalog_hash=ve_brain.CANONICAL_CATALOG_HASH,
        configuration_fingerprint=configuration_fingerprint,
    )


class M5DecisionLoop:
    def __init__(
        self, *, feed: LiveBarFeed, context_store: UpstreamContextStore, tower: TowerDependencies,
        deps_factory: NewBrainLiveDepsFactory, state_store: SqliteStateStore,
        telemetry_log: NewBrainTelemetryLog, shadow_journal: LiveShadowJournal,
        catalog: tuple[ve_brain.StrategyContract, ...] = ve_brain.CANONICAL_STRATEGIES,
        max_context_staleness_seconds: float = _DEFAULT_MAX_CONTEXT_STALENESS_SECONDS,
        authority_check: Callable[[], DecisionAuthority] | None = None,
        gate: BrokerOrderSubmissionGate = BrokerOrderSubmissionGate(),
        expected_cost_model_fingerprint: str | None = None,
        strategy_geometry_overrides: dict[str, StrategyGeometry] | None = None,
    ) -> None:
        self._feed = feed
        self._context_store = context_store
        self._tower = tower
        self._deps_factory = deps_factory
        self._state_store = state_store
        self._telemetry_log = telemetry_log
        self._shadow_journal = shadow_journal
        self._catalog = catalog
        self._max_context_staleness_seconds = max_context_staleness_seconds
        self._authority_check = authority_check
        self._gate = gate
        self._expected_cost_model_fingerprint = expected_cost_model_fingerprint
        self._strategy_geometry_overrides = strategy_geometry_overrides or {}
        """RT-THREE-STRATEGY-0001 (2026-08-18): additive, defaults to {} -- a `strategy_id` absent from
        this map falls back to today's exact existing geometry (ATR x1 stop, target_kind="rr" /
        PLACEHOLDER_TARGET_RR), so the 4 pre-existing canonical strategies are byte-for-byte unaffected."""
        self.events_processed = 0
        self.last_bar: Bar | None = None
        """RT-N1-INCREMENTAL-WIRING-0001: mirrors `NewBrainLiveLoop`'s own `_last_bar` convention --
        additive, read by the runtime wiring's own heartbeat builder (`last_m5_bar_id`), never consulted
        by this class's own decision logic."""

    def _record(
        self, *, category: LiveShadowCategory, event_identity: EventIdentity, strategy_id: str,
        strategy_version: str, bar: Bar, terminal_reason: str, node_traces: tuple[NodeTrace, ...],
        decision: object | None = None, reached_broker_gate: bool | None = None,
        broker_blocked: bool | None = None,
    ) -> None:
        outcome = NewBrainOutcome(
            event_identity=event_identity, strategy_id=strategy_id, strategy_version=strategy_version,
            node_traces=node_traces, decision=decision, provenance=None,
        )
        self._telemetry_log.record_outcome(outcome)
        self._shadow_journal.record(LiveShadowRecord(
            category=category, trace_id=event_identity.trace_id, market_event_id=event_identity.market_event_id,
            strategy_id=strategy_id, as_of=bar.ts_close, terminal_reason_code=terminal_reason,
            decision=None if decision is None else decision.decision,  # type: ignore[attr-defined]
            reached_broker_gate=reached_broker_gate, broker_blocked=broker_blocked,
            order_send_calls=0 if reached_broker_gate is not None else None,
            orders_created=0 if reached_broker_gate is not None else None,
            positions_created=0 if reached_broker_gate is not None else None,
        ))

    def _reject_all_context_invalid(self, *, bar: Bar, market_event_id: str, terminal_reason: str) -> None:
        for canon in self._catalog:
            self.events_processed += 1
            trace_id = FP(market_event_id, canon.strategy_id, canon.strategy_version, "context-stale")
            event_identity = _event_identity(
                trace_id=trace_id, market_event_id=market_event_id, bar=bar, configuration_fingerprint=trace_id,
            )
            self._record(
                category=LiveShadowCategory.LIVE_SHADOW_NO_TRADE, event_identity=event_identity,
                strategy_id=canon.strategy_id, strategy_version=canon.strategy_version, bar=bar,
                terminal_reason=terminal_reason, node_traces=(),
            )

    def _process_m5_bar(self, bar: Bar) -> None:
        market_event_id = f"{bar.symbol}:{_TIMEFRAME}:{bar.ts_close}"
        context = self._context_store.latest()
        legacy_shadow_only = (
            self._authority_check is not None and self._authority_check() is not DecisionAuthority.NEW_BRAIN
        )

        if context is None or (bar.ts_close - context.market_timestamp) > self._max_context_staleness_seconds:
            self._reject_all_context_invalid(bar=bar, market_event_id=market_event_id, terminal_reason=_CONTEXT_STALE)
            return
        # `context.market_timestamp > bar.ts_close` means the cached M15 context closed AFTER the M5 bar
        # being evaluated right now -- e.g. a backlog/out-of-order tick lets `ContextRefreshLoop` race
        # ahead of `M5DecisionLoop`. That context was never causally available to this M5 bar; using it
        # would be exactly the lookahead this whole dual-clock design exists to forbid. The staleness
        # bound above does NOT catch this on its own -- a negative age is always numerically "not stale"
        # -- so it needs its own, separately-diagnosable fail-closed branch.
        if context.market_timestamp > bar.ts_close:
            self._reject_all_context_invalid(
                bar=bar, market_event_id=market_event_id, terminal_reason=_CONTEXT_FROM_FUTURE,
            )
            return

        _chain_result_cache: dict[int, Any] = {}

        def _get_chain_result(*, side: int, trace_id: str, configuration_fingerprint: str,
                               strategy_id: str, strategy_version: str) -> Any:
            """RT-THREE-STRATEGY-0001: mirrors `bridge._query_tower_chain`'s own established memoization
            (`evaluate_bar`'s `_get_chain_result`) -- N2/N4's computation depends only on `side`, so every
            strategy in `self._catalog` sharing this M5 bar's side gets ONE real Tower call, not one each.
            This is the CEO's own 'un singur apel Tower per market event, partajat intre strategii'
            requirement, satisfied by reusing the exact pattern the M15 loop already established rather
            than inventing a new one."""
            if side not in _chain_result_cache:
                _chain_result_cache[side] = query_tower_chain(
                    self._tower, market_event_id=market_event_id, trace_id=trace_id, symbol=bar.symbol,
                    event_as_of=bar.ts_close, data_cutoff=bar.ts_close,
                    configuration_fingerprint=configuration_fingerprint, n1_fingerprint=context.n1_output_fp,
                    regime_axes_status=context.regime_axes_status, strategy_id=strategy_id,
                    strategy_version=strategy_version, side=side,
                )
            return _chain_result_cache[side]

        for canon in self._catalog:
            self.events_processed += 1
            eligibility = next(
                d for d in context.eligibility_decisions
                if d.strategy_id == canon.strategy_id and d.strategy_version == canon.strategy_version  # type: ignore[attr-defined]
            )
            trace_id = FP(market_event_id, canon.strategy_id, canon.strategy_version)
            configuration_fingerprint = FP(trace_id, ve_brain.VE_BRAIN_VERSION)
            event_identity = _event_identity(
                trace_id=trace_id, market_event_id=market_event_id, bar=bar,
                configuration_fingerprint=configuration_fingerprint,
            )
            traces: list[NodeTrace] = [
                NodeTrace(
                    trace_id=trace_id, node_name="N1", input_fingerprint=context.context_id,
                    output=context.n1_output_fp, reason_codes=("CACHED_FROM_M15_CONTEXT",), latency_seconds=0.0,
                    component_version=ve_brain.N1_CONTRACT_VERSION,
                ),
                NodeTrace(
                    trace_id=trace_id, node_name="Router", input_fingerprint=context.context_id,
                    output=FP(str(eligibility.eligible), str(eligibility.mode.value)),  # type: ignore[attr-defined]
                    reason_codes=eligibility.reason_codes, latency_seconds=0.0,  # type: ignore[attr-defined]
                    component_version=ve_brain.ROUTER_VERSION,
                ),
            ]

            if not eligibility.eligible:  # type: ignore[attr-defined]
                self._record(
                    category=LiveShadowCategory.LIVE_SHADOW_NO_TRADE, event_identity=event_identity,
                    strategy_id=canon.strategy_id, strategy_version=canon.strategy_version, bar=bar,
                    terminal_reason="NO_DECISION", node_traces=tuple(traces),
                )
                continue

            if context.atr is None or context.entry_price is None:
                self._record(
                    category=LiveShadowCategory.LIVE_SHADOW_NO_TRADE, event_identity=event_identity,
                    strategy_id=canon.strategy_id, strategy_version=canon.strategy_version, bar=bar,
                    terminal_reason="ATR_HISTORY_INSUFFICIENT", node_traces=tuple(traces),
                )
                continue

            cost_components = None
            cost_unavailable_reason = "COST_MODEL_UNAVAILABLE"
            if (self._expected_cost_model_fingerprint is not None
                    and self._expected_cost_model_fingerprint != cost_model_configuration_fingerprint()):
                cost_unavailable_reason = "COST_MODEL_FINGERPRINT_MISMATCH"
            else:
                try:
                    cost_components = resolve_cost_components(tier="BASE")
                except CostModelUnavailableError:
                    pass

            if cost_components is None:
                self._record(
                    category=LiveShadowCategory.LIVE_SHADOW_NO_TRADE, event_identity=event_identity,
                    strategy_id=canon.strategy_id, strategy_version=canon.strategy_version, bar=bar,
                    terminal_reason=cost_unavailable_reason, node_traces=tuple(traces),
                )
                continue

            side = side_from_strategy(canon)
            bias_direction = "LONG" if side == 1 else "SHORT"
            entry_price = context.entry_price
            geometry = self._strategy_geometry_overrides.get(canon.strategy_id)
            stop_atr_multiplier = geometry.stop_atr_multiplier if geometry is not None else _DEFAULT_STOP_ATR_MULTIPLIER
            stop_price = entry_price - stop_atr_multiplier * context.atr

            if legacy_shadow_only:
                self._record(
                    category=LiveShadowCategory.LIVE_SHADOW_EVENT, event_identity=event_identity,
                    strategy_id=canon.strategy_id, strategy_version=canon.strategy_version, bar=bar,
                    terminal_reason="LEGACY_SHADOW_TELEMETRY_AUTHORITY_NOT_NEW_BRAIN", node_traces=tuple(traces),
                )
                continue

            chain_result = _get_chain_result(
                side=side, trace_id=trace_id, configuration_fingerprint=configuration_fingerprint,
                strategy_id=canon.strategy_id, strategy_version=canon.strategy_version,
            )
            traces.append(NodeTrace(
                trace_id=trace_id, node_name="TowerN2",
                input_fingerprint=chain_result.n2_node_input_fingerprint or FP(market_event_id, "n2-unavailable"),
                output=FP(str(chain_result.bias_available), chain_result.n2_output_fingerprint or "", str(side)),
                reason_codes=chain_result.reason_codes, latency_seconds=0.0,
                component_version=chain_result.n2_contract_version or chain_result.tower_version,
            ))
            traces.append(NodeTrace(
                trace_id=trace_id, node_name="TowerN3",
                input_fingerprint=chain_result.n3_node_input_fingerprint or FP(market_event_id, "n3-unavailable"),
                output=FP(str(chain_result.market_map_available), str(chain_result.levels_available)),
                reason_codes=chain_result.reason_codes, latency_seconds=0.0,
                component_version=chain_result.n3_contract_version or chain_result.tower_version,
            ))
            traces.append(NodeTrace(
                trace_id=trace_id, node_name="TowerN4",
                input_fingerprint=chain_result.n4_node_input_fingerprint or FP(market_event_id, "n4-unavailable"),
                output=FP(str(chain_result.confirmation_available), chain_result.n4_event_fingerprint or ""),
                reason_codes=chain_result.reason_codes, latency_seconds=0.0,
                component_version=chain_result.n4_contract_version or chain_result.tower_version,
            ))
            traces.append(NodeTrace(
                trace_id=trace_id, node_name="CostModel", input_fingerprint=cost_model_configuration_fingerprint(),
                output=FP(str(cost_components.full_spread_price), str(cost_components.entry_slippage_price)),
                reason_codes=(), latency_seconds=0.0,
                component_version=f"{SHADOW_COST_MODEL_VERSION}:{CALIBRATION_STATUS}",
            ))

            regime_label = eligibility.matched_regimes[0] if eligibility.matched_regimes else None  # type: ignore[attr-defined]
            probability_inputs = load_probability_inputs(canon.strategy_id, canon.strategy_version)
            target_kind = geometry.target_kind if geometry is not None else "rr"
            target_param = geometry.target_param if geometry is not None else PLACEHOLDER_TARGET_RR
            candidate = ve_brain.DecisionRequest(
                contract_id=ve_brain.INPUT_CONTRACT_ID, strategy_id=canon.strategy_id,
                strategy_version=canon.strategy_version, validation_status=canon.validation_status,
                strategy_family=canon.strategy_family,
                strategy_policy_fingerprint=ve_brain.strategy_policy_fingerprint(canon),
                market_event_id=market_event_id, regime_fingerprint=eligibility.regime_fingerprint,  # type: ignore[attr-defined]
                market_state_ref=context.n1_output_fp, regime_label=regime_label, bias_direction=bias_direction,
                market_map_available=chain_result.market_map_available,
                levels_available=chain_result.levels_available,
                confirmation_available=chain_result.confirmation_available,
                entry_price=entry_price, stop_price=stop_price, target_kind=target_kind,
                target_param=target_param, holding_window=canon.holding_window, atr=context.atr,
                probability_inputs=probability_inputs, full_spread_price=cost_components.full_spread_price,
                entry_slippage_price=cost_components.entry_slippage_price,
                exit_slippage_price=cost_components.exit_slippage_price, symbol=bar.symbol, timeframe=_TIMEFRAME,
                block_start=0, block_end=bar.ts_close, segment_id="live-m5", manifest_hash="live-m5-manifest",
                n1_contract_version=ve_brain.N1_CONTRACT_VERSION,
                raw_axis_schema_version=ve_brain.RAW_AXIS_SCHEMA_VERSION, router_version=ve_brain.ROUTER_VERSION,
                eligibility_policy_version=ELIGIBILITY_POLICY_VERSION,
                measurement_contract_version=ve_brain.MEASUREMENT_CONTRACT_VERSION,
                configuration_fingerprint=configuration_fingerprint,
            )
            response = ve_brain.decide_n6(candidate, eligibility)
            traces.append(NodeTrace(
                trace_id=trace_id, node_name="EV", input_fingerprint=FP(trace_id, "ev"),
                output=FP(str(response.expected_value_net)), reason_codes=(), latency_seconds=0.0,
                component_version=ve_brain.ENGINE_VERSION,
            ))
            traces.append(NodeTrace(
                trace_id=trace_id, node_name="N6", input_fingerprint=configuration_fingerprint,
                output=FP(str(response.decision), str(response.configuration_fingerprint)),
                reason_codes=response.reason_codes, latency_seconds=0.0, component_version=str(response.engine_version),
            ))

            terminal_reason = response.reason_codes[0] if response.reason_codes else "NO_DECISION"
            if response.decision not in ("TRADE", "SHADOW_TRADE_CANDIDATE"):
                self._record(
                    category=LiveShadowCategory.LIVE_SHADOW_NO_TRADE, event_identity=event_identity,
                    strategy_id=canon.strategy_id, strategy_version=canon.strategy_version, bar=bar,
                    terminal_reason=terminal_reason, node_traces=tuple(traces), decision=response,
                )
                continue

            provenance = DecisionProvenance(
                source=NEW_BRAIN_SOURCE, trace_id=trace_id, catalog_hash=ve_brain.CANONICAL_CATALOG_HASH,
                configuration_fingerprint=str(response.configuration_fingerprint),
            )
            if target_kind == "rr" and target_param is not None:
                target_price: float | None = entry_price + target_param * (entry_price - stop_price)
            elif target_kind == "price" and target_param is not None:
                target_price = target_param
            else:
                # target_kind == "none" (e.g. G0037's canonical time-stop exit): no price target exists --
                # None is the faithful representation, never an invented RR-derived value.
                target_price = None

            outcome_for_risk = NewBrainOutcome(
                event_identity=event_identity, strategy_id=canon.strategy_id,
                strategy_version=canon.strategy_version, node_traces=tuple(traces), decision=response,
                provenance=provenance, entry_price=entry_price, stop_price=stop_price,
                target_price=target_price,
            )

            circuit_state = load_persisted_circuit_state(self._state_store)
            risk_decision = submit_new_brain_candidate(
                outcome_for_risk, account=self._deps_factory.account(), portfolio=self._deps_factory.portfolio(),
                instrument=self._deps_factory.instrument(),
                risk_context=self._deps_factory.risk_context(as_of=bar.ts_close),
                risk_config=self._deps_factory.risk_config(), circuit_state=circuit_state,
            )
            shadow_result = attempt_shadow_execution(risk_decision, gate=self._gate)

            risk_trace = NodeTrace(
                trace_id=trace_id, node_name="RiskManager", input_fingerprint=trace_id,
                output=str(risk_decision.approved), reason_codes=risk_decision.reason_codes,
                latency_seconds=0.0, component_version="risk_gate_v1",
            )
            exec_trace = NodeTrace(
                trace_id=trace_id, node_name="ExecutionAdapter", input_fingerprint=trace_id,
                output=f"{shadow_result.reached_broker_gate}|{shadow_result.blocked}",
                reason_codes=() if shadow_result.blocked is False else (shadow_result.reason,),
                latency_seconds=0.0, component_version="broker_gate_v1",
            )
            self._telemetry_log.record_outcome(outcome_for_risk, extra_traces=(risk_trace, exec_trace))

            category = (
                LiveShadowCategory.LIVE_SHADOW_BLOCKED_AT_BROKER if shadow_result.reached_broker_gate
                else LiveShadowCategory.LIVE_SHADOW_CANDIDATE
            )
            self._shadow_journal.record(LiveShadowRecord(
                category=category, trace_id=trace_id, market_event_id=market_event_id,
                strategy_id=canon.strategy_id, as_of=bar.ts_close, terminal_reason_code=terminal_reason,
                decision=response.decision, reached_broker_gate=shadow_result.reached_broker_gate,
                broker_blocked=shadow_result.blocked, order_send_calls=0, orders_created=0, positions_created=0,
            ))

    def tick(self) -> bool:
        circuit_state = load_persisted_circuit_state(self._state_store)
        if circuit_state.state is not EngineState.READY:
            return False
        for bar in self._feed.poll():
            self.last_bar = bar
            self._process_m5_bar(bar)
        return True
