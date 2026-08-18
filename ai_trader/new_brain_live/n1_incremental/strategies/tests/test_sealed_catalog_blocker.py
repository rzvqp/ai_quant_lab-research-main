"""RT-THREE-STRATEGY-0001 adversarial blocker proof -- CEO directive 2026-08-18
(`INTEGRATION_BLOCKED_UNKNOWN_STRATEGY_CATALOG`).

**The finding**: `ve_brain.decide_n6` resolves `(strategy_id, strategy_version)` against its
own INTERNAL, SEALED catalog (`ve_brain.n6._SEALED_CATALOG`) -- four entries only, all
`strategy_version="v1"`. G0037/G0184/G0059 are real, well-formed `ve_brain.StrategyContract`
instances (proven elsewhere -- `canonical_specs.py` -- to resolve `eligible=True` under the
real N1/Router chain), but they were never registered in `ve_brain`'s own package, so
`_SEALED_CATALOG.resolve(...)` returns `None` for all three, and `decide_n6` returns
`NO_TRADE`/`UNKNOWN_STRATEGY` at step 3 of its pipeline -- unconditionally, before
eligibility, N3/N4 availability, or `probability_inputs` are ever examined (steps 8+).

This is NOT the previously (incorrectly) documented `MISSING_PROBABILITY_INPUTS` gap --
that gate is never reached at all. `ve_brain` itself is never modified by this file or
anything else in this repo (verified read-only, `verify_pin`-style discipline throughout
this codebase) -- these tests only PROVE the blocker via the real, installed `decide_n6`,
never work around it.

**Method**: for each of the three strategies, build a `DecisionRequest` with EVERY OTHER
field maximally valid -- `eligible=True`, N3/N4 available, level/map available, and (in
one case) a real, well-formed `probability_inputs` -- so `UNKNOWN_STRATEGY` can only be
explained by the sealed-catalog rejection itself, never by some other missing input.
A control case using the REAL `trend_pullback` canonical strategy (which IS in the sealed
catalog) with a deliberately-broken eligibility proves the harness correctly reaches the
LATER `MISSING_OR_INVALID_ELIGIBILITY` check when the strategy_id IS recognized -- i.e.
the ordering difference is caused by catalog membership, not by anything else in this
test file's own construction."""

from __future__ import annotations

import ve_brain  # type: ignore[import-untyped]

from ai_trader.new_brain_live.n1_incremental.strategies import canonical_specs as cs

_THREE_CONTRACTS = (cs.CONTRACT_G0037, cs.CONTRACT_G0184, cs.CONTRACT_G0059)


def _request(
    canon: ve_brain.StrategyContract, *, market_map_available: bool = True, levels_available: bool = True,
    confirmation_available: bool = True, probability_inputs: object | None = None,
) -> ve_brain.DecisionRequest:
    """Every field maximally valid by construction -- the only lever this helper leaves
    open is the handful of args callers vary to prove `UNKNOWN_STRATEGY` is independent
    of them."""
    return ve_brain.DecisionRequest(
        contract_id=ve_brain.INPUT_CONTRACT_ID, strategy_id=canon.strategy_id,
        strategy_version=canon.strategy_version, validation_status=canon.validation_status,
        strategy_family=canon.strategy_family,
        strategy_policy_fingerprint=ve_brain.strategy_policy_fingerprint(canon),
        market_event_id="blocker-proof-event", regime_fingerprint="blocker-proof-fp",
        market_state_ref="blocker-proof-n1fp", regime_label="TREND_UP", bias_direction="LONG",
        market_map_available=market_map_available, levels_available=levels_available,
        confirmation_available=confirmation_available,
        entry_price=2400.0, stop_price=2396.0, target_kind="none", target_param=None,
        holding_window=canon.holding_window, atr=2.0, probability_inputs=probability_inputs,
        full_spread_price=0.1, entry_slippage_price=0.01, exit_slippage_price=0.01,
        symbol="XAUUSD", timeframe="M5", block_start=0, block_end=1,
        segment_id="blocker-proof-segment", manifest_hash="blocker-proof-manifest",
        n1_contract_version=ve_brain.N1_CONTRACT_VERSION,
        raw_axis_schema_version=ve_brain.RAW_AXIS_SCHEMA_VERSION, router_version=ve_brain.ROUTER_VERSION,
        eligibility_policy_version="blocker-proof-eligibility-v1",
        measurement_contract_version=ve_brain.MEASUREMENT_CONTRACT_VERSION,
        configuration_fingerprint="blocker-proof-cf",
    )


def _eligible_decision(canon: ve_brain.StrategyContract, *, eligible: bool = True) -> ve_brain.EligibilityDecision:
    return ve_brain.EligibilityDecision(
        strategy_id=canon.strategy_id, strategy_version=canon.strategy_version,
        market_event_id="blocker-proof-event", regime_fingerprint="blocker-proof-fp",
        router_version=ve_brain.ROUTER_VERSION, eligible=eligible,
        mode=ve_brain.RoutingMode.NORMAL if eligible else ve_brain.RoutingMode.INELIGIBLE,
        matched_regimes=("TREND_UP",) if eligible else (), reason_codes=("ROUTER_ELIGIBLE",) if eligible else (),
    )


def _real_probability_inputs() -> ve_brain.ProbabilityInputs:
    """A real, well-formed `ProbabilityInputs` -- used ONLY to prove `UNKNOWN_STRATEGY`
    fires even when this normally-missing gate would otherwise be satisfied. Not evidence
    of real target/horizon exit-type data existing for these strategies (it does not --
    see `canonical_specs.py`'s own module docstring)."""
    cell = ve_brain.OutcomeCell(n=100, n_target=40, n_horizon=60, sum_horizon_R=5.0)
    return ve_brain.ProbabilityInputs(hierarchy=(ve_brain.HierarchyLevel(cell=cell, siblings=()),), credibility=0.8)


def test_g0037_unknown_strategy_with_everything_else_maximally_valid() -> None:
    canon = cs.CONTRACT_G0037
    response = ve_brain.decide_n6(_request(canon), _eligible_decision(canon, eligible=True))
    assert response.decision == "NO_TRADE"
    assert response.reason_codes == ("UNKNOWN_STRATEGY",)


def test_g0184_unknown_strategy_with_everything_else_maximally_valid() -> None:
    canon = cs.CONTRACT_G0184
    response = ve_brain.decide_n6(_request(canon), _eligible_decision(canon, eligible=True))
    assert response.decision == "NO_TRADE"
    assert response.reason_codes == ("UNKNOWN_STRATEGY",)


def test_g0059_unknown_strategy_with_everything_else_maximally_valid() -> None:
    canon = cs.CONTRACT_G0059
    response = ve_brain.decide_n6(_request(canon), _eligible_decision(canon, eligible=True))
    assert response.decision == "NO_TRADE"
    assert response.reason_codes == ("UNKNOWN_STRATEGY",)


def test_unknown_strategy_fires_even_when_eligibility_decision_is_missing() -> None:
    """`eligibility=None` would normally cause `MISSING_OR_INVALID_ELIGIBILITY` (step 8) --
    proves the catalog rejection (step 3) happens strictly BEFORE that check."""
    canon = cs.CONTRACT_G0037
    response = ve_brain.decide_n6(_request(canon), None)
    assert response.decision == "NO_TRADE"
    assert response.reason_codes == ("UNKNOWN_STRATEGY",)


def test_unknown_strategy_fires_even_when_router_marked_it_ineligible() -> None:
    """An ineligible Router decision would normally ALSO reach `MISSING_OR_INVALID_ELIGIBILITY`
    -- still never reached, because the catalog check is unconditional and earlier."""
    canon = cs.CONTRACT_G0184
    response = ve_brain.decide_n6(_request(canon), _eligible_decision(canon, eligible=False))
    assert response.decision == "NO_TRADE"
    assert response.reason_codes == ("UNKNOWN_STRATEGY",)


def test_unknown_strategy_fires_even_when_n3_n4_are_unavailable() -> None:
    """`market_map_available=False`/`confirmation_available=False` would normally produce
    `MISSING_LEVEL_INPUT`/`MISSING_CONFIRMATION` (step 8) -- still masked by step 3."""
    canon = cs.CONTRACT_G0059
    request = _request(canon, market_map_available=False, levels_available=False, confirmation_available=False)
    response = ve_brain.decide_n6(request, _eligible_decision(canon, eligible=True))
    assert response.decision == "NO_TRADE"
    assert response.reason_codes == ("UNKNOWN_STRATEGY",)


def test_unknown_strategy_fires_even_when_probability_inputs_are_present() -> None:
    """The decisive proof against the ORIGINAL (incorrect) `MISSING_PROBABILITY_INPUTS`
    disclosure: even with a real, well-formed `probability_inputs` supplied -- the gate
    that WOULD matter if step 3 didn't already block first -- the decision is still
    `UNKNOWN_STRATEGY`, never `MISSING_PROBABILITY_INPUTS`, never a real EV outcome."""
    canon = cs.CONTRACT_G0037
    request = _request(canon, probability_inputs=_real_probability_inputs())
    response = ve_brain.decide_n6(request, _eligible_decision(canon, eligible=True))
    assert response.decision == "NO_TRADE"
    assert response.reason_codes == ("UNKNOWN_STRATEGY",)


def test_control_real_canonical_strategy_reaches_the_later_eligibility_check() -> None:
    """Control case: `trend_pullback` (strategy_version="v1") IS in the sealed catalog.
    With the SAME test harness, the SAME kind of deliberately-missing eligibility, the
    decision correctly reaches step 8 (`MISSING_OR_INVALID_ELIGIBILITY`), never
    `UNKNOWN_STRATEGY` -- proving the difference observed above is caused by sealed-catalog
    membership, not by anything else in `_request`'s own construction."""
    canon = next(c for c in ve_brain.CANONICAL_STRATEGIES if c.strategy_id == "trend_pullback")
    response = ve_brain.decide_n6(_request(canon), None)
    assert response.decision == "NO_TRADE"
    assert response.reason_codes == ("MISSING_OR_INVALID_ELIGIBILITY",)
    assert response.reason_codes != ("UNKNOWN_STRATEGY",)
