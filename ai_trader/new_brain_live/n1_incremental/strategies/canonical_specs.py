"""The three `ve_brain.StrategyContract` instances themselves, plus their per-strategy execution
geometry (stop-ATR-multiplier, target_kind/target_param) -- `ve_brain.StrategyContract` has no fields
for these (the existing 4 canonical strategies all share ONE hardcoded ATR-multiplier-of-1 stop and ONE
module-level `PLACEHOLDER_TARGET_RR` target in `bridge.py`/`m5_decision_loop.py`). `StrategyGeometry`
itself is defined in `dual_clock.m5_decision_loop` (the actual consumer, via the additive
`strategy_geometry_overrides` constructor param) -- this module only imports that type and builds the
concrete `GEOMETRY` registry for these three strategies, so `dual_clock` never depends on this
higher-level `strategies` package.

Every field traces directly to `CANONICAL_RERUN_RECORDS.json` (see this package's own `__init__.py`
docstring for the full spec table) -- `entry_rule`/`invalidation_rule`/`exit_rule` below are the EXACT
canonical spec strings (`"pullback3"`/`"atr2"`/`"time:40.0"` etc.), not free-text description."""

from __future__ import annotations

import os

import ve_brain  # type: ignore[import-untyped]

from ai_trader.new_brain_live.dual_clock.m5_decision_loop import StrategyGeometry

STRATEGY_ID_G0037 = "g0037_trend_up_pullback_long"
STRATEGY_ID_G0184 = "g0184_trend_up_continuation_long"
STRATEGY_ID_G0059 = "g0059_trend_up_momentum_long"

INTEGRATION_STATUS = "INTEGRATION_BLOCKED_UNKNOWN_STRATEGY_CATALOG"
"""CEO stop-and-record decision, 2026-08-18 -- see `strategies/__init__.py`'s own docstring and
`tests/test_sealed_catalog_blocker.py` for the full, reproducible finding: `ve_brain.decide_n6`'s
internal sealed catalog only recognizes 4 strategies, none of which are these three. Every strategy_id
below maps to this SAME status -- there is no per-strategy variant, since the blocking mechanism
(catalog-membership rejection at decide_n6's step 3) is identical and unconditional for all three."""

INTEGRATION_STATUS_BY_STRATEGY_ID: dict[str, str] = {
    STRATEGY_ID_G0037: INTEGRATION_STATUS,
    STRATEGY_ID_G0184: INTEGRATION_STATUS,
    STRATEGY_ID_G0059: INTEGRATION_STATUS,
}

FLAG_ENV_VAR_G0037 = "AI_TRADER_STRATEGY_G0037_ENABLED"
FLAG_ENV_VAR_G0184 = "AI_TRADER_STRATEGY_G0184_ENABLED"
FLAG_ENV_VAR_G0059 = "AI_TRADER_STRATEGY_G0059_ENABLED"
_FLAG_ON_VALUE = "ON"
"""Fails closed to OFF for anything other than this exact literal -- unset, empty, a typo -- same
discipline as `runtime_loop.resolve_runtime_mode`."""

_TREND_UP = (ve_brain.SemanticRegime.TREND_UP,)

CONTRACT_G0037 = ve_brain.StrategyContract(
    strategy_id=STRATEGY_ID_G0037, strategy_family="TREND_UP_PULLBACK",
    allowed_regimes=_TREND_UP, allowed_directions=("LONG",), arming_regimes=(), trigger_transition=None,
    minimum_regime_confidence=0.0, required_N2_bias=None, required_N3_map=True, required_N4_confirmation=None,
    entry_rule="pullback3", invalidation_rule="atr2", exit_rule="time:40.0", holding_window=40,
    validation_status=ve_brain.ValidationStatus.SHADOW_ELIGIBLE, strategy_version="canonical-71ebd13-88da9894aeb1",
    measurement_contract_version="alpha-canonical-rerun-71ebd13",
)
CONTRACT_G0184 = ve_brain.StrategyContract(
    strategy_id=STRATEGY_ID_G0184, strategy_family="TREND_UP_CONTINUATION",
    allowed_regimes=_TREND_UP, allowed_directions=("LONG",), arming_regimes=(), trigger_transition=None,
    minimum_regime_confidence=0.0, required_N2_bias=None, required_N3_map=True, required_N4_confirmation=None,
    entry_rule="continuation", invalidation_rule="atr2", exit_rule="rr:3.0", holding_window=40,
    validation_status=ve_brain.ValidationStatus.SHADOW_ELIGIBLE, strategy_version="canonical-71ebd13-b81e81c9457c",
    measurement_contract_version="alpha-canonical-rerun-71ebd13",
)
CONTRACT_G0059 = ve_brain.StrategyContract(
    strategy_id=STRATEGY_ID_G0059, strategy_family="TREND_UP_MOMENTUM",
    allowed_regimes=_TREND_UP, allowed_directions=("LONG",), arming_regimes=(), trigger_transition=None,
    minimum_regime_confidence=0.0, required_N2_bias=None, required_N3_map=True, required_N4_confirmation=None,
    entry_rule="momentum", invalidation_rule="atr2", exit_rule="rr:3.0", holding_window=40,
    validation_status=ve_brain.ValidationStatus.SHADOW_ELIGIBLE, strategy_version="canonical-71ebd13-6f92d6eec429",
    measurement_contract_version="alpha-canonical-rerun-71ebd13",
)

GEOMETRY: dict[str, StrategyGeometry] = {
    # G0037: exit_kind="time" -- no price target in the canonical spec at all. target_kind="none" is the
    # FAITHFUL representation (ve_brain.contracts._require only accepts "rr"/"price"/"none") -- inventing
    # an RR value here to force a trade signal is exactly the "aproximare" the CEO's instruction forbids.
    STRATEGY_ID_G0037: StrategyGeometry(stop_atr_multiplier=2.0, target_kind="none", target_param=None),
    STRATEGY_ID_G0184: StrategyGeometry(stop_atr_multiplier=2.0, target_kind="rr", target_param=3.0),
    STRATEGY_ID_G0059: StrategyGeometry(stop_atr_multiplier=2.0, target_kind="rr", target_param=3.0),
}

_ALL: tuple[tuple[str, ve_brain.StrategyContract, str], ...] = (
    (STRATEGY_ID_G0037, CONTRACT_G0037, FLAG_ENV_VAR_G0037),
    (STRATEGY_ID_G0184, CONTRACT_G0184, FLAG_ENV_VAR_G0184),
    (STRATEGY_ID_G0059, CONTRACT_G0059, FLAG_ENV_VAR_G0059),
)


def flag_enabled(env_var: str, env: dict[str, str] | None = None) -> bool:
    source = os.environ if env is None else env
    return source.get(env_var, "") == _FLAG_ON_VALUE


def enabled_catalog_additions(env: dict[str, str] | None = None) -> tuple[ve_brain.StrategyContract, ...]:
    """Returns only the contracts whose OWN flag is explicitly ON -- empty tuple if all three flags are
    unset (the real, current, default state)."""
    return tuple(contract for _sid, contract, flag in _ALL if flag_enabled(flag, env))


def extended_catalog(env: dict[str, str] | None = None) -> tuple[ve_brain.StrategyContract, ...]:
    """`ve_brain.CANONICAL_STRATEGIES` plus whichever of the three flags are ON -- with every flag OFF
    (the default), this is BYTE-IDENTICAL to `ve_brain.CANONICAL_STRATEGIES` itself."""
    return ve_brain.CANONICAL_STRATEGIES + enabled_catalog_additions(env)  # type: ignore[no-any-return]
