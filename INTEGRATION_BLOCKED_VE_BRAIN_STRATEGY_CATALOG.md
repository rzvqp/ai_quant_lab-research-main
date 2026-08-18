# INTEGRATION_BLOCKED_VE_BRAIN_STRATEGY_CATALOG

**Date**: 2026-08-18
**Division**: AI Trader
**Scope**: G0037 (TREND_UP pullback long), G0184 (TREND_UP continuation long), G0059 (TREND_UP momentum long)
**Decision**: CEO stop-and-record, 2026-08-18 -- stop the background full regression, do not continue an
observational-only delivery, record the blocker, wait for a versioned `ve_brain` catalog artifact + Red Team
PASS before resuming.

## 1. Regression / LIVE_SHADOW status

- No `pytest` process is running anywhere on the machine at the time of this report (verified via a live
  process listing). The background full-`ai_trader/` regression referenced from the prior turn is not active;
  there was nothing further to stop.
- `LIVE_SHADOW`'s Windows Scheduled Task (`AITraderLiveShadow`) is confirmed `Running`, untouched (read-only
  `Get-ScheduledTask` check). Its two `entrypoint.py` processes are alive and were not stopped, signaled, or
  restarted at any point during this work.
- No `set_authority` call, no Scheduled Task modification, no deployment, no cutover was performed.

## 2. The finding

`ve_brain.decide_n6` resolves the incoming `(strategy_id, strategy_version)` against its own **internal,
sealed** catalog (`ve_brain.n6._SEALED_CATALOG`) -- not a parameter, not populatable from outside the
installed package. A real `decide_n6()` call for each of G0037/G0184/G0059, with every other input maximally
valid (`eligible=True`, N3/N4 available, market_map/levels available, and in one case a real, well-formed
`probability_inputs`), returns:

```
decision: NO_TRADE
reason_codes: ('UNKNOWN_STRATEGY',)
```

This fires at **step 3** of `decide_n6`'s pipeline -- strictly before eligibility (step 8), N3/N4 availability
(step 8), or `probability_inputs` (step 8) are ever examined. None of the three strategies can reach a
`TRADE`/`SHADOW_TRADE_CANDIDATE` decision, or even the EV/`probability_inputs` gate, through the installed
`ve_brain` package as it exists today.

This **corrects** an earlier, incorrect claim (in this session, caught before it was reported as final) that
these strategies would resolve to `NO_TRADE`/`MISSING_PROBABILITY_INPUTS` -- that gate is never reached at
all; `UNKNOWN_STRATEGY` is unconditional and strictly earlier. `ve_brain` itself was never modified, and no
local workaround (e.g. monkeypatching the sealed catalog) was attempted.

## 3. Reproducible adversarial proof

`ai_trader/new_brain_live/n1_incremental/strategies/tests/test_sealed_catalog_blocker.py` -- 8 tests, all
passing, mypy `--strict` clean:

| test | proves |
|---|---|
| `test_g003{7,4}_unknown_strategy_with_everything_else_maximally_valid` (x3, one per strategy) | `UNKNOWN_STRATEGY` with eligible=True, N3/N4 available |
| `test_unknown_strategy_fires_even_when_eligibility_decision_is_missing` | fires before the `eligibility is None` check |
| `test_unknown_strategy_fires_even_when_router_marked_it_ineligible` | fires before the eligibility-mismatch check |
| `test_unknown_strategy_fires_even_when_n3_n4_are_unavailable` | fires before the N3/N4 checks |
| `test_unknown_strategy_fires_even_when_probability_inputs_are_present` | fires before the `probability_inputs` check -- direct rebuttal of the earlier incorrect claim |
| `test_control_real_canonical_strategy_reaches_the_later_eligibility_check` | control: the REAL `trend_pullback` (in the sealed catalog) correctly reaches `MISSING_OR_INVALID_ELIGIBILITY` with the SAME harness -- proves the difference is sealed-catalog membership, not test construction |

Kept as permanent evidence; never to be relaxed to pass.

## 4. `ve_brain`'s internal sealed catalog -- exact inventory

Installed package (read-only inspection, `importlib.metadata`):

| field | value |
|---|---|
| package name | `ve_brain` |
| installed version | `0.1.3` |
| package location | `<venv>\Lib\site-packages\ve_brain\__init__.py` |
| `ve_brain/n6.py` SHA-256 (installed, base64) | `7DQbF16vICeGzfEoGkNS252NP8KlPpT1JgBvSvNdYzM` |
| `ve_brain/__init__.py` SHA-256 (installed, base64) | `Pd-UXOXtyqd02ABYcApgcHWrYbOPOWAkpwTNt1YJWd0` |
| committed wheel path | `ai_quant_lab-wp5b\ve_brain\release\ve_brain-0.1.3-py3-none-any.whl` |
| committed wheel SHA-256 | `edd208ad6c2c943b17a11759ece1fdf5ab2a7025779b191764c383ecce987d11` |

Cross-referenced against this repo's own pre-existing `BrainArtifactPin` schema
(`ai_trader/mandate2_readiness/artifact_pin.py`), which already carries `catalog_version`/`catalog_hash`
fields for exactly this purpose.

`ve_brain.n6._SEALED_CATALOG` (`SealedRegistry`, `sealed=True`):

| strategy_id | strategy_version | strategy_family | validation_status |
|---|---|---|---|
| `trend_pullback` | `v1` | `TREND_PULLBACK` | `RATIFIED` |
| `range_fade` | `v1` | `RANGE_MEAN_REVERSION` | `RATIFIED` |
| `trend_shadow` | `v1` | `TREND_PULLBACK` | `SHADOW_ELIGIBLE` |
| `trend_experimental` | `v1` | `TREND_PULLBACK` | `EXPERIMENTAL` |

- `catalog_version`: `ve-canonical-catalog-v1`
- `content_hash`: `37b95393df85dc2b`
- All four share `measurement_contract_version="canonical-evaluator-v2.7.66-A2"`.

G0037/G0184/G0059 use `strategy_version` values derived from Alpha's canonical rerun (e.g.
`canonical-71ebd13-88da9894aeb1`) -- structurally impossible to collide with `"v1"`, and in any case absent
from `_SEALED_CATALOG._by_key` entirely.

## 5. Status marker

All three strategies marked, in code (`canonical_specs.INTEGRATION_STATUS` /
`INTEGRATION_STATUS_BY_STRATEGY_ID`) and in the package docstring (`strategies/__init__.py`):

```
INTEGRATION_BLOCKED_UNKNOWN_STRATEGY_CATALOG
```

## 6. Safety invariants (re-verified)

- All three strategy feature flags (`AI_TRADER_STRATEGY_G00{37,184,59}_ENABLED`) default OFF -- exact-literal
  `"ON"` required, fails closed for unset/empty/any other value (`canonical_specs.flag_enabled`, tested).
- `AI_TRADER_RUNTIME_MODE` still defaults to `LEGACY_M15` -- unaffected by this segment's work.
- `BrokerOrderSubmissionGate.enabled` defaults `False` (`ai_trader/mandate2_readiness/broker_gate.py:57`,
  re-verified by direct read this session). `order_send_calls=0` throughout every code path touched.
- No deployment, restart, or cutover was performed anywhere in this session.

## 7. Commits

- Main worktree (`ai-trader-implementation`): `3ad9a66` -- final runtime wiring checkpoint (unrelated to the
  blocker; final N1-incremental/dual-clock wiring, tested, mypy clean, committed as-is per CEO instruction to
  preserve finished work).
- Strategies worktree (`ai-trader-three-strategies`, based on `9f0c13c`): `89ea544` (geometry + shared-Tower-
  call wiring + blocker discovery) followed by the adversarial-proof/documentation-correction commit
  containing this report.

## 8. Next step

After a new, versioned `ve_brain` release registers G0037/G0184/G0059 in its own sealed catalog, and Red Team
issues a PASS on that artifact: resume exactly here (geometry wiring and feature-flag plumbing are already
built and tested), then run the ONE required final `pytest ai_trader/` regression on the combined commit
(request-scoped time + dual-clock M15/M5 + N1 incremental hydration + the three strategies), and deliver
`READY_FOR_THREE_STRATEGY_N1_INCREMENTAL_DUAL_CLOCK_CUTOVER_REVIEW`.
