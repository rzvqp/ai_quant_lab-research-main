"""Three canonical PROVISIONAL_SHADOW_ONLY strategies -- CEO amendment 2026-08-18, sourced exclusively
from Alpha's canonical rerun records (commit `71ebd13`, `reports/n1_rerun/CANONICAL_SCREENING_REPORT.md` +
`CANONICAL_RERUN_RECORDS.json`, itself authorized by RT-N1-0001 `N1_HANDOFF_PASS` `5352570` + RT-N1-0002
`N1_INCREMENTAL_PASS` `6230ee5`): G0037 (TREND_UP pullback), G0184 (TREND_UP continuation), G0059
(TREND_UP momentum) -- the only 3 economically-distinct mechanisms that survived canonical N1/Router
regime-gated screening of the 355 hypotheses, all TREND_UP long.

**Every entry/stop/exit/holding/eligibility parameter below is read verbatim from `CANONICAL_RERUN_
RECORDS.json`'s own `spec` field for `CAND-G0037`/`CAND-G0184`/`CAND-G0059`, never approximated:**

| id | entry | stop | hold | exit_kind | exit_param | hsf | eval_run_hash |
|---|---|---|---|---|---|---|---|
| G0037 | pullback3 | atr2 | 40 | time | 40.0 | 88da9894aeb1 | 04bbc6511f98aa78 |
| G0184 | continuation | atr2 | 40 | rr | 3.0 | b81e81c9457c | 46f6934b38a8e3f9 |
| G0059 | momentum | atr2 | 40 | rr | 3.0 | 6f92d6eec429 | e9281125eba557d7 |

**Status: `INTEGRATION_BLOCKED_UNKNOWN_STRATEGY_CATALOG` (CEO stop-and-record decision, 2026-08-18).**

**The real, verified finding** (superseding an earlier, incorrect draft of this docstring that claimed
these strategies would resolve to `NO_TRADE`/`MISSING_PROBABILITY_INPUTS`): `ve_brain.decide_n6`
resolves the incoming `(strategy_id, strategy_version)` against its own INTERNAL, SEALED catalog
(`ve_brain.n6._SEALED_CATALOG`) -- four entries only (`trend_pullback`, `range_fade`, `trend_shadow`,
`trend_experimental`, every one `strategy_version="v1"`), baked into the installed package, not a
parameter, not populatable from outside. A real `decide_n6()` call for G0037 with EVERY OTHER field
maximally valid (`eligible=True`, N3/N4 available, market_map/levels available, and even a real,
well-formed `probability_inputs`) still returns `NO_TRADE`/`UNKNOWN_STRATEGY` -- this fires at step 3
of `decide_n6`'s own pipeline, strictly before eligibility, N3/N4 availability, or `probability_inputs`
are ever examined (steps 8+). Proven with 8 adversarial tests, including a control case using the real
`trend_pullback` contract to show the SAME test harness correctly reaches the LATER
`MISSING_OR_INVALID_ELIGIBILITY` check when the strategy_id IS in the sealed catalog -- see
`tests/test_sealed_catalog_blocker.py`, kept as permanent evidence, never relaxed to pass.

This means G0037/G0184/G0059 -- as `ve_brain.StrategyContract` instances built OUTSIDE the installed
package -- can **never** reach a `TRADE`/`SHADOW_TRADE_CANDIDATE` decision, or even the EV/
`probability_inputs` gate, through `ve_brain.decide_n6` as it exists today. This is NOT a data-
availability gap the way the old `MISSING_PROBABILITY_INPUTS` framing implied -- it is a structural
limitation of the installed `ve_brain` package itself. The only path forward is a new, versioned
`ve_brain` release that registers these three strategies in its own sealed catalog, followed by a
Red Team pass on that artifact. `ve_brain` itself was never modified by this package or by anything
else in this repo, and no local workaround was attempted -- consistent with this codebase's standing
"read an artifact with exactly the convention its producer wrote it with" discipline.

(The `probability_inputs` data-availability finding from the earlier draft remains TRUE as a secondary,
independent fact -- `CANONICAL_RERUN_RECORDS.json` still carries no per-strategy target/horizon
exit-type breakdown for any of the three -- but it is now moot: `UNKNOWN_STRATEGY` blocks these
strategies before that gate would ever be reached, even if the breakdown existed.)

Status at delivery: built and tested (geometry, feature flags, real-N1/Router-eligibility, the
adversarial blocker proof), each strategy behind its OWN feature flag, all three DEFAULT OFF. Not wired
into `entrypoint.py`'s default catalog -- `new_brain_live.n1_incremental.runtime_loop` continues using
`ve_brain.CANONICAL_STRATEGIES` unless a flag explicitly opts a strategy in, which would still produce
only `NO_TRADE`/`UNKNOWN_STRATEGY` telemetry today. No deployment, restart, or cutover performed."""
