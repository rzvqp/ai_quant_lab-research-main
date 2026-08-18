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

**A genuine, disclosed finding, not a workaround**: `ve_brain.ProbabilityInputs.hierarchy` requires
`OutcomeCell(n, n_target, n_horizon, sum_horizon_R)` -- a per-strategy breakdown of target-hit vs.
horizon(time-stop)-exit trade counts. The canonical Alpha records carry `n`/`win_rate`/`EV_net_avg_R`/`PF`
at the AGGREGATE level only -- no such breakdown exists anywhere in `CANONICAL_RERUN_RECORDS.json` for
any of the three. Building `OutcomeCell` from this data would mean INVENTING the missing counts, which
the CEO's own instruction explicitly forbids ("Nu inventa sau aproxima parametrii"). Per the CEO's own
equally explicit fallback ("lipsă/invaliditate probability inputs → NO_TRADE, fără fallback"), this
package therefore makes NO change to `probability_source.load_probability_inputs` at all -- it continues
returning `None` for every strategy, these three included, exactly as it already does for the four
existing canonical strategies. All three therefore resolve to `NO_TRADE`/`MISSING_PROBABILITY_INPUTS`
through `ve_brain`'s own already-established gate until a genuine target/horizon breakdown exists --
observational shadow telemetry only, structurally incapable of producing a TRADE decision today, which is
exactly what `PROVISIONAL_SHADOW_ONLY` means.

Status at delivery: built and tested, each behind its OWN feature flag, all three DEFAULT OFF. Not wired
into `entrypoint.py`'s default catalog -- `new_brain_live.n1_incremental.runtime_loop` continues using
`ve_brain.CANONICAL_STRATEGIES` unless a flag explicitly opts a strategy in."""
