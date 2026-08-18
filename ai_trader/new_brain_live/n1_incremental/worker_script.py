"""Runs EXCLUSIVELY under `.alpha_n1_venv`'s own interpreter, as a fresh subprocess per invocation --
never imported in-process anywhere else. Imports ONLY the standard library and `ve_n1_replay`; never
`ai_trader.*` (the real one, from this repo) -- this file has no way to reach it, deliberately, so it
stays safe to run even though `ve_n1_replay` itself vendors a namespace-colliding `ai_trader.n1_replay`
internally (see this package's own `__init__.py` docstring).

Protocol: exactly one JSON object read from stdin, exactly one JSON object written to stdout, process
exits. No persistent state, no socket, no handshake -- N1 hydration/M15-refresh calls happen at most
every ~15 minutes in production, so a fresh interpreter per call costs nothing that matters, and it is
categorically simpler (and therefore more auditable) than a persistent server the tower worker needed
for its own, much higher-frequency, N2/N3/N4 calls."""

from __future__ import annotations

import base64
import json
import pickle
import sys
from typing import Any

import ve_n1_replay as n1r  # type: ignore[import-not-found]
"""Deliberately unresolvable under the main venv's own mypy run -- `ve_n1_replay` is never installed
there (see this package's own `__init__.py`). Runtime-verified instead: this exact file is exercised
end-to-end via a real subprocess call under `.alpha_n1_venv` in `tests/test_client.py`."""


def _bar_from_dict(d: dict[str, Any]) -> Any:
    return n1r.Bar(
        symbol=d["symbol"], ts_open=d["ts_open"], ts_close=d["ts_close"], open=d["open"],
        high=d["high"], low=d["low"], close=d["close"], volume=d.get("volume"),
        is_backfilled=d.get("is_backfilled", False),
    )


def _bar_to_dict(bar: Any) -> dict[str, Any]:
    return {
        "symbol": bar.symbol, "ts_open": bar.ts_open, "ts_close": bar.ts_close, "open": bar.open,
        "high": bar.high, "low": bar.low, "close": bar.close, "volume": bar.volume,
        "is_backfilled": bar.is_backfilled,
    }


def _axes_to_dict(axes: Any) -> dict[str, Any]:
    return {
        "is_compressed": axes.is_compressed, "is_displacement": axes.is_displacement,
        "direction": axes.direction, "structure": axes.structure,
        "volatility_state": axes.volatility_state, "n1_contract_version": axes.n1_contract_version,
        "raw_axis_schema_version": axes.raw_axis_schema_version,
    }


def _eligibility_to_dict(d: Any) -> dict[str, Any]:
    return {
        "strategy_id": d.strategy_id, "strategy_version": d.strategy_version,
        "market_event_id": d.market_event_id, "regime_fingerprint": d.regime_fingerprint,
        "router_version": d.router_version, "eligible": d.eligible, "mode": d.mode.value,
        "matched_regimes": list(d.matched_regimes), "reason_codes": list(d.reason_codes),
    }


def _identity_to_dict(identity: Any) -> dict[str, Any]:
    return {
        "implementation_commit": identity.implementation_commit,
        "wrapped_runtime_commit": identity.wrapped_runtime_commit,
        "ve_brain_version": identity.ve_brain_version,
        "ve_brain_wheel_sha256": identity.ve_brain_wheel_sha256,
        "detector_source_commit": identity.detector_source_commit,
        "detector_configuration_fingerprint": identity.detector_configuration_fingerprint,
        "n1_contract_version": identity.n1_contract_version, "router_version": identity.router_version,
        "raw_axis_schema_version": identity.raw_axis_schema_version,
        "n1_replay_schema_version": identity.n1_replay_schema_version,
        "symbol": identity.symbol, "timeframe": identity.timeframe,
        "bar_interval_seconds": identity.bar_interval_seconds, "fingerprint": identity.fingerprint(),
    }


def _result_to_dict(result: Any) -> dict[str, Any]:
    return {
        "raw_axes": _axes_to_dict(result.raw_axes),
        "applicable_regimes": sorted(str(r) for r in result.applicable_regimes),
        "eligibility_decisions": [_eligibility_to_dict(d) for d in result.eligibility_decisions],
        "n1_contract_version": result.n1_contract_version, "router_version": result.router_version,
        "detector_configuration_fingerprint": result.detector_configuration_fingerprint,
        "input_data_identity": result.input_data_identity, "output_fingerprint": result.output_fingerprint,
        "last_closed_bar": _bar_to_dict(result.last_closed_bar),
        "reason_codes": list(result.reason_codes), "regime_axes_status": list(result.regime_axes_status),
        "availability_status": result.availability_status,
        "n1_output_fingerprint": result.n1_output_fingerprint,
        "router_output_fingerprint": result.router_output_fingerprint,
        "evaluation_identity": _identity_to_dict(result.evaluation_identity),
    }


def _snapshot_to_blob(snapshot: Any) -> str:
    """`N1IncrementalSnapshot.last_result` is a real, internally-typed object (`ai_trader.n1_replay.
    types.N1ReplayResult`, vendored inside `ve_n1_replay` itself) -- empirically confirmed that `engine.
    restore()` accepts a hand-reconstructed dict or `None` there WITHOUT raising, but a SUBSEQUENT `observe_
    closed_bar()` call then fails deep inside the library (`TypeError: 'int' object is not callable`),
    because the engine's own continuity logic dereferences it as a real object on the next bar. Rather
    than reverse-engineer that internal shape (fragile, breaks silently on any future library-internal
    change), this hands the ENTIRE snapshot round-trip to `pickle` -- both the write and the read happen in
    the SAME venv, same `ve_n1_replay`/`ve_brain` versions, every time (this file only ever runs under
    `.alpha_n1_venv`), so `pickle` is the correct tool here, not a JSON reimplementation of the library's
    own internal object graph. The blob is opaque to every OTHER file in this repo (`client.py` only ever
    stores and replays it back unchanged) -- never unpickled anywhere outside this one worker process, and
    never sourced from anything other than this same worker's own prior output."""
    return base64.b64encode(pickle.dumps(snapshot)).decode("ascii")


def _snapshot_from_blob(blob: str) -> Any:
    return pickle.loads(base64.b64decode(blob))


def _handle(request: dict[str, Any]) -> dict[str, Any]:
    engine = n1r.N1IncrementalReplayEngine(
        symbol=request["symbol"], timeframe=request["timeframe"],
        bar_interval_seconds=request["bar_interval_seconds"],
        implementation_commit=request["implementation_commit"],
        max_staleness_seconds=request.get("max_staleness_seconds"),
    )

    restored = False
    restore_rejected_reason: str | None = None
    restore_snapshot_blob = request.get("restore_snapshot_blob")
    if restore_snapshot_blob is not None:
        try:
            engine.restore(_snapshot_from_blob(restore_snapshot_blob))
            restored = True
        except n1r.IncompatibleSnapshotError as exc:
            restore_rejected_reason = f"IncompatibleSnapshotError: {exc}"
        except n1r.N1ReplayError as exc:
            restore_rejected_reason = f"{type(exc).__name__}: {exc}"
        except (pickle.UnpicklingError, EOFError, AttributeError, ValueError) as exc:
            restore_rejected_reason = f"UnpicklableSnapshot: {type(exc).__name__}: {exc}"

    bars_processed = 0
    last_result = None
    try:
        for bar_dict in request["bars"]:
            bar = _bar_from_dict(bar_dict)
            last_result = engine.observe_closed_bar(bar, as_of=bar.ts_close)
            bars_processed += 1
    except n1r.N1ReplayError as exc:
        return {
            "ok": True, "rejected": True, "rejection_reason": f"{type(exc).__name__}: {exc}",
            "restored_from_snapshot": restored, "restore_rejected_reason": restore_rejected_reason,
            "bars_processed": bars_processed, "last_result": None, "snapshot_blob": None, "identity": None,
        }

    wall_clock_now = request.get("wall_clock_now")
    if wall_clock_now is not None:
        try:
            engine.assert_not_stale(now=wall_clock_now)
        except n1r.StaleStateError as exc:
            return {
                "ok": True, "rejected": True, "rejection_reason": f"StaleStateError: {exc}",
                "restored_from_snapshot": restored, "restore_rejected_reason": restore_rejected_reason,
                "bars_processed": bars_processed,
                "last_result": None if last_result is None else _result_to_dict(last_result),
                "snapshot_blob": None, "identity": _identity_to_dict(engine.identity),
            }

    snapshot = engine.snapshot()
    return {
        "ok": True, "rejected": False, "rejection_reason": restore_rejected_reason,
        "restored_from_snapshot": restored, "restore_rejected_reason": restore_rejected_reason,
        "bars_processed": bars_processed,
        "last_result": None if last_result is None else _result_to_dict(last_result),
        "snapshot_blob": _snapshot_to_blob(snapshot), "identity": _identity_to_dict(engine.identity),
        "artifact": {
            "ve_n1_replay_version": n1r.VE_N1_REPLAY_VERSION, "ai_source_commit": n1r.AI_SOURCE_COMMIT,
            "detector_submodule_commit": n1r.DETECTOR_SUBMODULE_COMMIT,
        },
    }


def main() -> None:
    raw = sys.stdin.read()
    try:
        request = json.loads(raw)
        response = _handle(request)
        # `allow_nan=False`: a NaN/Infinity anywhere in the response (a malformed bar's `high`/`low`, a
        # detector edge case) must fail LOUDLY here as a `ValueError` -- caught below, turned into an
        # honest `ok=False` error the client fails closed on -- never silently round-trip as the
        # non-standard `NaN`/`Infinity` JSON literals `json.dumps`'s own default would emit, which the
        # client's `json.loads` would just as silently accept back into a stop_price computation.
        payload = json.dumps(response, allow_nan=False)
    except Exception as exc:  # noqa: BLE001 -- must always emit valid JSON, never a bare traceback on stdout
        payload = json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"})
    sys.stdout.write(payload)


if __name__ == "__main__":
    main()
