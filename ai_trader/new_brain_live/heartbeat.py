"""LIVE_SHADOW heartbeat (RT-N1-PERSIST-0001 section 3). A single, OVERWRITTEN current-status
snapshot -- not an append-only history (that already exists, separately: `NewBrainTelemetryLog` and
`LiveShadowJournal` are the durable per-event record; a heartbeat written every ~30s for a service
meant to run non-stop indefinitely would grow those unboundedly for no operational benefit). Uses
`SqliteStateStore.set_text`/`get_text` (new, additive method -- RT-N1-PERSIST-0001 was the first
caller needing overwrite-latest TEXT rather than `REAL` storage).

Every field the CEO's own list names is present. `HeartbeatMonitor` is the watchdog's own read path --
deliberately separate from `HeartbeatWriter` (the live loop's write path) so a read-only watchdog
process never needs write access or any live-process state to evaluate freshness."""

from __future__ import annotations

import dataclasses
import json
import time

from ai_trader.persistent_state.store import SqliteStateStore

_HEARTBEAT_KEY = "new_brain_live.heartbeat"


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class LiveShadowHeartbeat:
    timestamp_utc: int
    pid: int
    process_start_identity: str
    runtime_commit: str
    authority: str
    broker_gate_state: str
    tower_worker_session_id: str | None
    last_closed_bar_id: str | None
    last_market_event_id: str | None
    last_journal_sequence: int
    last_outcome_reason: str | None
    mt5_connected: bool
    balance: float | None
    equity: float | None
    open_orders: int | None
    open_positions: int | None
    runtime_mode: str = "LEGACY_M15"
    """RT-N1-INCREMENTAL-WIRING-0001 (2026-08-18): `"LEGACY_M15"` or `"N1_INCREMENTAL_DUAL_CLOCK"` --
    additive, defaulted so `from_json` on a heartbeat any OLD (pre-this-change) process already wrote
    deserializes unchanged, and so `NewBrainLiveLoop`'s own existing `_build_heartbeat()` needs only ONE
    new line (`runtime_mode="LEGACY_M15"`), never a restructure."""
    n1_version_pin: str | None = None
    """`ve_n1_replay` version + wheel SHA-256 short form, e.g. `"0.1.1@2cff7e7b"` -- `None` under
    `LEGACY_M15` (no N1 replay artifact involved), populated under `N1_INCREMENTAL_DUAL_CLOCK`."""
    snapshot_identity: str | None = None
    last_m15_bar_id: str | None = None
    last_m5_bar_id: str | None = None
    context_timestamp: int | None = None
    tower_state: str | None = None
    order_send_calls: int = 0

    def to_json(self) -> str:
        return json.dumps(dataclasses.asdict(self))

    @staticmethod
    def from_json(text: str) -> "LiveShadowHeartbeat":
        data = json.loads(text)
        return LiveShadowHeartbeat(**data)

    @property
    def age_seconds(self) -> float:
        return time.time() - self.timestamp_utc


class HeartbeatWriter:
    """The live loop's own write path -- one `record()` call per tick, always overwriting."""

    def __init__(self, state_store: SqliteStateStore, key: str = _HEARTBEAT_KEY) -> None:
        self._state_store = state_store
        self._key = key

    def record(self, heartbeat: LiveShadowHeartbeat) -> None:
        self._state_store.set_text(self._key, heartbeat.to_json())


class HeartbeatMonitor:
    """The watchdog's own read-only path -- a fresh `SqliteStateStore` connection, never the live
    loop's own connection object (the watchdog is always a SEPARATE process)."""

    def __init__(self, state_store: SqliteStateStore, key: str = _HEARTBEAT_KEY) -> None:
        self._state_store = state_store
        self._key = key

    def latest(self) -> LiveShadowHeartbeat | None:
        raw = self._state_store.get_text(self._key)
        return None if raw is None else LiveShadowHeartbeat.from_json(raw)

    def is_stale(self, *, max_age_seconds: float, now: float | None = None) -> bool:
        """`True` if there is NO heartbeat at all (never started, or state store wiped), or the most
        recent one is older than `max_age_seconds`. Fail-closed direction: absence of evidence is
        treated as staleness, never as "must be fine."""
        heartbeat = self.latest()
        if heartbeat is None:
            return True
        current = now if now is not None else time.time()
        return (current - heartbeat.timestamp_utc) > max_age_seconds
