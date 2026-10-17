"""Seeded load generator for the course platform, and a Debezium emulator for offline runs.

The generator keeps the source database in memory and produces operations (insert, update,
delete, DDL). The same operations can be

* turned into Debezium Postgres records (`DebeziumEmitter`), for the offline demo and tests, or
* executed as SQL against a real Postgres (`live.py`), for the Docker stack.

Row values are stored the way Debezium shows them in JSON (timestamps as ISO strings, numeric as
strings while a column is `numeric`), so the emulator only has to wrap rows in envelopes.

Planted on purpose, so the demo has real effects to show and check:

* a nullable column `courses.level` is added part-way through (schema evolution);
* `payments.amount_cents` is changed to `numeric` for a short window and back (a type change:
  those events go to quarantine, and the lake rows they touched stay behind the source);
* a few users are deleted with their enrolments and payments in one transaction (deletes,
  tombstones);
* `deliver` interleaves topics, can shuffle records beyond Kafka's ordering guarantee and can
  re-deliver a block after a simulated consumer crash (out-of-order and duplicate events).
"""

from __future__ import annotations

import copy
import random
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from .sources import TABLE_ORDER

START = datetime(2026, 9, 1, 8, 0, tzinfo=UTC)
TOPIC_PREFIX = "courseplatform"
CATEGORIES = ("data", "language", "programming", "teaching", "design")
COUNTRIES = ("FI", "FI", "FI", "SE", "VN", "EE", "DE", "NO")
FIRST = ("Aino", "Minh", "Sara", "Eero", "Linh", "Mikko", "Anna", "Hai", "Elsa", "Juho")
LAST = ("Virtanen", "Nguyen", "Lind", "Korhonen", "Tran", "Niemi", "Berg", "Makinen")
LEVELS = ("beginner", "intermediate", "advanced")

ADD_COLUMN_SQL = "ALTER TABLE courses ADD COLUMN level text"
TO_NUMERIC_SQL = "ALTER TABLE payments ALTER COLUMN amount_cents TYPE numeric(12,2)"
TO_BIGINT_SQL = (
    "ALTER TABLE payments ALTER COLUMN amount_cents TYPE bigint USING amount_cents::bigint"
)


def iso(ts: datetime) -> str:
    """Debezium ZonedTimestamp style: ISO-8601 in UTC with a `Z` suffix."""
    return ts.astimezone(UTC).isoformat().replace("+00:00", "Z")


def ms(ts: datetime) -> int:
    return int(ts.timestamp() * 1000)


@dataclass(frozen=True)
class Op:
    """One source operation. `tx` groups operations committed together."""

    kind: str  # insert | update | delete | ddl
    table: str
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    at: datetime
    tx: int
    sql: str = ""


@dataclass
class GeneratorConfig:
    seed: int = 7
    users: int = 300
    courses: int = 24
    steps: int = 3000
    add_column_at: float = 0.35
    type_change_at: float = 0.6
    type_change_steps: int = 30
    checkpoint_at: float = 0.5


@dataclass
class SourceDatabase:
    """The generator's copy of the source tables: the truth to reconcile the lake against."""

    tables: dict[str, dict[int, dict[str, Any]]] = field(
        default_factory=lambda: {t: {} for t in TABLE_ORDER}
    )

    def snapshot(self) -> dict[str, dict[int, dict[str, Any]]]:
        return copy.deepcopy(self.tables)


class Generator:
    """Seeded course-platform workload: an initial database, then a stream of operations."""

    def __init__(self, config: GeneratorConfig | None = None) -> None:
        self.config = config or GeneratorConfig()
        self.rng = random.Random(self.config.seed)
        self.db = SourceDatabase()
        self.clock = START
        self.tx = 1000
        self.next_id = {"users": 1, "courses": 100, "enrolments": 1, "payments": 1}
        self.numeric_amounts = False
        self.has_level = False
        self.checkpoint_ms: int | None = None
        self.checkpoint: dict[int, dict[str, Any]] = {}

    # ---- helpers ------------------------------------------------------------------------

    def _id(self, table: str) -> int:
        value = self.next_id[table]
        self.next_id[table] += 1
        return value

    def _amount(self, cents: int) -> int | str:
        return f"{cents}.00" if self.numeric_amounts else cents

    def _user(self, at: datetime) -> dict[str, Any]:
        uid = self._id("users")
        first, last = self.rng.choice(FIRST), self.rng.choice(LAST)
        return {
            "id": uid,
            "email": f"{first.lower()}.{last.lower()}{uid}@example.com",
            "full_name": f"{first} {last}",
            "country": self.rng.choice(COUNTRIES),
            "created_at": iso(at),
            "updated_at": iso(at),
        }

    def _course(self, at: datetime) -> dict[str, Any]:
        cid = self._id("courses")
        category = self.rng.choice(CATEGORIES)
        row: dict[str, Any] = {
            "id": cid,
            "title": f"{category.title()} course {cid}",
            "category": category,
            "price_cents": self.rng.choice((1900, 2900, 3900, 4900, 7900)),
            "published": self.rng.random() < 0.85,
            "created_at": iso(at),
            "updated_at": iso(at),
        }
        if self.has_level:
            row["level"] = self.rng.choice(LEVELS)
        return row

    def _enrolment(self, user_id: int, course_id: int, at: datetime) -> dict[str, Any]:
        return {
            "id": self._id("enrolments"),
            "user_id": user_id,
            "course_id": course_id,
            "status": "active",
            "progress_pct": 0,
            "enrolled_at": iso(at),
            "updated_at": iso(at),
        }

    def _payment(self, enrolment: dict[str, Any], status: str, at: datetime) -> dict[str, Any]:
        price = self.db.tables["courses"][enrolment["course_id"]]["price_cents"]
        return {
            "id": self._id("payments"),
            "enrolment_id": enrolment["id"],
            "amount_cents": self._amount(price),
            "currency": "EUR",
            "status": status,
            "created_at": iso(at),
            "updated_at": iso(at),
        }

    # ---- initial database ---------------------------------------------------------------

    def initial(self) -> SourceDatabase:
        """The database before CDC is switched on (what Debezium's snapshot will read)."""
        t = self.db.tables
        for i in range(self.config.courses):
            row = self._course(START - timedelta(days=60 - i))
            t["courses"][row["id"]] = row
        course_ids = sorted(t["courses"])
        for i in range(self.config.users):
            user = self._user(START - timedelta(days=30, minutes=-7 * i))
            t["users"][user["id"]] = user
            for course_id in self.rng.sample(
                course_ids, k=self.rng.randint(1, min(4, len(course_ids)))
            ):
                at = START - timedelta(days=self.rng.randint(1, 25), seconds=i)
                enrolment = self._enrolment(user["id"], course_id, at)
                enrolment["progress_pct"] = self.rng.randint(0, 90)
                t["enrolments"][enrolment["id"]] = enrolment
                if self.rng.random() < 0.7:
                    payment = self._payment(enrolment, "paid", at)
                    t["payments"][payment["id"]] = payment
        return self.db

    # ---- change stream ------------------------------------------------------------------

    def _write(self, kind: str, table: str, row: dict[str, Any], at: datetime) -> Op:
        rows = self.db.tables[table]
        before = copy.deepcopy(rows.get(row["id"]))
        if kind == "delete":
            del rows[row["id"]]
            return Op(kind, table, before, None, at, self.tx)
        rows[row["id"]] = row
        return Op(kind, table, before, copy.deepcopy(row), at, self.tx)

    def _update(self, table: str, pk: int, at: datetime, **changes: Any) -> Op:
        row = {**self.db.tables[table][pk], **changes, "updated_at": iso(at)}
        return self._write("update", table, row, at)

    def _ddl(self, sql: str, at: datetime) -> Op:
        return Op("ddl", "", None, None, at, self.tx, sql=sql)

    def changes(self) -> Iterator[Op]:
        """Yield operations in commit order. Call `initial()` first."""
        cfg = self.config
        add_at = int(cfg.steps * cfg.add_column_at)
        numeric_from = int(cfg.steps * cfg.type_change_at)
        numeric_to = numeric_from + cfg.type_change_steps
        checkpoint_step = int(cfg.steps * cfg.checkpoint_at)
        for step in range(cfg.steps):
            self.clock += timedelta(seconds=self.rng.randint(1, 90))
            self.tx += 1
            at = self.clock
            if step == add_at:
                self.has_level = True
                for row in self.db.tables["courses"].values():
                    row["level"] = None
                yield self._ddl(ADD_COLUMN_SQL, at)
            if step == numeric_from:
                self.numeric_amounts = True
                for row in self.db.tables["payments"].values():
                    row["amount_cents"] = f"{row['amount_cents']}.00"
                yield self._ddl(TO_NUMERIC_SQL, at)
            if step == numeric_to:
                # The rewrite back to bigint changes every row but emits no change events.
                self.numeric_amounts = False
                for row in self.db.tables["payments"].values():
                    row["amount_cents"] = int(str(row["amount_cents"]).split(".")[0])
                yield self._ddl(TO_BIGINT_SQL, at)
            yield from self._step(at)
            if step == checkpoint_step:
                self.checkpoint_ms = ms(at)
                self.checkpoint = copy.deepcopy(self.db.tables["enrolments"])

    def _step(self, at: datetime) -> Iterator[Op]:
        t = self.db.tables
        r = self.rng.random()
        active = [e for e in t["enrolments"].values() if e["status"] == "active"]
        if r < 0.10 or not t["users"]:
            yield self._write("insert", "users", self._user(at), at)
        elif r < 0.16:
            uid = self.rng.choice(sorted(t["users"]))
            email = f"user{uid}.{self.rng.randint(1, 999)}@example.com"
            yield self._update("users", uid, at, email=email)
        elif r < 0.30:
            uid = self.rng.choice(sorted(t["users"]))
            cid = self.rng.choice(sorted(t["courses"]))
            enrolment = self._enrolment(uid, cid, at)
            yield self._write("insert", "enrolments", enrolment, at)
            yield self._write("insert", "payments", self._payment(enrolment, "pending", at), at)
        elif r < 0.65 and active:
            e = self.rng.choice(active)
            progress = min(100, e["progress_pct"] + self.rng.randint(5, 30))
            status = "completed" if progress == 100 else "active"
            yield self._update("enrolments", e["id"], at, progress_pct=progress, status=status)
        elif r < 0.75:
            pending = sorted(p for p, row in t["payments"].items() if row["status"] == "pending")
            if pending:
                pid = self.rng.choice(pending)
                status = "paid" if self.rng.random() < 0.85 else "failed"
                yield self._update("payments", pid, at, status=status)
        elif r < 0.81 and active:
            e = self.rng.choice(active)
            yield self._update("enrolments", e["id"], at, status="cancelled")
            for pid, row in sorted(t["payments"].items()):
                if row["enrolment_id"] == e["id"] and row["status"] == "paid":
                    yield self._update("payments", pid, at, status="refunded")
        elif r < 0.85:
            cid = self.rng.choice(sorted(t["courses"]))
            price = self.rng.choice((1900, 2900, 3900, 4900, 7900))
            extra = {"level": self.rng.choice(LEVELS)} if self.has_level else {}
            yield self._update("courses", cid, at, price_cents=price, **extra)
        elif r < 0.89:
            gone = sorted(
                p for p, row in t["payments"].items() if row["status"] in ("failed", "pending")
            )
            if gone:
                pid = self.rng.choice(gone)
                yield self._write("delete", "payments", t["payments"][pid], at)
        elif r < 0.91:
            yield from self._delete_user(self.rng.choice(sorted(t["users"])), at)
        elif r < 0.94:
            yield self._write("insert", "courses", self._course(at), at)
        else:
            cid = self.rng.choice(sorted(t["courses"]))
            published = not t["courses"][cid]["published"]
            yield self._update("courses", cid, at, published=published)

    def _delete_user(self, uid: int, at: datetime) -> Iterator[Op]:
        """GDPR-style delete: children first, all in one transaction."""
        t = self.db.tables
        enrolments = sorted(e for e, row in t["enrolments"].items() if row["user_id"] == uid)
        for pid in sorted(
            p for p, row in t["payments"].items() if row["enrolment_id"] in enrolments
        ):
            yield self._write("delete", "payments", t["payments"][pid], at)
        for eid in enrolments:
            yield self._write("delete", "enrolments", t["enrolments"][eid], at)
        yield self._write("delete", "users", t["users"][uid], at)


class DebeziumEmitter:
    """Wraps rows in Debezium 2.x Postgres envelopes (JSON converter, schemas disabled).

    LSNs grow with every change, like WAL positions; snapshot rows all share the LSN at which
    the snapshot started. Each table is one topic with one partition (Debezium's default), and
    every delete is followed by a tombstone.
    """

    def __init__(self, seed: int = 7, start_lsn: int = 24_023_928) -> None:
        self.rng = random.Random(seed + 1)
        self.lsn = start_lsn
        self.offsets: dict[str, int] = {}

    def _record(self, table: str, key: dict[str, Any], value: Any) -> dict[str, Any]:
        topic = f"{TOPIC_PREFIX}.public.{table}"
        offset = self.offsets.get(topic, 0)
        self.offsets[topic] = offset + 1
        return {"topic": topic, "partition": 0, "offset": offset, "key": key, "value": value}

    def _envelope(
        self,
        table: str,
        op: str,
        before: dict[str, Any] | None,
        after: dict[str, Any] | None,
        at: datetime,
        tx: int,
        snapshot: str = "false",
        last_commit_lsn: int | None = None,
    ) -> dict[str, Any]:
        ts = ms(at)
        source = {
            "version": "2.7.3.Final",
            "connector": "postgresql",
            "name": TOPIC_PREFIX,
            "ts_ms": ts,
            "snapshot": snapshot,
            "db": "courses",
            "sequence": f'["{last_commit_lsn or "null"}","{self.lsn}"]'.replace('"null"', "null"),
            "ts_us": ts * 1000,
            "ts_ns": ts * 1_000_000,
            "schema": "public",
            "table": table,
            "txId": tx,
            "lsn": self.lsn,
            "xmin": None,
        }
        emitted = ts + self.rng.randint(20, 400)
        return {
            "before": before,
            "after": after,
            "source": source,
            "transaction": None,
            "op": op,
            "ts_ms": emitted,
            "ts_us": emitted * 1000,
            "ts_ns": emitted * 1_000_000,
        }

    def snapshot(self, db: SourceDatabase, at: datetime = START) -> list[dict[str, Any]]:
        """`op = r` records for every row, parents first, sharing one LSN."""
        rows = [(t, row) for t in TABLE_ORDER for _, row in sorted(db.tables[t].items())]
        out = []
        for i, (table, row) in enumerate(rows):
            flag = "first" if i == 0 else "last" if i == len(rows) - 1 else "true"
            value = self._envelope(table, "r", None, copy.deepcopy(row), at, 999, snapshot=flag)
            out.append(self._record(table, {"id": row["id"]}, value))
        return out

    def stream(self, ops: Iterator[Op]) -> list[dict[str, Any]]:
        """Records for a sequence of operations; DDL emits nothing (pgoutput has no DDL)."""
        out: list[dict[str, Any]] = []
        last_commit = self.lsn
        current_tx: int | None = None
        for op in ops:
            if op.kind == "ddl":
                continue
            if op.tx != current_tx:
                last_commit, current_tx = self.lsn, op.tx
            self.lsn += self.rng.randint(56, 600)
            code = {"insert": "c", "update": "u", "delete": "d"}[op.kind]
            row = op.after if op.after is not None else op.before or {}
            key = {"id": row["id"]}
            value = self._envelope(
                op.table, code, op.before, op.after, op.at, op.tx, last_commit_lsn=last_commit
            )
            out.append(self._record(op.table, key, value))
            if op.kind == "delete":
                out.append(self._record(op.table, key, None))
        return out


def deliver(
    records: list[dict[str, Any]],
    seed: int = 7,
    shuffle_window: int = 0,
    redeliver_at: float | None = None,
    redeliver_count: int = 0,
) -> list[dict[str, Any]]:
    """Arrival order as a consumer would see it.

    Topics are interleaved at random (Kafka orders records only within a partition). Then,
    optionally, records are shuffled inside windows of `shuffle_window` (stronger disorder than
    Kafka allows, to prove the merge does not depend on order), and a block of
    `redeliver_count` records is delivered twice, as after a crash before offsets were committed.
    """
    rng = random.Random(seed + 2)
    queues: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        queues.setdefault(record["topic"], []).append(record)
    positions = {topic: 0 for topic in queues}
    arrival: list[dict[str, Any]] = []
    while positions:
        topic = rng.choice(sorted(positions))
        arrival.append(queues[topic][positions[topic]])
        positions[topic] += 1
        if positions[topic] == len(queues[topic]):
            del positions[topic]
    if shuffle_window > 1:
        for start in range(0, len(arrival), shuffle_window):
            window = arrival[start : start + shuffle_window]
            rng.shuffle(window)
            arrival[start : start + shuffle_window] = window
    if redeliver_at is not None and redeliver_count > 0:
        cut = int(len(arrival) * redeliver_at)
        arrival = arrival[:cut] + arrival[max(0, cut - redeliver_count) : cut] + arrival[cut:]
    return arrival
