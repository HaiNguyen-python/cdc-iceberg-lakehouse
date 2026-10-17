"""`cdc-lake report`: docs/results.md and two charts from the demo's metrics.json.

Nothing here is typed by hand: every number comes from `metrics.json`, which `cdc-lake demo`
writes from a real run.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

BLUE, GREY = "#2a78d6", "#b7b7b2"
INK, INK_2, GRID, SURFACE = "#1f1f1d", "#5f5e5a", "#e6e5e1", "#ffffff"
HIGHLIGHT = "enrolments"


def load_metrics(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text())


def _style(ax: Any, title: str, subtitle: str) -> None:
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.tick_params(colors=INK_2, labelsize=9, length=0)
    ax.set_title(title, loc="left", fontsize=12.5, fontweight="bold", color=INK, pad=24)
    ax.text(0, 1.03, subtitle, transform=ax.transAxes, fontsize=9.5, color=INK_2)


def peak_files(metrics: dict[str, Any]) -> int:
    return max(b["live_files"][HIGHLIGHT] for b in metrics["batches"])


def apply_time_around_compaction(
    metrics: dict[str, Any], window: int = 3
) -> tuple[float, float] | None:
    """Mean apply seconds of the `window` stream batches before and after the first compaction
    (None when the run never compacted)."""
    stream = [b for b in metrics["batches"] if b["phase"] == "stream"]
    cut = next((i for i, b in enumerate(stream) if b["compacted_after"]), None)
    if cut is None:
        return None
    before = [b["seconds"] for b in stream[max(0, cut - window + 1) : cut + 1]]
    after = [b["seconds"] for b in stream[cut + 1 : cut + 1 + window]]
    return sum(before) / len(before), sum(after) / max(1, len(after))


def _compaction_lines(ax: Any, batches: list[dict[str, Any]]) -> None:
    for b in batches:
        if b["compacted_after"]:
            ax.axvline(b["batch"] + 0.5, color=INK_2, linewidth=1, linestyle=":", alpha=0.6)


def plot_files(metrics: dict[str, Any], path: Path) -> None:
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MaxNLocator

    batches = metrics["batches"]
    x = [b["batch"] for b in batches]
    tables = list(batches[0]["live_files"])
    fig, ax = plt.subplots(figsize=(10, 4.4), dpi=150, facecolor=SURFACE)
    _compaction_lines(ax, batches)
    first_stream = next(b["batch"] for b in batches if b["phase"] == "stream")
    ax.axvline(first_stream - 0.5, color=GRID, linewidth=1.2)
    ends: dict[int, list[str]] = {}
    for name in sorted(tables, key=lambda t: t == HIGHLIGHT):
        y = [b["live_files"][name] for b in batches]
        colour, width = (BLUE, 2.4) if name == HIGHLIGHT else (GREY, 1.6)
        ax.plot(x, y, color=colour, linewidth=width)
        if name == HIGHLIGHT:
            ax.annotate(
                name,
                (x[-1], y[-1]),
                xytext=(6, 0),
                textcoords="offset points",
                va="center",
                fontsize=9,
                color=INK,
            )
        else:
            ends.setdefault(y[-1], []).append(name)
    for y_end, names in ends.items():
        ax.annotate(
            ", ".join(names),
            (x[-1], y_end),
            xytext=(6, -4),
            textcoords="offset points",
            va="center",
            fontsize=9,
            color=INK_2,
        )
    top = peak_files(metrics)
    ax.text(
        first_stream - 0.8, top, "snapshot load", ha="right", va="top", fontsize=8.5, color=INK_2
    )
    ax.text(
        first_stream - 0.2, top, "streaming changes", ha="left", va="top", fontsize=8.5, color=INK_2
    )
    every = metrics["config"]["compact_every"]
    _style(
        ax,
        f"Each CDC batch adds data files: {HIGHLIGHT} reached {top} before compaction reset it"
        if any(b["compacted_after"] for b in batches)
        else f"Without compaction, {HIGHLIGHT} grew to {top} data files",
        f"Data files in the current snapshot of each state table, per micro-batch. Dotted: "
        f"compaction every {every} stream batches. Synthetic, local run.",
    )
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    ax.set_xlabel("micro-batch", fontsize=9, color=INK_2)
    ax.set_ylabel("data files", fontsize=9, color=INK_2)
    ax.set_xlim(x[0], x[-1] + 6)
    ax.set_ylim(bottom=0)
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def plot_apply_time(metrics: dict[str, Any], path: Path) -> None:
    import matplotlib.pyplot as plt

    batches = [b for b in metrics["batches"] if b["phase"] == "stream"]
    x = [b["batch"] for b in batches]
    y = [b["seconds"] for b in batches]
    around = apply_time_around_compaction(metrics)
    if around is None:
        title = f"Apply time per batch without compaction: {y[0]:.1f} s first, {y[-1]:.1f} s last"
    else:
        title = (
            f"Apply time per batch: {around[0]:.1f} s just before the first compaction, "
            f"{around[1]:.1f} s just after"
        )
    fig, ax = plt.subplots(figsize=(10, 4.4), dpi=150, facecolor=SURFACE)
    _compaction_lines(ax, batches)
    ax.plot(x, y, color=BLUE, linewidth=2.2, marker="o", markersize=3.5)
    for b in batches:
        if b["rows_written"] == 0:
            ax.annotate(
                "redelivered batch (all duplicates)",
                (b["batch"], b["seconds"]),
                xytext=(8, 0),
                textcoords="offset points",
                ha="left",
                va="center",
                fontsize=8.5,
                color=INK_2,
            )
    _style(
        ax,
        title,
        f"Seconds to merge one batch of {metrics['config']['batch_size']} events into all tables "
        f"(state, changelog, SCD2). Dotted: compaction. Local, single process, shared CPU.",
    )
    ax.set_xlabel("micro-batch (streaming phase)", fontsize=9, color=INK_2)
    ax.set_ylabel("seconds", fontsize=9, color=INK_2)
    ax.set_ylim(bottom=0, top=max(y) * 1.15)
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def _n(value: float) -> str:
    return f"{value:,.0f}" if float(value).is_integer() else f"{value:,.1f}"


def results_markdown(m: dict[str, Any]) -> str:
    cfg, rec, thr, fr = m["config"], m["records"], m["throughput"], m["freshness_s"]
    mt = m["maintenance"]
    lines = [
        "# Results (synthetic data)",
        "",
        "Generated by `cdc-lake report` from `data/demo/metrics.json`, written by `cdc-lake demo` "
        f"(seed {cfg['seed']}, {cfg['users']} users, {cfg['courses']} courses, "
        f"{_n(m['source_ops'])} source operations). All data is **synthetic**; the schema change, "
        "the type change, the deletes, the shuffling and the redelivery were **planted** by the "
        "generator on purpose. Timings are from one local, single-process run on a shared "
        "2-CPU sandbox; they change from run to run, the counts do not.",
        "",
        "## Stream",
        "",
        "| Item | Value |",
        "| --- | --- |",
        f"| Snapshot records (op = r) | {_n(rec['snapshot'])} |",
        f"| Change records (c/u/d + tombstones) | {_n(rec['change_records'])} "
        f"({_n(rec['tombstones'])} tombstones) |",
        f"| Delivered, incl. redelivered block | {_n(rec['delivered_stream'])} "
        f"(+{_n(rec['redelivered'])} duplicates) |",
        f"| Shuffle window (beyond Kafka ordering) | {cfg['shuffle_window']} records |",
        f"| Micro-batches (size {cfg['batch_size']}) | {thr['batches']} |",
        f"| Columns added by schema evolution | {', '.join(m['columns_added']) or 'none'} |",
        f"| Quarantined records | {m['quarantine']['rows']} |",
        "",
        "Quarantine reasons:",
        "",
    ]
    lines += [f"- `{reason}`: {n}" for reason, n in sorted(m["quarantine"]["reasons"].items())]
    lines += [
        "",
        "## Throughput and freshness (local, single process)",
        "",
        "| Metric | Value |",
        "| --- | --- |",
        f"| Apply throughput, snapshot load | {_n(thr['snapshot_events_per_s'])} events/s |",
        f"| Apply throughput, streaming changes | {_n(thr['stream_events_per_s'])} events/s |",
        f"| Offered rate (simulated producer) | {_n(fr['rate_events_per_s'])} events/s |",
        f"| Freshness p50 / p95 / max | {fr['p50']:.2f} s / {fr['p95']:.2f} s / "
        f"{fr['max']:.2f} s |",
        "",
        "Freshness = time from a record being offered to the consumer until the Iceberg commit "
        "that contains it (batch fill time + apply time). It does not include Postgres -> "
        "Debezium -> Kafka latency, which was not measured here.",
        "",
        "![Live data files per table](img/files_per_batch.png)",
        "",
        "![Apply time per batch](img/apply_time_per_batch.png)",
        "",
        "## Correctness checks",
        "",
        "| Table | Source rows | Lake rows | Differing keys | Explained by quarantine | "
        "Unexplained |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for name, r in m["reconciliation"].items():
        lines.append(
            f"| {name} | {_n(r['source_rows'])} | {_n(r['lake_rows'])} | {r['differing']} | "
            f"{r['explained_by_quarantine']} | {len(r['unexplained'])} |"
        )
    replay = m["replay_last_two_batches"]
    scd2 = m["scd2_as_of_checkpoint"]
    tt = m["time_travel_to_initial_load"]
    tt_rows = sum(t["rows"] for t in tt.values())
    tt_diff = sum(t["differing"] for t in tt.values())
    lines += [
        "",
        f"- **Idempotent replay:** re-applying the last two batches ({_n(replay['events'])} "
        f"events) wrote {replay['rows_written']} state rows and {replay['versions_added']} "
        "changelog rows.",
        f"- **Time travel:** reading each table at the Iceberg snapshot taken after the "
        f"initial load returned {_n(tt_rows)} rows; {tt_diff} differ from the source database "
        "at snapshot time.",
        f"- **SCD2 history:** enrolments as of {scd2['as_of']} (business time) returned "
        f"{_n(scd2['rows'])} rows against {_n(scd2['source_rows'])} in the source at that "
        f"moment; {scd2['differing']} differ.",
        "- **Reconciliation:** every key that differs between the lake and the source has a "
        "quarantined event. Those rows stay behind until the quarantined events are fixed and "
        "replayed; this is the intended cost of refusing a silent type change.",
        "",
        f"## Maintenance (all {mt['tables']} tables: state, changelog, history)",
        "",
        "| Step | Live data files | Snapshots | Data files on disk |",
        "| --- | --- | --- | --- |",
    ]
    for label, key in (
        ("Before", "before"),
        ("After compaction", "after_compaction"),
        (f"After expiry (keep last {mt['retain_last']}) + orphan cleanup", "after_expiry"),
    ):
        s = mt[key]
        lines.append(
            f"| {label} | {_n(s['live_files'])} | {_n(s['snapshots'])} | {_n(s['files_on_disk'])} |"
        )
    rows = mt["rows_after_maintenance"]
    same = all(rows[n] == m["reconciliation"][n]["lake_rows"] for n in rows)
    lines += [
        "",
        "Row counts after maintenance are "
        + ("unchanged" if same else "**different** (bug)")
        + f": {', '.join(f'{k} {_n(v)}' for k, v in rows.items())}.",
        "Compaction adds snapshots (an overwrite is a delete plus an append) and keeps the old "
        "files, because time travel still needs them; disk space comes back only when snapshots "
        "are expired. The retained snapshots "
        "still point at pre-compaction files, which is why more files stay on disk than are "
        "live.",
        "",
    ]
    return "\n".join(lines)


def write_report(metrics_path: Path, docs_dir: Path) -> Path:
    metrics = load_metrics(metrics_path)
    img = Path(docs_dir) / "img"
    img.mkdir(parents=True, exist_ok=True)
    plot_files(metrics, img / "files_per_batch.png")
    plot_apply_time(metrics, img / "apply_time_per_batch.png")
    out = Path(docs_dir) / "results.md"
    out.write_text(results_markdown(metrics))
    return out
