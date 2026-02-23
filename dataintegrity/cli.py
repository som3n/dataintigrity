"""
cli.py
------
Command-line interface for the dataintegrity SDK.

Entry point: ``dataintegrity``

Commands
--------
* ``audit``  — load a CSV, run the full integrity pipeline, and print results.

v0.2 flags (``audit`` command)
-------------------------------
* ``--track``          — save version and compare to previous if one exists.
* ``--history``        — show version history for this data source.

v0.2.1 flags (``audit`` command)
---------------------------------
* ``--output``         — output format: ``pretty`` (default) or ``json``.
* ``--save-manifest``  — write the ExecutionManifest to ``<filepath>.manifest.json``.
* ``--save-history``   — record audit result in the local history store.
* ``--json-output``    — legacy alias for ``--output json`` (kept for compat).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

import click

from dataintegrity.connectors.csv import CSVConnector
from dataintegrity.core.config import IntegrityConfig
from dataintegrity.core.dataset import Dataset
from dataintegrity.core.result_schema import DatasetAuditResult
from dataintegrity.ingestion.normalizer import Normalizer
from dataintegrity.ingestion.pii import PIIDetector
from dataintegrity.integrity.engine import IntegrityEngine


# ---------------------------------------------------------------------------
# Helpers — rendering
# ---------------------------------------------------------------------------

def _score_bar(score: float, width: int = 30) -> str:
    """Render a simple ASCII progress bar for a 0–100 score."""
    filled = int(round((score / 100) * width))
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {score:.1f}/100"


def _dim_label(score: float) -> str:
    """Return a coloured grade label based on the 0–1 dimension score."""
    pct = score * 100
    if pct >= 90:
        colour = "green"
        label = "EXCELLENT"
    elif pct >= 75:
        colour = "cyan"
        label = "GOOD"
    elif pct >= 50:
        colour = "yellow"
        label = "FAIR"
    else:
        colour = "red"
        label = "POOR"
    return click.style(f"{label} ({pct:.1f}%)", fg=colour)


def _severity_style(severity: str) -> str:
    """Return a styled severity label."""
    colours = {
        "stable":   "green",
        "minor":    "cyan",
        "moderate": "yellow",
        "critical": "red",
    }
    colour = colours.get(severity, "white")
    return click.style(severity.upper(), fg=colour, bold=True)


def _delta_style(delta: float) -> str:
    """Return a colour-coded delta string."""
    sign = "+" if delta >= 0 else ""
    if delta >= 0:
        colour = "green"
    elif delta > -2:
        colour = "cyan"
    elif delta > -5:
        colour = "yellow"
    else:
        colour = "red"
    return click.style(f"{sign}{delta:.2f}", fg=colour, bold=True)


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------

@click.group()
@click.version_option(version="0.2.1", prog_name="dataintegrity")
def cli():
    """dataintegrity — Data Infrastructure SDK audit toolkit (v0.2.1)."""


# ---------------------------------------------------------------------------
# audit command
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("filepath", type=click.Path(exists=True, dir_okay=False), required=False)
@click.option(
    "--dsn", type=str,
    help="PostgreSQL connection DSN (e.g. postgresql://user:pass@host:port/db).",
)
@click.option(
    "--table", type=str,
    help="Database table name to audit (requires --dsn).",
)
@click.option(
    "--query", type=str,
    help="Custom SQL query to audit (requires --dsn).",
)
@click.option(
    "--encoding", default="utf-8-sig", show_default=True,
    help="CSV file encoding.",
)
@click.option(
    "--delimiter", default=",", show_default=True,
    help="CSV column delimiter.",
)
@click.option(
    "--no-normalize", is_flag=True, default=False,
    help="Skip column-name normalisation.",
)
@click.option(
    "--output", "output_format",
    type=click.Choice(["pretty", "json"], case_sensitive=False),
    default="pretty", show_default=True,
    help="Output format: pretty (default) or json.",
)
@click.option(
    "--json-output", is_flag=True, default=False,
    help="[Legacy] Alias for --output json.",
)
@click.option(
    "--pii-threshold", default=0.05, show_default=True, type=float,
    help="Drift p-value threshold.",
)
@click.option(
    "--sample-size", default=None, type=int,
    help="Maximum rows to read (useful for large CSVs).",
)
@click.option(
    "--track", is_flag=True, default=False,
    help="Save this audit as a version and compare against the previous one.",
)
@click.option(
    "--history", is_flag=True, default=False,
    help="Show version history for this data source.",
)
@click.option(
    "--save-manifest", is_flag=True, default=False,
    help="Write the ExecutionManifest to <filepath>.manifest.json.",
)
@click.option(
    "--save-history", is_flag=True, default=False,
    help="Record this audit result in the local history store (~/.dataintegrity/history).",
)
def audit(
    filepath: Optional[str],
    dsn: Optional[str],
    table: Optional[str],
    query: Optional[str],
    encoding: str,
    delimiter: str,
    no_normalize: bool,
    output_format: str,
    json_output: bool,
    pii_threshold: float,
    sample_size: Optional[int],
    track: bool,
    history: bool,
    save_manifest: bool,
    save_history: bool,
):
    """
    Run a full data integrity audit on a CSV file or PostgreSQL database.

    \b
    FILEPATH  Path to the CSV file to audit (not required if --dsn is used).

    Outputs:
      - DataScore (0–100 composite quality score)
      - Per-dimension metric breakdown
      - Dataset fingerprint (SHA-256)
      - PII report
      - (--track) Score delta vs. previous version
      - (--history) Full version history
      - (--save-manifest) Write manifest to file
      - (--save-history) Record to local history store
    """
    # --json-output is a legacy flag; normalise to output_format
    if json_output:
        output_format = "json"

    # Validation: Ensure either filepath or dsn is provided
    if not filepath and not dsn:
        raise click.UsageError("Either FILEPATH or --dsn must be provided.")
    if filepath and dsn:
        raise click.UsageError("Cannot provide both FILEPATH and --dsn. Choose one.")

    config = IntegrityConfig(drift_p_threshold=pii_threshold)

    # ---- Load ----
    if dsn:
        # PostgreSQL Path
        from dataintegrity.connectors.postgres import PostgresConnector
        if not query and not table:
            raise click.UsageError("Audit via --dsn requires either --table or --query.")
        
        sql_query = query if query else f"SELECT * FROM {table}"
        source_id = table if table else f"query:{hash(sql_query)}"
        
        click.echo(f"\n🔗  Connecting to Database …")
        # Simplified DSN parsing for the connector (it expects host, port, etc separately)
        # However, PostgresConnector can be improved to handle DSN directly or we parse here.
        # For now, let's assume SQLAlchemy create_engine in the connector handles the URL.
        # We need to refine PostgresConnector to accept a single URL/DSN or parse it.
        
        # Refined Logic: We'll parse the DSN here or pass it directly.
        # Let's check how PostgresConnector is built. It expects host, port, db, user, pass.
        # I will update PostgresConnector later to handle DSN or parse it here.
        
        from sqlalchemy.engine import make_url
        try:
            url = make_url(dsn)
            connector = PostgresConnector(
                host=url.host or "localhost",
                port=url.port or 5432,
                database=url.database or "",
                user=url.username or "",
                password=url.password or "",
                query=sql_query
            )
            connector.connect()
            df = connector.fetch()
        except Exception as exc:
            click.echo(click.style(f"\n✗  Database error: {exc}", fg="red"), err=True)
            sys.exit(1)
        
        source_label = f"DB:{source_id}"
    else:
        # CSV Path
        click.echo(f"\n🔍  Loading  {click.style(filepath, fg='cyan', bold=True)}") # type: ignore
        connector = CSVConnector(
            filepath, # type: ignore
            encoding=encoding,
            delimiter=delimiter,
            sample_size=sample_size,
        )
        try:
            connector.connect()
            df = connector.fetch()
        except Exception as exc:
            click.echo(click.style(f"\n✗  Could not load file: {exc}", fg="red"), err=True)
            sys.exit(1)
        
        source_label = filepath # type: ignore

    dataset = Dataset(df, source=source_label)
    click.echo(f"   Loaded {dataset.shape[0]:,} rows × {dataset.shape[1]} columns.")

    # ---- Normalize ----
    if not no_normalize:
        normalizer = Normalizer()
        dataset = normalizer.normalize(dataset)
        click.echo("   Column names normalised.")

    # ---- Integrity Engine ----
    click.echo("\n⚙️   Running integrity engine …")
    engine = IntegrityEngine(config=config)
    audit_result: DatasetAuditResult = engine.run(dataset)

    # ---- PII Scan ----
    click.echo("🔒  Scanning for PII …")
    pii_detector = PIIDetector(config=config)
    pii_report = pii_detector.scan(dataset)

    # Attach PII summary to result for JSON output
    audit_result.pii_summary = pii_report

    # ---- Save Manifest ----
    if save_manifest:
        # For DB runs, we save to a specific metadata folder or source_id.manifest.json
        manifest_filename = f"{source_id}.manifest.json" if dsn else Path(filepath).with_suffix(".manifest.json") # type: ignore
        manifest_path = Path(manifest_filename)
        manifest_path.write_text(audit_result.manifest.to_json(), encoding="utf-8")
        click.echo(
            f"📄  Manifest saved → {click.style(str(manifest_path), fg='cyan')}"
        )

    # ---- Save History ----
    if save_history:
        try:
            from dataintegrity.integrity.history import IntegrityHistoryTracker
            tracker = IntegrityHistoryTracker()
            history_path = tracker.record(audit_result)
            click.echo(
                f"📈  History recorded → {click.style(str(history_path), fg='cyan')}"
            )
        except Exception as exc:  # pragma: no cover
            click.echo(
                click.style(f"⚠  Could not save history: {exc}", fg="yellow"),
                err=True,
            )

    # ---- Output ----
    if output_format == "json":
        full_output = audit_result.to_dict()
        click.echo(json.dumps(full_output, indent=2, default=str))
        return

    # Human-readable audit report — uses legacy dict for renderer compat
    legacy = audit_result.to_legacy_dict()
    _print_audit_report(source_label, legacy, pii_report, dataset) # type: ignore

    # ---- Versioning / History ----
    if track or history:
        _handle_versioning(
            filepath=source_label, # type: ignore
            dataset=dataset,
            config=config,
            do_track=track,
            show_history=history,
        )


# ---------------------------------------------------------------------------
# Versioning helpers
# ---------------------------------------------------------------------------

def _handle_versioning(
    filepath: str,
    dataset: Dataset,
    config: IntegrityConfig,
    do_track: bool,
    show_history: bool,
) -> None:
    """Run versioning logic: save version, compare, and/or show history."""
    try:
        from dataintegrity.core.versioning import DatasetVersion
        from dataintegrity.core.store import LocalVersionStore
        from dataintegrity.integrity.comparison import IntegrityComparator
    except ImportError as exc:
        click.echo(
            click.style(f"⚠  Versioning modules unavailable: {exc}", fg="yellow"),
            err=True,
        )
        return

    store = LocalVersionStore()
    w = 60
    divider = click.style("─" * w, fg="bright_black")

    if show_history:
        _print_version_history(store, filepath, divider)

    if do_track:
        # Load previous before saving current
        previous = store.load_latest(filepath) if store.exists(filepath) else None

        # Create and save current version
        current_version = DatasetVersion(dataset)
        store.save(current_version)
        click.echo(
            f"\n💾  {click.style('Version saved', fg='green', bold=True)}"
            f"  id={click.style(current_version.version_id, fg='cyan')}"
        )

        if previous is not None:
            comparator = IntegrityComparator(config=config)
            report = comparator.compare_versions(current_version, previous)
            _print_comparison_report(report, divider)
        else:
            click.echo(
                click.style(
                    "   No previous version found — this is the baseline.", fg="cyan"
                )
            )


def _print_version_history(store, filepath: str, divider: str) -> None:
    """Render the stored version history for *filepath*."""
    click.echo(f"\n{divider}")
    click.echo(click.style("  VERSION HISTORY", bold=True, fg="bright_white"))
    click.echo(divider)

    versions = store.load_all(filepath)
    if not versions:
        click.echo("  No versions stored for this source.")
        click.echo(divider)
        return

    for i, v in enumerate(reversed(versions), start=1):
        ts = v.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")
        score_str = f"{v.data_score:.1f}" if v.data_score is not None else "n/a"
        click.echo(
            f"  [{i:>2}]  "
            + click.style(ts, fg="cyan")
            + f"  score={click.style(score_str, fg='green', bold=True)}"
            f"  id={click.style(v.version_id, fg='bright_black')}"
        )
    click.echo(divider)


def _print_comparison_report(report: dict, divider: str) -> None:
    """Render the delta comparison report."""
    click.echo(f"\n{divider}")
    click.echo(click.style("  DELTA COMPARISON REPORT", bold=True, fg="bright_white"))
    click.echo(divider)

    prev_score = report["previous_score"]
    curr_score = report["current_score"]
    delta = report["score_delta"]
    severity = report["severity"]

    click.echo(
        f"  Previous DataScore : {click.style(f'{prev_score:.1f}', fg='yellow', bold=True)}"
    )
    click.echo(
        f"  Current  DataScore : {click.style(f'{curr_score:.1f}', fg='yellow', bold=True)}"
    )
    click.echo(
        f"  Delta              : {_delta_style(delta)}"
        f"  ({_severity_style(severity)})"
    )
    click.echo(f"  Baseline version   : {click.style(report['previous_version_id'], fg='bright_black')}")

    # Dimension deltas
    dim_deltas = report.get("dimension_deltas", {})
    if dim_deltas:
        click.echo(f"\n{divider}")
        click.echo(click.style("  DIMENSION DELTAS", bold=True, fg="bright_white"))
        click.echo(divider)
        for dim, ddelta in dim_deltas.items():
            pct_delta = ddelta * 100
            bar = _delta_style(pct_delta)
            click.echo(f"  {dim:<16} {bar}%")

    # Drift report
    drifted = report.get("drifted_columns", [])
    if report.get("drift_available"):
        click.echo(f"\n{divider}")
        click.echo(click.style("  DRIFT DETECTION (KS TEST)", bold=True, fg="bright_white"))
        click.echo(divider)
        if drifted:
            click.echo(
                click.style("  ⚠  Drift detected in:", fg="yellow", bold=True)
            )
            for col in drifted:
                click.echo(f"      - {click.style(col, fg='yellow')}")
        else:
            click.echo(
                click.style("  ✓  No significant drift detected.", fg="green")
            )

    click.echo(f"\n{divider}\n")


# ---------------------------------------------------------------------------
# Audit report renderer
# ---------------------------------------------------------------------------

def _print_audit_report(filepath: str, result: dict, pii_report: dict, dataset: Dataset) -> None:
    """Render a human-readable audit report to stdout."""
    w = 60
    divider = click.style("─" * w, fg="bright_black")

    click.echo(f"\n{divider}")
    click.echo(click.style("  DATA INTEGRITY AUDIT REPORT", bold=True, fg="bright_white"))
    click.echo(divider)

    # Source info
    click.echo(f"  Source    : {filepath}")
    click.echo(f"  Rows      : {result['shape'][0]:,}")
    click.echo(f"  Columns   : {result['shape'][1]}")
    click.echo(f"  Fingerprint: {result['fingerprint'][:32]}…")

    # DataScore
    score = result["data_score"]
    if score >= 90:
        score_color = "green"
    elif score >= 70:
        score_color = "cyan"
    elif score >= 50:
        score_color = "yellow"
    else:
        score_color = "red"

    click.echo(f"\n{divider}")
    click.echo(click.style("  COMPOSITE DATASCORE", bold=True, fg="bright_white"))
    click.echo(divider)
    click.echo(f"  {click.style(_score_bar(score), fg=score_color, bold=True)}")

    # Dimension breakdown
    click.echo(f"\n{divider}")
    click.echo(click.style("  DIMENSION BREAKDOWN", bold=True, fg="bright_white"))
    click.echo(divider)

    for dim, info in result["breakdown"].items():
        raw = info.get("adjusted_score", info.get("raw_score", 0.0))
        label = _dim_label(float(raw))
        severity = info.get("severity", "N/A")
        wt = f"(weight {info['weight']:.0%}, severity {severity})"
        click.echo(f"  {dim:<14} {label:<30} {wt}")

    # PII Report
    click.echo(f"\n{divider}")
    click.echo(click.style("  PII SCAN REPORT", bold=True, fg="bright_white"))
    click.echo(divider)

    pii_hits = {col: info for col, info in pii_report.items() if info["pii_detected"]}
    if not pii_hits:
        click.echo(click.style("  ✓  No PII detected across all columns.", fg="green"))
    else:
        for col, info in pii_hits.items():
            patterns = ", ".join(
                f"{k}:{v}" for k, v in info["patterns_hit"].items() if v > 0
            )
            click.echo(
                click.style(f"  ⚠  {col}", fg="yellow", bold=True)
                + f"  →  {info['count']} row(s) affected  [{patterns}]"
            )

    click.echo(f"\n{divider}\n")
