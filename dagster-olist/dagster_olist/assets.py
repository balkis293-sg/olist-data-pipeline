import os
import subprocess
from pathlib import Path

from dagster import AssetExecutionContext, MetadataValue, asset


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MELTANO_DIR = PROJECT_ROOT / "meltano-olist"
DBT_DIR = PROJECT_ROOT / "dbt_olist"


def run_command(command: list[str], cwd: Path, context: AssetExecutionContext):
    context.log.info(f"Running command: {' '.join(command)}")

    result = subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )

    if result.stdout:
        context.log.info(result.stdout)

    if result.stderr:
        context.log.warning(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(command)}\n"
            f"Exit code: {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )


@asset
def meltano_ingestion(context: AssetExecutionContext):
    """Extract and load raw Olist CSV files into BigQuery using Meltano."""
    run_command(
        ["meltano", "run", "tap-spreadsheets-anywhere", "target-bigquery"],
        MELTANO_DIR,
        context,
    )


@asset(deps=[meltano_ingestion])
def dbt_dependencies(context: AssetExecutionContext):
    """Install dbt package dependencies."""
    run_command(["dbt", "deps"], DBT_DIR, context)


@asset(deps=[dbt_dependencies])
def dbt_run(context: AssetExecutionContext):
    """Build dbt staging, marts, and BI views in BigQuery."""
    run_command(["dbt", "run"], DBT_DIR, context)


@asset(deps=[dbt_run])
def dbt_test(context: AssetExecutionContext):
    """Run dbt data quality tests."""
    run_command(["dbt", "test"], DBT_DIR, context)


@asset(deps=[dbt_test])
def looker_studio_dashboard(context: AssetExecutionContext):
    """BI consumption layer: Looker Studio reads refreshed BigQuery BI views."""

    dashboard_url = "https://datastudio.google.com/reporting/381641d9-3f02-4398-ba32-15b2e9b59934" 

    context.add_output_metadata(
        {
            "dashboard_url": MetadataValue.url(dashboard_url),
            "status": "Looker Studio dashboard is ready after dbt models and tests complete.",
            "source_views": [
                "olist_dwh.bi_customer_rfm",
                "olist_dwh.bi_customer_segments",
                "olist_dwh.bi_segment_summary",
                "olist_dwh.bi_executive_summary",
            ],
        }
    )