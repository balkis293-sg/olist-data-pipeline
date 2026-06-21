from dagster import Definitions, ScheduleDefinition, define_asset_job

from dagster_olist.assets import (
    dbt_dependencies,
    dbt_run,
    dbt_test,
    looker_studio_dashboard,
    meltano_ingestion,
)


olist_pipeline_job = define_asset_job(
    name="olist_pipeline_job",
    selection=[
        "meltano_ingestion",
        "dbt_dependencies",
        "dbt_run",
        "dbt_test",
        "looker_studio_dashboard",
    ],
)


daily_olist_schedule = ScheduleDefinition(
    job=olist_pipeline_job,
    cron_schedule="0 9 * * *",
)


defs = Definitions(
    assets=[
        meltano_ingestion,
        dbt_dependencies,
        dbt_run,
        dbt_test,
        looker_studio_dashboard,
    ],
    jobs=[olist_pipeline_job],
    schedules=[daily_olist_schedule],
)