# olist-data-pipeline
Module 2 Assignment - ELT Pipeline with Olist Dataset

## Setup

1. Download the Olist dataset from Kaggle:
   https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

2. We are using the elt environment.yml setup for this project, run the below command on terminal first. 

```bash
conda activate elt
```

### Phase 1: Meltano 

3. Place all the raw Olist CSV files in `meltano-olist/data/`

4. Copy .env.example to .env and fill in your values:
   - GCP Project ID
   - Path to your local data folder
   - Path to your GCP credentials JSON

### Phase 2: dbt Warehousing -> dbt_olist folder 

5. Save JSON file for service authorization from GCP for your project, into a local folder, and copy the path to JSON file 

6. Update profiles,yml file inside dbt folder 

```bash 
olist_data_pipeline:   # name of data set, must match what is on the dbt_project.yml file 
  outputs:
    dev:
      dataset: olist_dwh # name of output dataset inside GCP 
      job_execution_timeout_seconds: 300 # default 
      job_retries: 1 # default 
      keyfile: /../<Service Auth file name>.json #copy paste file path from step 4 
      location: US # default
      method: service-account 
      priority: interactive # default 
      project: <enter name of project from GCP> # change to name of dataset
      threads: 4 # default 
      type: bigquery 
  target: dev # default 
```

7. update dbt_project.yml (follow example below)

```bash
# Name your project! Project names should contain only lowercase characters
# and underscores. A good package name should reflect your organization's
# name or the intended use of these models
name: 'olist_data_pipeline'
version: '1.0.0'

# This setting configures which "profile" dbt uses for this project.
profile: 'olist_data_pipeline'

# These configurations specify where dbt should look for different types of files.
# The `model-paths` config, for example, states that models in this project can be
# found in the "models/" directory. You probably won't need to change these!
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:         # directories to be removed by `dbt clean`
  - "target"
  - "dbt_packages"

# Configuring models
# Full documentation: https://docs.getdbt.com/docs/configuring-models

# In this example config, we tell dbt to build all models in the example/
# directory as views. These settings can be overridden in the individual model
# files using the `{{ config(...) }}` macro.
models:
  olist_data_pipeline:
    # Config indicated by + and applies to all files under models/example/
    tables:
      +materialized: table or view 
```

8. dbt commands to run staging and tests - ensure that all commands run successfully 

```bash

dbt debug 

dbt parse

dbt run 

dbt test
```

