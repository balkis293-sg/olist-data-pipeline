# Olist Data Pipeline

Module 2 Assignment — Brazilian E-Commerce Analytics Pipeline

This repository contains an end-to-end ELT data project using the **Olist Brazilian E-Commerce Dataset**. The project ingests raw e-commerce CSV files into **Google BigQuery**, transforms them with **dbt**, applies automated data quality tests, and supports downstream Python analysis for customer retention and RFM segmentation.

## Business Objective

The project addresses the question:

> Which customers are most likely to become repeat buyers, and how can Olist increase customer retention?

The final analysis identifies high-value customer groups, at-risk customers, and targeted retention opportunities. Key outputs include customer segmentation, monthly sales trends, top product categories, and executive recommendations.

## Architecture Overview

```text
Olist CSV Files
    ↓
Meltano
    ↓
BigQuery Raw Dataset: olist_raw
    ↓
dbt Core
    ↓
Staging Views: stg_*
    ↓
Star Schema: olist_dwh
    ↓
Python / Pandas Analysis
    ↓
Executive Insights & Slide Deck
```

### Key Tools

| Tool | Purpose |
|---|---|
| Meltano | Extract and load raw CSV files into BigQuery |
| Google BigQuery | Cloud data warehouse for raw and transformed data |
| dbt Core | SQL-based ELT transformations and data tests |
| dbt-expectations | Extended data quality testing |
| Python / pandas | Business analysis and RFM customer segmentation |
| SQLAlchemy | Python connection to BigQuery |

## Repository Structure

```text
olist-data-pipeline/
├── meltano-olist/              # Meltano ingestion project
│   ├── meltano.yml
│   ├── .env.example
│   └── data/                   # Local raw CSV files, not committed
├── dbt_olist/                  # dbt transformation project
│   ├── models/
│   │   ├── staging/            # Cleaned staging views
│   │   └── marts/              # Star schema dimensions and fact table
│   ├── sources.yml
│   ├── schema.yml
│   ├── dbt_project.yml
│   └── packages.yml
├── notebooks/                  # EDA and analysis notebooks
├── docs/                       # Technical report and diagrams
├── slides/                     # Executive presentation deck
└── README.md
```

## Phase 1: Data Ingestion

Raw Olist CSV files are loaded into BigQuery using Meltano.

### Raw Dataset

- BigQuery project: `olist-assignment-497915`
- Raw dataset: `olist_raw`
- Data warehouse dataset: `olist_dwh`

### Raw Tables Loaded

| Raw Table | Source CSV |
|---|---|
| `orders` | `olist_orders_dataset.csv` |
| `customers` | `olist_customers_dataset.csv` |
| `products` | `olist_products_dataset.csv` |
| `order_items` | `olist_order_items_dataset.csv` |
| `order_payments` | `olist_order_payments_dataset.csv` |
| `order_reviews` | `olist_order_reviews_dataset.csv` |
| `sellers` | `olist_sellers_dataset.csv` |
| `category_translation` | `product_category_name_translation.csv` |

Approximate total records loaded: **550,785**.

### Meltano Setup

1. Download the Olist dataset from Kaggle:

   https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

2. Place the raw CSV files inside:

```bash
meltano-olist/data/
```

3. Copy the environment template:

```bash
cd meltano-olist
cp .env.example .env
```

4. Fill in the required values in `.env`:

```text
GCP_PROJECT_ID=<your-gcp-project-id>
DATA_DIR=<path-to-local-data-folder>
GOOGLE_APPLICATION_CREDENTIALS=<path-to-service-account-json>
```

5. Run Meltano ingestion according to the project configuration.

## Phase 2 and 3: Data Warehouse Design and ELT Pipeline

The dbt project transforms raw BigQuery tables into cleaned staging views and a dimensional star schema.

### dbt Models

#### Staging Views

| Model | Purpose |
|---|---|
| `stg_customers` | Cleans customer IDs and location fields |
| `stg_orders` | Standardises order IDs, statuses, and timestamps |
| `stg_order_items` | Cleans order-item prices and freight values |
| `stg_payments` | Standardises payment values, types, and instalments |
| `stg_products` | Cleans product attributes |
| `stg_reviews` | Cleans review IDs and review scores |
| `stg_sellers` | Cleans seller IDs and location fields |

#### Star Schema Marts

| Model | Type | Purpose |
|---|---|---|
| `dim_customers` | Dimension | Customer identifiers and location attributes |
| `dim_products` | Dimension | Product metadata and attributes |
| `dim_sellers` | Dimension | Seller identifiers and location attributes |
| `dim_date` | Dimension | Calendar table for time-series reporting |
| `fact_orders` | Fact | Order-item level transaction table |

The `fact_orders` table is built at the **order-item grain**, meaning:

```text
1 row = 1 product within 1 order
```

This preserves product-level and seller-level sales detail.

Core measures include:

| Measure | Description |
|---|---|
| `price` | Product sale price for the order item |
| `freight_value` | Shipping cost for the order item |
| `total_sale_amount` | Derived metric: `price + freight_value` |

## dbt Setup

From the repository root:

```bash
cd dbt_olist
```

### 1. Activate Conda Environment

Use the project Conda environment:

```bash
conda activate olist
```

If your local environment is named differently, activate the environment that contains dbt and the BigQuery adapter.

### 2. Configure `profiles.yml`

Create or update `dbt_olist/profiles.yml`:

```yaml
olist_data_pipeline:
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: olist-assignment-497915
      dataset: olist_dwh
      location: US
      keyfile: /absolute/path/to/your/service-account.json
      threads: 4
      priority: interactive
      job_execution_timeout_seconds: 300
      job_retries: 1
  target: dev
```

Do **not** commit service account JSON files to GitHub.

### 3. Install dbt Packages

```bash
dbt deps
```

### 4. Validate and Build

```bash
dbt parse
dbt run
dbt test
```

Expected dbt build output:

```text
PASS=12 WARN=0 ERROR=0 SKIP=0 TOTAL=12
```

Expected dbt test output:

```text
PASS=68 WARN=0 ERROR=0 SKIP=0 TOTAL=68
```

## Data Quality Testing

Data quality is implemented directly in dbt using:

- dbt built-in generic tests
- dbt-expectations tests

### Test Coverage

| Test Type | Purpose |
|---|---|
| `not_null` | Ensures required fields are populated |
| `unique` | Ensures primary keys are not duplicated |
| `relationships` | Validates foreign key relationships between fact and dimension tables |
| value range tests | Checks prices, freight, payments, review scores, and date fields |
| format tests | Validates state-code length and date-key format |
| row count tests | Detects unexpected data loss in major tables |

The test suite protects against broken joins, invalid financial values, missing keys, and incomplete loads.

## Phase 5: Python Analysis

The analysis phase connects to BigQuery using SQLAlchemy and pandas.

Main analyses include:

| Analysis | Purpose |
|---|---|
| Monthly sales trends | Understand revenue and order volume over time |
| Top-selling products | Identify high-value product categories |
| Customer segmentation | Group customers by purchase behaviour using RFM analysis |
| Business recommendations | Prioritise retention and win-back campaigns |

RFM metrics are calculated in Python using the clean warehouse tables, allowing analysts to adjust scoring thresholds without rebuilding the dbt warehouse.

## Key Business Findings

| Finding | Business Meaning |
|---|---|
| 75% of customers purchased only once | Olist has a major repeat-purchase challenge |
| Average order frequency is around 1.03 | Repeat buying is currently limited |
| At-Risk customers represent about R$3.69M in historical revenue | Strong win-back campaign opportunity |
| Champions are 6.5% of customers but drive 12.1% of revenue | High-value customers should be protected |
| Health & Beauty is the highest-revenue category | Marketing should consider value, not only unit volume |

## Recommended Actions

1. **Win Back At-Risk Customers**  
   Target dormant customers with personalised discounts and reactivation campaigns.

2. **Protect Champion Customers**  
   Offer loyalty perks, priority service, and VIP benefits to high-value customers.

3. **Convert Promising Customers**  
   Use second-purchase campaigns to turn recent one-time buyers into repeat customers.

## Documentation and Deliverables

| Deliverable | Location |
|---|---|
| Technical report | `docs/technical_report.md` |
| Data flow diagram | `docs/data_flow_pipeline.png` |
| dbt project | `dbt_olist/` |
| Meltano project | `meltano-olist/` |
| Analysis notebooks | `notebooks/` |
| Executive slide deck | `slides/` |

## Notes

- The geolocation dataset was excluded from the core warehouse scope because the main business problem focuses on customer retention and repeat purchase behaviour.
- Pipeline orchestration was considered but scoped out of the MVP. Future work may include Dagster or Airflow for automated scheduling.
- Service account credentials, `.env` files, raw data files, and local system files such as `.DS_Store` should not be committed.

## Common Commands

```bash
# dbt
cd dbt_olist
dbt deps
dbt parse
dbt run
dbt test

# Git
git status
git add docs/technical_report.md README.md
git commit -m "Update project documentation"
git push origin elt_part_clean
```

## Orchestration 

```bash 
cd dagster-olist
dagster dev
```

1. Open the link from terminal (using Cmd/Cntrl + Click)
2. Click on "Materialize Asset" and run all assets