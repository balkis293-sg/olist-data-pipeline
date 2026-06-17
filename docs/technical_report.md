# Technical Report: Olist E-Commerce Data Pipeline

> **Project:** Brazilian E-Commerce Analytics Pipeline
> **Dataset:** [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?resource=download)

---

## Table of Contents

1. [Data Ingestion](#1-data-ingestion)
2. [Data Warehouse Design](#2-data-warehouse-design) *(lizhou)*
3. [ELT Pipeline](#3-elt-pipeline) *(Balkis)*
4. [Data Quality Testing](#4-data-quality-testing) *(qiuxuan)*
5. [Data Analysis with Python](#5-data-analysis-with-python) *(Maegala)*
6. [Pipeline Orchestration](#6-pipeline-orchestration) *(Optional)*
7. [Architecture Overview](#7-architecture-overview)

---

## 1. Data Ingestion

**Owner:** Member 1
**Tool:** [Meltano](https://meltano.com/) v3.x
**Destination:** Google BigQuery (`olist_raw` dataset)

### 1.1 Objective

The goal of this stage is to extract raw CSV data from the Olist Kaggle dataset and load it into Google BigQuery without any transformation. This establishes a stable raw layer that all downstream pipeline stages can build upon.

### 1.2 Tool Selection: Meltano

Meltano was chosen over a custom Python script because it handles schema inference, batch uploading, and incremental state management out of the box — reducing boilerplate code and making the pipeline easier to maintain and reproduce across team members.

### 1.3 Plugin Configuration

Two Meltano plugins were used:

- **Extractor — `tap-spreadsheets-anywhere`:** Reads CSV files from local paths, automatically inferring schemas from column headers.
- **Loader — `target-bigquery`:** Writes extracted records into BigQuery tables, with each CSV mapping to one table in the `olist_raw` dataset.

### 1.4 Tables Loaded

All 8 tables were successfully loaded into `olist-assignment-497915.olist_raw`:

| Table | Description | Approx. Row Count |
|---|---|---|
| `orders` | Core order records with status and timestamps | ~99,000 |
| `customers` | Customer location and unique identifiers | ~99,000 |
| `order_items` | Line items per order (product, seller, price) | ~112,000 |
| `order_payments` | Payment method and value per order | ~103,000 |
| `order_reviews` | Customer review scores and comments | ~100,000 |
| `products` | Product metadata and category names (Portuguese) | ~33,000 |
| `sellers` | Seller location information | ~3,000 |
| `category_translation` | Portuguese → English category name mapping | ~71 |

**Total records loaded: ~550,785**

### 1.5 Raw Data Observations

Preliminary exploratory analysis (`notebooks/eda_raw_data.ipynb`) revealed the following, which downstream stages should account for:

- Timestamp columns are loaded as `STRING` type in BigQuery — these must be cast to `TIMESTAMP` in staging models.
- `order_reviews`: `review_comment_title` and `review_comment_message` contain a high proportion of NULLs (expected, as these are optional fields).
- `products`: approximately 1.6% of rows have a NULL `product_category_name`.
- `order_payments`: multiple rows per `order_id` are expected (one row per payment installment). Aggregation must be handled carefully to avoid fan-out in the fact table.
- `order_items`: the primary key is a composite of `order_id` + `order_item_id`, not `order_id` alone.
- Orders span from **September 2016 to October 2018**, with volume peaking in **late 2017 through mid-2018**.

### 1.6 Design Decision

Following the ELT pattern, raw data is loaded into BigQuery before any transformation. This preserves the original source data, allows transformations to be re-run without re-ingesting, and provides a clear audit trail for data lineage.

---

*Sections 2–7 to be completed by others*

## 4. Data Quality Testing
**Tool:** [dbt Tests](https://docs.getdbt.com/docs/build/data-tests) — Built-in Generic, Singular, and [dbt-expectations](https://github.com/calogica/dbt-expectations)
**Location:** `models/schema.yml`, `models/schema_expectations.yml`, and `tests/`
---
### 4.1 Objective
Ensure data integrity, referential consistency, and business-logic correctness across all layers of the warehouse — from staging through to the final RFM analytics mart. Tests act as automated guardrails that catch problems before bad data reaches analysts or downstream Python analysis notebooks.
---
### 4.2 Testing Framework
All tests run under a single command (`dbt test`). We use three complementary test types:
| Type | Source File | What It Validates |
|------|-------------|-------------------|
| Built-in Generic | `schema.yml` | Nulls, uniqueness, referential integrity (FK → PK) |
| dbt-expectations | `schema_expectations.yml` | Value ranges, regex patterns, data types, row counts, distributions |
| Singular | `tests/*.sql` | Cross-column business logic that YAML cannot express |
#### Why Three Types?
1. **Built-in generics** are irreplaceable for `relationships` tests (foreign key validation) and provide the clearest syntax for `not_null` / `unique`.
2. **dbt-expectations** covers everything that built-in generics cannot: range checks, string patterns, statistical distribution bounds, and table-level row counts.
3. **Singular tests** remain for multi-column conditional logic — specifically, verifying that derived boolean flags are consistent with the underlying numeric metrics.
---
#### Why use DBT expectation over Great Expectation?
1. transformations are happening in BigQuery so we choose dbt-expectations (or dbt's built-in tests) over Great Expectations because it keeps data quality checks inside the same workflow as the transformations.
2. Benefits:
    - No data movement
    - No separate execution environment
    - Leverages BigQuery's compute engine
    - Simpler architecture
---
### 4.3 Installation
```yaml
# dbt_olist/packages.yml
packages:
- package: calogica/dbt_expectations
version: [">=0.10.0", "<0.11.0"]
dbt deps
---
```
4.4 Built-in Generic Tests (schema.yml) — 38 Tests
====================================================

STAGING LAYER (7 models, 13 tests)
-----------------------------------
Model              | Column              | Tests
-------------------|---------------------|---------------------------
stg_customers      | customer_id         | not_null
stg_customers      | customer_unique_id  | not_null
stg_orders         | order_id            | not_null, unique
stg_orders         | customer_id         | not_null
stg_order_items    | order_id            | not_null
stg_order_items    | product_id          | not_null
stg_order_items    | seller_id           | not_null
stg_payments       | order_id            | not_null
stg_payments       | payment_value       | not_null
stg_products       | product_id          | not_null, unique
stg_sellers        | seller_id           | not_null, unique
stg_reviews        | review_id           | not_null
stg_reviews        | order_id            | not_null

DIMENSION & FACT TABLES (5 models, 15 tests)
----------------------------------------------
Model              | Column              | Tests
-------------------|---------------------|---------------------------
dim_customers      | customer_key        | not_null, unique
dim_products       | product_key         | not_null, unique
dim_sellers        | seller_key          | not_null, unique
dim_date           | date_key            | not_null, unique
fact_orders        | order_item_sk       | not_null, unique
fact_orders        | order_key           | not_null
fact_orders        | customer_key        | not_null, relationships -> dim_customers
fact_orders        | product_key         | not_null, relationships -> dim_products
fact_orders        | seller_key          | not_null, relationships -> dim_sellers
fact_orders        | date_key            | not_null, relationships -> dim_date

INTERMEDIATE & RFM MART (3 models, 10 tests)
----------------------------------------------
Model              | Column              | Tests
-------------------|---------------------|---------------------------
int_order_payments | order_id            | not_null, unique
int_order_payments | order_revenue       | not_null
int_customer_orders| customer_unique_id  | not_null
int_customer_orders| order_id            | not_null
int_customer_orders| order_revenue       | not_null
fct_customer_rfm   | customer_unique_id  | not_null, unique
fct_customer_rfm   | recency_days        | not_null
fct_customer_rfm   | frequency           | not_null
fct_customer_rfm   | monetary_value      | not_null
fct_customer_rfm   | customer_segment    | not_null


4.5 dbt-expectations Tests (schema.yml) — 30 Tests
====================================================

STAGING LAYER — Type, Range & Set Validation
----------------------------------------------
Model              | Column                    | Expectation                                          | Purpose
-------------------|---------------------------|------------------------------------------------------|----------------------------------
stg_orders         | order_id                  | expect_column_values_to_be_of_type: string           | Confirms type post-ingestion
stg_orders         | order_status              | expect_column_values_to_be_in_set (8 values)         | Only valid statuses allowed
stg_orders         | order_purchase_timestamp  | expect_column_values_to_be_of_type: timestamp        | Timestamp cast succeeded
stg_payments       | payment_value             | expect_column_values_to_be_between: [0, 100000]      | No negatives or absurd values
stg_payments       | payment_type              | expect_column_values_to_be_in_set (5 values)         | Only valid payment methods
stg_payments       | payment_installments      | expect_column_values_to_be_between: [0, 24]          | Max 24 installments
stg_order_items    | price                     | expect_column_values_to_be_between: >0               | Prices must be positive
stg_order_items    | freight_value             | expect_column_values_to_be_between: >=0              | Freight never negative
stg_products       | product_weight_g          | expect_column_values_to_be_between: >0 (warn)        | Positive weight expected
stg_reviews        | review_score              | expect_column_values_to_be_between: [1, 5]           | Valid star rating

DIMENSION TABLES — Shape & Format Validation
----------------------------------------------
Model              | Column / Table            | Expectation                                          | Purpose
-------------------|---------------------------|------------------------------------------------------|----------------------------------
dim_customers      | (table)                   | expect_table_row_count_to_be_between: [90000, 110000]| No catastrophic data loss
dim_customers      | customer_state            | expect_column_value_lengths_to_equal: 2              | Brazilian state code format
dim_sellers        | (table)                   | expect_table_row_count_to_be_between: [3000, 4000]   | Expected seller population
dim_sellers        | seller_state              | expect_column_value_lengths_to_equal: 2              | State code format
dim_sellers        | seller_zip_code_prefix    | expect_column_value_lengths_to_equal: 5              | Zip prefix format
dim_date           | (table)                   | expect_table_row_count_to_equal: 1096                | Exactly 3 years, no gaps
dim_date           | date_key                  | expect_column_value_lengths_to_equal: 8              | YYYYMMDD is 8 chars
dim_date           | date_key                  | expect_column_values_to_match_regex: ^[0-9]{8}$      | Numeric format only
dim_date           | year                      | expect_column_values_to_be_in_set: [2016, 2017, 2018]| Only expected years
dim_date           | month                     | expect_column_values_to_be_between: [1, 12]          | Valid months
dim_date           | quarter                   | expect_column_values_to_be_between: [1, 4]           | Valid quarters
dim_date           | day_of_week               | expect_column_values_to_be_between: [1, 7]           | Valid day numbers

FACT & INTERMEDIATE — Value & Distribution Validation
------------------------------------------------------
Model              | Column                    | Expectation                                          | Purpose
-------------------|---------------------------|------------------------------------------------------|----------------------------------
fact_orders        | (table)                   | expect_table_row_count_to_be_between: [100000, 130000]| Row count sanity
fact_orders        | price                     | expect_column_values_to_be_between: >0               | Positive prices
fact_orders        | price                     | expect_column_mean_to_be_between: [50, 200]          | Average in expected range
fact_orders        | freight_value             | expect_column_values_to_be_between: >=0              | Non-negative freight
fact_orders        | date_key                  | expect_column_values_to_match_regex (YYYYMMDD)       | Valid date format + range
int_order_payments | order_revenue             | expect_column_values_to_be_between: >0               | Positive revenue
int_order_payments | order_revenue             | expect_column_quantile_values_to_be_between: med [50, 200] | Distribution check
int_customer_orders| order_status              | expect_column_values_to_be_in_set: ['delivered']     | Filter logic verified
int_customer_orders| order_revenue             | expect_column_values_to_be_between: >=0              | Non-negative revenue

RFM MART — Segmentation Logic Validation
------------------------------------------
Model              | Column                    | Expectation                                          | Purpose
-------------------|---------------------------|------------------------------------------------------|----------------------------------
fct_customer_rfm   | (table)                   | expect_table_row_count_to_be_between: [90000, 100000]| ~95,420 expected customers
fct_customer_rfm   | recency_days              | expect_column_values_to_be_between: [0, 800]         | Non-negative, bounded
fct_customer_rfm   | recency_days              | expect_column_mean_to_be_between: [100, 400]         | Average recency sanity
fct_customer_rfm   | frequency                 | expect_column_values_to_be_between: [1, 50]          | At least 1, cap outliers
fct_customer_rfm   | frequency                 | expect_column_mean_to_be_between: [1, 1.5]           | Validates ~1.03 from analysis
fct_customer_rfm   | monetary_value            | expect_column_values_to_be_between: [0, 100000]      | Non-negative, bounded
fct_customer_rfm   | avg_order_value           | expect_column_values_to_be_between: >0               | Positive AOV
fct_customer_rfm   | customer_segment          | expect_column_values_to_be_in_set (5 segments)       | Only valid labels
fct_customer_rfm   | customer_segment          | expect_column_distinct_count_to_equal: 5             | All 5 segments populated
fct_customer_rfm   | is_repeat_buyer           | expect_column_values_to_be_in_set: [true, false]     | Boolean only
fct_customer_rfm   | is_churn_risk             | expect_column_values_to_be_in_set: [true, false]     | Boolean only


4.6 Singular Tests (tests/) — 2 Tests
=======================================

File                                           | Rule Enforced
-----------------------------------------------|-----------------------------------------------------
assert_rfm_repeat_buyer_flag_consistent.sql    | is_repeat_buyer = true if and only if frequency >= 2
assert_rfm_churn_risk_flag_consistent.sql      | is_churn_risk = true if and only if recency_days > 180

Example (assert_rfm_repeat_buyer_flag_consistent.sql):

    select customer_unique_id, frequency, is_repeat_buyer
    from {{ ref('fct_customer_rfm') }}
    where (frequency >= 2 and is_repeat_buyer = false)
       or (frequency < 2 and is_repeat_buyer = true)

Returns rows where the flag contradicts the metric. Zero rows = test passes.


4.7 Test Organisation by Failure Domain
=========================================

    Source -> Staging -> Intermediate -> Star Schema -> RFM Mart
              ----------------------------+---------   ----+----
                            Star schema tests          RFM tests

Scenario                                   | What It Tells You
-------------------------------------------|---------------------------------------------------
Star schema tests FAIL                     | Root cause is upstream (bad source data or staging SQL)
Star schema tests PASS, RFM tests FAIL     | Problem is isolated to fct_customer_rfm.sql logic
All tests PASS                             | Pipeline is healthy and production-ready


4.8 How to Run
===============

    # Install packages
    dbt deps

    # Run all 70 tests
    dbt test

    # Run by model
    dbt test --select fact_orders
    dbt test --select fct_customer_rfm

    # Run by type
    dbt test --select test_type:generic
    dbt test --select test_type:singular


4.9 Results
============

    Completed with 0 errors, 0 warnings and 0 failures.
    Done. PASS=70  WARN=0  ERROR=0  SKIP=0  TOTAL=70

Category                              | Count
--------------------------------------|------
Built-in generic tests (schema.yml)   | 38
dbt-expectations tests (schema.yml)   | 30
Singular tests (tests/)               | 2
TOTAL                                 | 70


4.10 Design Decisions
======================

Decision                                          | Rationale
--------------------------------------------------|--------------------------------------------------
Three test types working together                 | Each covers a different class of data defect
Built-in generics for FK validation               | relationships is irreplaceable (no dbt-expectations equivalent)
dbt-expectations for value/range rules            | Declarative YAML is easier to maintain than custom SQL
Singular tests only for cross-column logic        | Minimise custom SQL maintenance
Distribution tests (mean, quantile)               | Catches subtle drift that row-level checks miss
Row count bounds on all major tables              | Smoke test against catastrophic data loss
Separate star schema vs. RFM tests                | Enables faster debugging through elimination
