#Phase 2 (Data Warehouse Design)

#Step 1: Create the Dimension Tables (The "Who, What, When, Where")

-- 1. Create Date Dimension (dim_date)
CREATE TABLE IF NOT EXISTS `olist-assignment-497915.olist_dwh.dim_date` (
    date_key STRING,           -- e.g., '20180115' (Primary Key)
    full_date DATE,            
    year INT64,
    month INT64,
    quarter INT64,
    day_of_week INT64,
    is_weekend BOOLEAN
);

-- 2. Create Customers Dimension (dim_customers)
CREATE TABLE IF NOT EXISTS `olist-assignment-497915.olist_dwh.dim_customers` (
    customer_key STRING,       -- Primary Key
    customer_id STRING,        
    customer_unique_id STRING,
    customer_city STRING,
    customer_state STRING
);

-- 3. Create Products Dimension (dim_products)
CREATE TABLE IF NOT EXISTS `olist-assignment-497915.olist_dwh.dim_products` (
    product_key STRING,        -- Primary Key
    product_id STRING,
    category_name STRING,      -- Portuguese name
    category_english STRING,   -- Translated name
    weight_g FLOAT64
);

-- 4. Create Sellers Dimension (dim_sellers)
CREATE TABLE IF NOT EXISTS `olist-assignment-497915.olist_dwh.dim_sellers` (
    seller_key STRING,         -- Primary Key
    seller_id STRING,
    seller_city STRING,
    seller_state STRING,
    seller_zip_code_prefix STRING
);

#Step 2: Create the Fact Table (The "Measurable Events")
#Notice how the Fact table holds the Foreign Keys (_key) that will map directly back to the Primary Keys in your dimension tables.

-- 5. Create Order Items Fact (fct_order_items)
CREATE TABLE IF NOT EXISTS `olist-assignment-497915.olist_dwh.fct_order_items` (
    order_item_sk STRING,      -- Surrogate Key for the specific row (Primary Key)
    date_key STRING,           -- Foreign Key -> dim_date
    customer_key STRING,       -- Foreign Key -> dim_customers
    product_key STRING,        -- Foreign Key -> dim_products
    seller_key STRING,         -- Foreign Key -> dim_sellers
    price FLOAT64,
    freight_value FLOAT64,
    review_score INT64
);