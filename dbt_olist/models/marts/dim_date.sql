-- models/marts/dim_date.sql

{{ config(materialized='table') }}

/* Step A: Create a "Spine" 
  We use BigQuery's built-in generator to create a continuous list of every single day 
  from January 1, 2016 to December 31, 2018 (covering the whole Olist dataset timeline).
*/
WITH date_spine AS (
    SELECT date_day
    FROM UNNEST(
        GENERATE_DATE_ARRAY(DATE('2016-01-01'), DATE('2018-12-31'), INTERVAL 1 DAY)
    ) AS date_day
)

/* Step B: Extract the details
  Now we take that simple list of dates and pull out the year, month, and weekend flags!
*/
SELECT
    -- This turns '2018-01-15' into a clean string '20180115' to match your schema diagram
    CAST(FORMAT_DATE('%Y%m%d', date_day) AS STRING) AS date_key,
    
    date_day AS full_date,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,
    
    -- In BigQuery, Sunday is 1 and Saturday is 7. This checks if the day is a weekend.
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN TRUE 
        ELSE FALSE
    END AS is_weekend

FROM date_spine