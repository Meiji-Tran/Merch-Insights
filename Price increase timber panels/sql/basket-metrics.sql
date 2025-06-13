WITH impacted_txns AS (
  SELECT DISTINCT
    stl.dw_sales_transaction_id AS txn_id,
    DATE_TRUNC('WEEK', stl.transaction_date) AS week_start
  FROM bdwprd_cds.sales.sales_transaction_line_fct stl
  JOIN bdwprd_cds.item.item_dim i
    ON stl.dw_item_id = i.dw_item_id
  WHERE
    stl.transaction_date BETWEEN {start_date} AND {end_date}
    AND i.item_number IN {product_group_items}
    AND stl.sales_reporting_include_ind = TRUE
    {loyalty_filter}
    AND stl.customer_type_code = 'Consumer'
    AND stl.country_code = {country}
),

basket_lines AS (
  SELECT
    stl.dw_sales_transaction_id AS txn_id,
    DATE_TRUNC('WEEK', stl.transaction_date) AS week_start,
    stl.total_exclude_gst_amount AS line_value,
    stl.sales_quantity           AS line_qty
  FROM bdwprd_cds.sales.sales_transaction_line_fct stl
  WHERE stl.dw_sales_transaction_id IN (SELECT txn_id FROM impacted_txns)
    AND stl.transaction_date BETWEEN {start_date} AND {end_date}
    AND stl.sales_reporting_include_ind = TRUE
    {loyalty_filter}
    AND stl.customer_type_code = 'Consumer'
    AND stl.country_code = {country}
),

basket_summary AS (
  SELECT
    txn_id,
    week_start,
    SUM(line_value) AS basket_value,
    SUM(line_qty)   AS basket_qty
  FROM basket_lines
  GROUP BY txn_id, week_start
),

weekly_metrics AS (
  SELECT
    bs.week_start,
    dd.merchant_week_of_year AS merchant_week,
    AVG(bs.basket_value) AS avg_basket_value,
    AVG(bs.basket_qty)   AS avg_basket_qty
  FROM basket_summary bs
  JOIN BDWPRD_CDS.COMMON.DATE_DIM dd
    ON dd.day_date = bs.week_start
  WHERE dd.merchant_week_of_year BETWEEN 26 AND 45
  GROUP BY bs.week_start, dd.merchant_week_of_year
)

-- **Bring merchant_week into your final output!**
SELECT
  merchant_week,
  avg_basket_value,
  avg_basket_qty
FROM weekly_metrics
ORDER BY merchant_week;