WITH seed_txns AS (
  SELECT DISTINCT
    stl.dw_sales_transaction_id AS txn_id,
    DATE_TRUNC('WEEK', stl.transaction_date) AS week_start
  FROM bdwprd_cds.sales.sales_transaction_line_fct stl
  JOIN bdwprd_cds.item.item_dim i
    ON stl.dw_item_id = i.dw_item_id
  WHERE 1=1
    AND stl.transaction_date BETWEEN {start_date} AND {end_date}
    AND i.item_number     IN {product_group_items}
    AND i.item_department_name = '300 INDOOR TIMBER AND BOARDS'
    AND stl.sales_reporting_include_ind = TRUE
    {loyalty_filter}
    AND stl.customer_type_code = 'Consumer'
    AND stl.country_code = {country}
),

cross_sums AS (
  SELECT
    s.week_start,
    dim.item_department_name,
    dim.item_sub_department_name,
    dim.item_class_name,
    dim.item_sub_class_name,
    dim.item_number,
    dim.item_description,
    SUM(stl.sales_quantity)                     AS total_qty,
    COUNT(DISTINCT stl.dw_sales_transaction_id) AS txn_count
  FROM seed_txns s
  JOIN bdwprd_cds.sales.sales_transaction_line_fct stl
    ON s.txn_id = stl.dw_sales_transaction_id
  JOIN bdwprd_cds.item.item_dim dim
    ON stl.dw_item_id = dim.dw_item_id
  WHERE 1=1
    AND stl.transaction_date BETWEEN {start_date} AND {end_date}
    AND dim.item_department_name = '300 INDOOR TIMBER AND BOARDS'
    AND dim.item_number NOT IN {product_group_items}
    AND stl.sales_reporting_include_ind = TRUE
    {loyalty_filter}
    AND stl.customer_type_code = 'Consumer'
    AND stl.country_code = {country}
  GROUP BY
    s.week_start,
    dim.item_department_name,
    dim.item_sub_department_name,
    dim.item_class_name,
    dim.item_sub_class_name,
    dim.item_number,
    dim.item_description
),

ranked AS (
  SELECT
    cs.week_start,
    dd.merchant_week_of_year     AS merchant_week,
    cs.item_department_name,
    cs.item_sub_department_name,
    cs.item_class_name,
    cs.item_sub_class_name,
    cs.item_number,
    cs.item_description,
    cs.total_qty / cs.txn_count   AS avg_qty_per_txn,
    ROW_NUMBER() OVER (
      PARTITION BY cs.week_start
      ORDER BY cs.total_qty/cs.txn_count DESC
    )                             AS rank
  FROM cross_sums cs
  JOIN BDWPRD_CDS.COMMON.DATE_DIM dd
    ON dd.day_date = cs.week_start
  WHERE dd.merchant_week_of_year BETWEEN 26 AND 45
)

SELECT
  merchant_week,
  rank,
  item_department_name,
  item_sub_department_name,
  item_class_name,
  item_sub_class_name,
  item_number,
  item_description,
  ROUND(avg_qty_per_txn, 2) AS avg_quantity_per_txn
FROM ranked
--WHERE rank <= 10
ORDER BY merchant_week, rank;
