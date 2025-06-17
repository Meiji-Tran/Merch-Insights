WITH dem_seg AS (
  SELECT
    entity                                      AS flybuys_membership_number_hash,
    value                                       AS demographic_segment
  FROM bdwprd_apps.customer_attributes.flybuys_household_primary_segment
  WHERE value NOT ILIKE 'unclassifiable%'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY entity ORDER BY timestamp DESC) = 1
),

ir AS (
  SELECT
    item_number,
    country_code,
    ITEM_SUB_DEPARTMENT_NAME                    AS range_name
  FROM {table_name}
),

profile AS (
  SELECT
    ds.demographic_segment,
    ir.range_name,
    -- range metrics
    SUM(IFF(ir.item_number IS NOT NULL, stl.total_exclude_gst_amount, 0))       AS range_sales,
    SUM(IFF(ir.item_number IS NOT NULL, stl.item_quantity,            0))       AS range_quantity,
    COUNT(DISTINCT IFF(ir.item_number IS NOT NULL, stl.dw_loyalty_flybuys_account_id, NULL))
                                                                               AS range_customers,
    -- overall metrics
    SUM(stl.total_exclude_gst_amount)                                        AS overall_sales,
    SUM(stl.item_quantity)                                                   AS overall_quantity,
    COUNT(DISTINCT stl.dw_loyalty_flybuys_account_id)                        AS overall_customers
  FROM bdwprd_cds.sales.sales_transaction_line_fct stl
  JOIN bdwprd_cds.item.item_dim i
    ON stl.dw_item_id = i.dw_item_id
  JOIN bdwprd_cds.loyalty_master.loyalty_flybuys_account_dim fa
    ON stl.dw_loyalty_flybuys_account_id = fa.dw_loyalty_flybuys_account_id
  JOIN dem_seg ds
    ON fa.flybuys_membership_number_hash = ds.flybuys_membership_number_hash
  LEFT JOIN ir
    ON i.item_number  = ir.item_number
   AND i.country_code = ir.country_code
  WHERE stl.sales_reporting_include_ind = TRUE
    AND stl.country_code             = '{country_code}'
    AND stl.transaction_date BETWEEN '{start_date}' AND '{end_date}'
    AND stl.customer_type_code       = '{customer_type}'
    AND i.ITEM_DEPARTMENT_NAME       = '{department_name}'
  GROUP BY
    ds.demographic_segment,
    ir.range_name
)

SELECT
  demographic_segment,
  range_name,
  range_sales,
  overall_sales,
  ROUND(100.0 * range_sales   / NULLIF(overall_sales,0),   2) AS pct_sales,
  range_quantity,
  overall_quantity,
  ROUND(100.0 * range_quantity/ NULLIF(overall_quantity,0),2) AS pct_qty,
  range_customers,
  overall_customers,
  ROUND(100.0 * range_customers/NULLIF(overall_customers,0),2) AS pct_customers
FROM profile
ORDER BY demographic_segment, range_name;