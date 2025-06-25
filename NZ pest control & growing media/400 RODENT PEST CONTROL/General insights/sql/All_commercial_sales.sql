WITH comm_sales AS (
 SELECT
 ds.commercial_industry_segment_code AS DEMOGRAPHIC_SEGMENT,
 COUNT(DISTINCT stl.dw_sales_commercial_account_id) AS N_CUSTOMERS,
 SUM(stl.total_exclude_gst_amount) AS SALES,
 COUNT(DISTINCT stl.dw_sales_transaction_id) AS TOTAL_TRX,
 SUM(sales_quantity) AS TOTAL_UNITS,
 COUNT(DISTINCT i.item_number) AS NUM_ITEMS_PURCHASED,
 COUNT(DISTINCT fa.dw_commercial_account_id) AS TOTAL_SEGMENT_SIZE
 FROM bdwprd_cds.sales.sales_transaction_line_fct stl
 INNER JOIN bdwprd_cds.item.item_dim i
 ON stl.dw_item_id = i.dw_item_id
 RIGHT JOIN bdwprd_cds.commercial.commercial_account_fct fa
 ON stl.dw_sales_commercial_account_id = fa.dw_commercial_account_id
 INNER JOIN BDWPRD_CDS.COMMERCIAL.COMMERCIAL_INDUSTRY_SEGMENT_DIM ds
 ON fa.dw_commercial_industry_segment_id = ds.dw_commercial_industry_segment_id
 INNER JOIN BDWPRD_CDS.LOCATION.LOCATION_DIM loc
 ON loc.dw_location_id = stl.dw_location_id
 WHERE 1=1
 AND stl.sales_reporting_include_ind = TRUE
 AND stl.country_code = 'AU'
 AND dw_sales_commercial_account_id IS NOT NULL
 AND stl.CUSTOMER_TYPE_CODE = 'Commercial'
 AND stl.transaction_date BETWEEN {start_date} AND {end_date}
 GROUP BY commercial_industry_segment_code
 ORDER BY commercial_industry_segment_code DESC
)
SELECT * FROM comm_sales