WITH comm_sales AS (
SELECT
ds.commercial_industry_segment_code AS demographic_segment,
i.item_category_name,
i.item_department_name,

COUNT(DISTINCT stl.dw_sales_commercial_account_id) AS n_customers,
SUM(stl.total_exclude_gst_amount) AS sales,
COUNT(DISTINCT stl.dw_sales_transaction_id) AS total_trx,
SUM(stl.sales_quantity) AS total_units,
COUNT(DISTINCT i.item_number) AS num_items_purchased
FROM bdwprd_cds.sales.sales_transaction_line_fct stl
INNER JOIN bdwprd_cds.item.item_dim i
ON stl.dw_item_id = i.dw_item_id
INNER JOIN bdwprd_cds.commercial.commercial_account_fct fa
ON stl.dw_sales_commercial_account_id = fa.dw_commercial_account_id
INNER JOIN bdwprd_cds.commercial.commercial_industry_segment_dim ds
ON fa.dw_commercial_industry_segment_id = ds.dw_commercial_industry_segment_id
INNER JOIN bdwprd_cds.location.location_dim loc
ON loc.dw_location_id = stl.dw_location_id
WHERE stl.sales_reporting_include_ind = TRUE
AND stl.country_code = 'AU'
AND stl.dw_sales_commercial_account_id IS NOT NULL
AND stl.customer_type_code = 'Commercial'
AND stl.transaction_date BETWEEN {start_date} AND {end_date}
AND i.item_department_name = {dept}
{additional_trx_condition}
{target_item_condition}

GROUP BY
ds.commercial_industry_segment_code,
i.item_category_name,
i.item_department_name
),
segment_sizes AS (
SELECT
ds.commercial_industry_segment_code AS demographic_segment,
COUNT(DISTINCT fa.dw_commercial_account_id) AS total_segment_size
FROM bdwprd_cds.commercial.commercial_account_fct fa
INNER JOIN bdwprd_cds.commercial.commercial_industry_segment_dim ds
ON fa.dw_commercial_industry_segment_id = ds.dw_commercial_industry_segment_id
GROUP BY ds.commercial_industry_segment_code
)

SELECT
cs.*,
ss.total_segment_size
FROM comm_sales cs
LEFT JOIN segment_sizes ss
ON cs.demographic_segment = ss.demographic_segment
ORDER BY cs.demographic_segment DESC;