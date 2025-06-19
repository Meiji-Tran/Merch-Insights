-- Commercial industry segments
-- Gather 40 largest industry segments
WITH top_industry_segments AS (
    SELECT
        cis.commercial_industry_segment_code,
        COUNT(ca.dw_commercial_account_id) AS segment_size
    FROM bdwprd_cds.commercial.commercial_account_fct ca
    INNER JOIN bdwprd_cds.commercial.commercial_industry_segment_dim cis
        ON ca.dw_commercial_industry_segment_id = cis.dw_commercial_industry_segment_id
    GROUP BY
        cis.commercial_industry_segment_code
    ORDER BY
        segment_size DESC
    LIMIT {n_commercial_segments}
)
SELECT
    i.item_class_name,
    cis.commercial_industry_segment_code,
    -- Item range metrics
    SUM(IFF(ir.item_number IS NOT NULL, stl.total_exclude_gst_amount, NULL)) AS range_sales,
    SUM(IFF(ir.item_number IS NOT NULL, stl.item_quantity, NULL)) AS range_quantity,
    COUNT(DISTINCT IFF(ir.item_number IS NOT NULL, stl.dw_commercial_account_id, NULL)) AS range_customers,
    -- Item range shares
    range_sales / SUM(range_sales) OVER () AS range_sales_share,
    range_quantity / SUM(range_quantity) OVER () AS range_quantity_share,
    range_customers / SUM(range_customers) OVER () AS range_customer_share,
    -- Overall metrics
    SUM(stl.total_exclude_gst_amount) AS overall_sales,
    SUM(stl.item_quantity) AS overall_quantity,
    COUNT(DISTINCT stl.dw_commercial_account_id) AS overall_customers,
    -- Overall shares
    overall_sales / SUM(overall_sales) OVER () AS overall_sales_share,
    overall_quantity / SUM(overall_quantity) OVER () AS overall_quantity_share,
    overall_customers / SUM(overall_customers) OVER () AS overall_customer_share,
    -- Indexes
    range_sales_share / overall_sales_share AS sales_index,
    range_quantity_share / overall_quantity_share AS quantity_index,
    range_customer_share / overall_customer_share AS customer_index
FROM bdwprd_cds.sales.sales_transaction_line_fct stl
INNER JOIN bdwprd_cds.item.item_dim i
    ON stl.dw_item_id = i.dw_item_id
INNER JOIN bdwprd_cds.location.location_dim l
    ON stl.dw_location_id = l.dw_location_id
INNER JOIN bdwprd_cds.commercial.commercial_account_fct ca
    ON stl.dw_commercial_account_id = ca.dw_commercial_account_id
INNER JOIN bdwprd_cds.commercial.commercial_industry_segment_dim cis
    ON ca.dw_commercial_industry_segment_id = cis.dw_commercial_industry_segment_id
-- 40 largest industry segments only
INNER JOIN top_industry_segments tis
    ON cis.commercial_industry_segment_code = tis.commercial_industry_segment_code
-- Join relevant range
LEFT JOIN {table_name} ir ON 1=1
    AND i.item_number = ir.item_number
    AND i.country_code = ir.country_code
WHERE 1=1
    AND stl.sales_reporting_include_ind = TRUE
    AND stl.country_code = 'AU'
    AND stl.transaction_date BETWEEN {start_date} AND {end_date}
    AND stl.customer_type_code = 'Commercial'
GROUP BY
    i.item_class_name,
    cis.commercial_industry_segment_code
ORDER BY
    i.item_class_name,
    cis.commercial_industry_segment_code
;
