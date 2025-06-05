-- Demographic Segment Profiling
-- Get most recent demographic segment classification
WITH dem_seg AS (
    SELECT
        entity AS flybuys_membership_number_hash,
        value AS demographic_segment
    FROM bdwprd_apps.customer_attributes.flybuys_household_primary_segment
    WHERE value NOT ILIKE 'unclassifiable%'
    -- Multiple model runs in table, get most recent result
    QUALIFY ROW_NUMBER() OVER (PARTITION BY entity ORDER BY timestamp DESC) = 1
)
SELECT
    ds.demographic_segment,
    -- Item range metrics
    SUM(IFF(ir.item_number IS NOT NULL, stl.total_exclude_gst_amount, NULL)) AS range_sales,
    SUM(IFF(ir.item_number IS NOT NULL, stl.item_quantity, NULL)) AS range_quantity,
    COUNT(DISTINCT IFF(ir.item_number IS NOT NULL, stl.dw_loyalty_flybuys_account_id, NULL)) AS range_customers,
    -- Item range shares
    range_sales / SUM(range_sales) OVER () AS range_sales_share,
    range_quantity / SUM(range_quantity) OVER () AS range_quantity_share,
    range_customers / SUM(range_customers) OVER () AS range_customer_share,
    -- Overall metrics
    SUM(stl.total_exclude_gst_amount) AS overall_sales,
    SUM(stl.item_quantity) AS overall_quantity,
    COUNT(DISTINCT stl.dw_loyalty_flybuys_account_id) AS overall_customers,
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
INNER JOIN bdwprd_cds.loyalty_master.loyalty_flybuys_account_dim fa
    ON stl.dw_loyalty_flybuys_account_id = fa.dw_loyalty_flybuys_account_id
INNER JOIN dem_seg ds
    ON fa.flybuys_membership_number_hash = ds.flybuys_membership_number_hash
-- Join relevant range
LEFT JOIN {table_name} ir ON 1=1
    AND i.item_number = ir.item_number
    AND i.country_code = ir.country_code
WHERE 1=1
    AND ds.demographic_segment != 'Unknown' -- Exclude unknown segments
    AND stl.sales_reporting_include_ind = TRUE
    AND stl.country_code = 'AU'
    AND stl.transaction_date BETWEEN {start_date} AND {end_date}
    AND stl.customer_type_code = 'Consumer'
GROUP BY
    ds.demographic_segment
ORDER BY
    ds.demographic_segment
;