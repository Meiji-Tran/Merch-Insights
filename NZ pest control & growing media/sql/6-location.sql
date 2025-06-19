-- Location
SELECT
    i.item_class_name,
    l.location_name,
    -- Item range metrics
    SUM(IFF(ir.item_number IS NOT NULL, stl.total_exclude_gst_amount, NULL)) AS range_sales,
    SUM(IFF(ir.item_number IS NOT NULL, stl.item_quantity, NULL)) AS range_quantity,
    -- Item range shares
    range_sales / SUM(range_sales) OVER () AS range_sales_share,
    range_quantity / SUM(range_quantity) OVER () AS range_quantity_share,
    -- Overall metrics
    SUM(stl.total_exclude_gst_amount) AS overall_sales,
    SUM(stl.item_quantity) AS overall_quantity,
    -- Overall shares
    overall_sales / SUM(overall_sales) OVER () AS overall_sales_share,
    overall_quantity / SUM(overall_quantity) OVER () AS overall_quantity_share,
    -- Indexes
    range_sales_share / overall_sales_share AS sales_index,
    range_quantity_share / overall_quantity_share AS quantity_index
FROM bdwprd_cds.sales.sales_transaction_line_fct stl
INNER JOIN bdwprd_cds.item.item_dim i
    ON stl.dw_item_id = i.dw_item_id
INNER JOIN bdwprd_cds.location.location_dim l
    ON stl.dw_location_id = l.dw_location_id
-- Join relevant range
LEFT JOIN {table_name} ir ON 1=1
    AND i.item_number = ir.item_number
    AND i.country_code = ir.country_code
WHERE 1=1
    AND stl.sales_reporting_include_ind = TRUE
    AND stl.country_code = 'AU'
    AND stl.transaction_date BETWEEN {start_date} AND {end_date}
GROUP BY
    i.item_class_name,
    l.location_name
ORDER BY
    i.item_class_name,
    l.location_name
;
