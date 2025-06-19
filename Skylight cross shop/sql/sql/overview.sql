SELECT 
    CASE WHEN GROUPING(i.item_number) = 1 THEN 'TOTAL' ELSE i.item_number END AS item_number,
    CASE WHEN GROUPING(i.item_description) = 1 THEN 'TOTAL' ELSE i.item_description END AS item_description,
    CASE WHEN GROUPING(i.item_class_name) = 1 THEN 'TOTAL' ELSE i.item_class_name END AS item_class_name,
    COUNT(DISTINCT stl.dw_sales_transaction_id) AS transactions,
    ROUND(SUM(stl.total_exclude_gst_amount)) AS sales_sum,
    ROUND(SUM(stl.sales_quantity)) AS quantity_sum
FROM bdwprd_cds.sales.sales_transaction_line_fct stl
INNER JOIN bdwprd_cds.item.item_dim i
    ON stl.dw_item_id = i.dw_item_id
INNER JOIN bdwprd_cds.merchandising.item_supplier_cost_price_fct isc
    ON i.dw_item_id = isc.dw_item_id
INNER JOIN bdwprd_cds.supplier.supplier_dim s
    ON s.dw_supplier_id = isc.dw_supplier_id
WHERE 
    stl.country_code = 'NZ'
    AND stl.sales_reporting_include_ind = TRUE
    AND stl.transaction_date BETWEEN '2023-06-18' AND '2025-06-18'
    AND stl.customer_type_code = 'Commercial'
    AND stl.dw_commercial_account_id != MD5_BINARY(-1)
    AND s.supplier_code = '4913000'
GROUP BY GROUPING SETS (
    (i.item_number, i.item_description, i.item_class_name),
    ()
)
ORDER BY 
    sales_sum DESC;

