SELECT 
    TO_CHAR(stl.transaction_date, 'YYYY') AS sales_year,
    i.item_number,
    i.item_description,
    i.item_class_name,
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
    AND stl.transaction_date BETWEEN '2020-06-18' AND '2025-06-18'
    AND stl.customer_type_code = 'Commercial'
    AND stl.dw_commercial_account_id != MD5_BINARY(-1)
    AND s.supplier_code = '4913000'
GROUP BY 
    TO_CHAR(stl.transaction_date, 'YYYY'),
    i.item_number,
    i.item_description,
    i.item_class_name
ORDER BY 
    sales_year, sales_sum DESC;