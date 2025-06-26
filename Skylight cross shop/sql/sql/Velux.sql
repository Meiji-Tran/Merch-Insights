SELECT
    COUNT(DISTINCT stl.dw_sales_transaction_id) AS total_transactions,
    ROUND(SUM(stl.total_exclude_gst_amount))     AS total_sales,
    ROUND(SUM(stl.sales_quantity))               AS total_quantity
FROM bdwprd_cds.sales.sales_transaction_line_fct AS stl
INNER JOIN bdwprd_cds.item.item_dim AS i
    ON stl.dw_item_id = i.dw_item_id
INNER JOIN bdwprd_cds.merchandising.item_supplier_cost_price_fct AS isc
    ON i.dw_item_id = isc.dw_item_id
INNER JOIN bdwprd_cds.supplier.supplier_dim AS s
    ON isc.dw_supplier_id = s.dw_supplier_id
WHERE
    stl.country_code                   = 'NZ'
    AND stl.sales_reporting_include_ind = TRUE
    AND stl.transaction_date BETWEEN '2020-06-18' AND '2025-06-18'
    AND stl.customer_type_code         = 'Commercial'
    AND stl.dw_commercial_account_id  != MD5_BINARY(-1)
    AND s.supplier_code                = '4913000';
