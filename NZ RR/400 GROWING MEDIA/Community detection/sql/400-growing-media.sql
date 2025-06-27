SELECT
    COUNT(DISTINCT stl.dw_sales_transaction_id) AS total_transactions,
    ROUND(SUM(stl.total_exclude_gst_amount))     AS total_sales,
    ROUND(SUM(stl.sales_quantity))               AS total_quantity
FROM bdwprd_cds.sales.sales_transaction_line_fct AS stl
INNER JOIN bdwprd_cds.item.item_dim AS i
    ON stl.dw_item_id = i.dw_item_id
WHERE
    stl.country_code                   = 'NZ'
    AND stl.sales_reporting_include_ind = TRUE
    AND stl.transaction_date BETWEEN '2024-06-01' AND '2025-05-31'
    AND i.item_sub_department_name                = '400 GROWING MEDIA';
