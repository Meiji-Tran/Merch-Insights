SELECT
    DATE_TRUNC('WEEK', stl.TRANSACTION_DATE) AS week_start,
    i.item_number,
    i.item_description,
    i.item_class_name,
    i.item_sub_class_name,
    
    SUM(stl.TOTAL_EXCLUDE_GST_AMOUNT) AS total_sales,
    SUM(stl.SALES_QUANTITY) AS total_quantity,
    COUNT(DISTINCT stl.dw_loyalty_flybuys_account_id) AS unique_customers

FROM bdwprd_cds.sales.sales_transaction_line_fct stl
JOIN bdwprd_cds.item.item_dim i
    ON stl.dw_item_id = i.dw_item_id

WHERE 1=1
    AND stl.TRANSACTION_DATE BETWEEN {start_date} AND {end_date}
    AND i.item_class_name IN ('500 PANELS', '500 PANELS BULK STACK')
    AND i.item_department_name != '.Unk'
    AND stl.sales_reporting_include_ind = TRUE 
    {loyalty_filter}
    AND stl.customer_type_code = 'Consumer'
    AND stl.country_code = {country}

GROUP BY
    1, 2, 3, 4, 5
ORDER BY
    week_start, item_number;