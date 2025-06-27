WITH growing_media_transactions AS (
    SELECT 
        stl.dw_sales_transaction_id
    FROM 
        bdwprd_cds.sales.sales_transaction_line_fct AS stl
    INNER JOIN 
        bdwprd_cds.item.item_dim AS i ON stl.dw_item_id = i.dw_item_id
    WHERE
        stl.country_code = 'NZ'
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.transaction_date BETWEEN '2024-06-01' AND '2025-05-31'
        AND i.item_sub_department_name = '400 GROWING MEDIA'
),
greenlife_transactions AS (
    SELECT 
        stl.dw_sales_transaction_id,
        i.item_sub_department_name
    FROM 
        bdwprd_cds.sales.sales_transaction_line_fct AS stl
    INNER JOIN 
        bdwprd_cds.item.item_dim AS i ON stl.dw_item_id = i.dw_item_id
    WHERE
        stl.country_code = 'NZ'
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.transaction_date BETWEEN '2024-06-01' AND '2025-05-31'
        AND i.item_department_name = '300 GREENLIFE'
)
SELECT
    COUNT(DISTINCT gm.dw_sales_transaction_id) AS total_transactions_with_greenlife,
    ROUND(SUM(stl.total_exclude_gst_amount)) AS total_sales,
    ROUND(SUM(stl.sales_quantity)) AS total_quantity,
    gl.item_sub_department_name,
    COUNT(DISTINCT gl.dw_sales_transaction_id) AS total_transactions_in_greenlife
FROM 
    growing_media_transactions gm
JOIN 
    greenlife_transactions gl 
    ON gm.dw_sales_transaction_id = gl.dw_sales_transaction_id
JOIN 
    bdwprd_cds.sales.sales_transaction_line_fct stl
    ON gm.dw_sales_transaction_id = stl.dw_sales_transaction_id
GROUP BY 
    gl.item_sub_department_name;
