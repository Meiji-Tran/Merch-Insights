WITH item_txns AS (
    SELECT
        stl.dw_sales_transaction_id AS transaction_id,
        i.item_number AS primary_item_number,
        i.item_class_name AS primary_item_class_name,
        CASE 
            WHEN stl.transaction_date < {rrp_cutoff} THEN 'Pre'
            ELSE 'Post'
        END AS period,
        stl.transaction_date
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    WHERE 1=1
        AND stl.transaction_date BETWEEN {start_date} AND {end_date}
        AND i.item_class_name IN ('500 PANELS', '500 PANELS BULK STACK')
        AND i.item_department_name != '.Unk'
        AND stl.sales_reporting_include_ind = TRUE
        {loyalty_filter}
        AND stl.customer_type_code = 'Consumer'
        AND stl.country_code = {country}
),

copurchase_items AS (
    SELECT
        it.primary_item_number,
        it.primary_item_class_name,
        it.period,
        cp.item_number AS copurchased_item_number,
        COUNT(DISTINCT it.transaction_id) AS primary_item_txn_count,
        COUNT(DISTINCT CASE WHEN cp.item_number != it.primary_item_number THEN it.transaction_id END) AS copurchase_txn_count
    FROM item_txns it
    JOIN bdwprd_cds.sales.sales_transaction_line_fct cp
        ON it.transaction_id = cp.dw_sales_transaction_id
    JOIN bdwprd_cds.item.item_dim cp_i
        ON cp.dw_item_id = cp_i.dw_item_id
    WHERE cp.item_number != it.primary_item_number
    GROUP BY
        it.primary_item_number,
        it.primary_item_class_name,
        it.period,
        cp.item_number
),

copurchase_details AS (
    SELECT
        ci.*,
        i.item_description,
        i.item_class_name,
        i.item_sub_class_name,
        i.item_sub_department_name,
        i.item_department_name,
        ROUND(copurchase_txn_count * 100.0 / NULLIF(primary_item_txn_count, 0), 2) AS percent_of_primary_txns
    FROM copurchase_items ci
    LEFT JOIN bdwprd_cds.item.item_dim i
        ON ci.copurchased_item_number = i.item_number
)

SELECT
    '{country}' AS country,
    *
FROM copurchase_details
ORDER BY primary_item_number, period, percent_of_primary_txns DESC
