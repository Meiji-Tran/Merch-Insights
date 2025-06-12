WITH item_txns AS (
    SELECT
        stl.dw_sales_transaction_id AS transaction_id,
        i.item_number AS primary_item,
        stl.transaction_date,
        DATE_TRUNC('WEEK', stl.transaction_date) AS week_start
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    WHERE 1=1
        AND stl.country_code = {country}
        AND stl.transaction_date BETWEEN {start_date} AND {end_date}
        AND i.item_number IN ({item_numbers})
        AND stl.customer_type_code = 'Consumer'
        AND stl.sales_reporting_include_ind = TRUE
        {loyalty_filter}
),
copurchases AS (
    SELECT
        it.primary_item,
        i2.item_number AS copurchased_item,
        COUNT(DISTINCT it.transaction_id) AS txn_with_copurchase
    FROM item_txns it
    JOIN bdwprd_cds.sales.sales_transaction_line_fct stl2
        ON stl2.dw_sales_transaction_id = it.transaction_id
    JOIN bdwprd_cds.item.item_dim i2
        ON stl2.dw_item_id = i2.dw_item_id
    WHERE i2.item_number != it.primary_item
    GROUP BY 1, 2
),
primary_txn_counts AS (
    SELECT
        primary_item,
        COUNT(DISTINCT transaction_id) AS total_txns
    FROM item_txns
    GROUP BY 1
)
SELECT
    cp.primary_item,
    cp.copurchased_item,
    i2.item_description,
    i2.item_sub_class_name,
    i2.item_class_name,
    i2.item_sub_department_name,
    i2.item_department_name,
    cp.txn_with_copurchase,
    ptc.total_txns,
    ROUND(cp.txn_with_copurchase * 100.0 / ptc.total_txns, 1) AS percent_of_txns
FROM copurchases cp
JOIN primary_txn_counts ptc ON cp.primary_item = ptc.primary_item
JOIN bdwprd_cds.item.item_dim i2 ON cp.copurchased_item = i2.item_number
QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.primary_item ORDER BY percent_of_txns DESC) <= 10
ORDER BY cp.primary_item, percent_of_txns DESC;
