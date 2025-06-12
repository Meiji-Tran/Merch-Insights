WITH base_transactions AS (
    SELECT DISTINCT
        stl.dw_sales_transaction_id AS txn_id,
        DATE_TRUNC('WEEK', stl.transaction_date) AS week_start
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    WHERE 1=1
        AND stl.transaction_date BETWEEN {start_date} AND {end_date}
        AND i.item_number IN {product_group_items}
        AND i.item_department_name = '300 INDOOR TIMBER AND BOARDS'
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.customer_type_code = 'Consumer'
        AND stl.country_code = {country}
        {loyalty_filter}
),

copurchases AS (
    SELECT
        bt.week_start,
        stl.dw_sales_transaction_id AS txn_id,
        i.item_number,
        i.item_description
    FROM base_transactions bt
    JOIN bdwprd_cds.sales.sales_transaction_line_fct stl
        ON bt.txn_id = stl.dw_sales_transaction_id
    JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    WHERE 1=1
        AND i.item_number NOT IN {product_group_items}
        AND i.item_department_name = '300 INDOOR TIMBER AND BOARDS'
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.customer_type_code = 'Consumer'
        AND stl.country_code = {country}
        {loyalty_filter}
),

txn_total AS (
    SELECT
        week_start,
        COUNT(DISTINCT txn_id) AS txn_count
    FROM base_transactions
    GROUP BY week_start
)

SELECT
    copurchases.week_start,
    copurchases.item_number,
    copurchases.item_description,
    COUNT(DISTINCT copurchases.txn_id) AS co_purchase_count,
    txn_total.txn_count,
    ROUND(COUNT(DISTINCT copurchases.txn_id) * 100.0 / txn_total.txn_count, 2) AS co_purchase_rate
FROM copurchases
JOIN txn_total
    ON copurchases.week_start = txn_total.week_start
GROUP BY
    copurchases.week_start, copurchases.item_number, copurchases.item_description, txn_total.txn_count
QUALIFY ROW_NUMBER() OVER (PARTITION BY copurchases.week_start ORDER BY co_purchase_count DESC) <= 10
ORDER BY
    copurchases.week_start, co_purchase_count DESC
