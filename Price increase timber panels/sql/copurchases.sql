-- copurchases.sql

WITH product_group_txn AS (
    SELECT
        stl.dw_sales_transaction_id AS transaction_id,
        DATE_TRUNC('WEEK', stl.transaction_date) AS week_start
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    WHERE 1=1
        AND stl.transaction_date BETWEEN TO_DATE({start_rrp_date}) - 10 * 7 AND TO_DATE({start_rrp_date}) + 10 * 7
        AND stl.item_number IN {product_group_items}
        AND stl.sales_reporting_include_ind = TRUE
        {loyalty_filter}
        AND stl.customer_type_code = 'Consumer'
        AND stl.country_code = {country}
    GROUP BY 1, 2
),

copurchase_candidates AS (
    SELECT
        stl.dw_sales_transaction_id AS transaction_id,
        DATE_TRUNC('WEEK', stl.transaction_date) AS week_start,
        i.item_number,
        i.item_description,
        i.item_department_name
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    WHERE 1=1
        AND stl.transaction_date BETWEEN TO_DATE({start_rrp_date}) - 10 * 7 AND TO_DATE({start_rrp_date}) + 10 * 7
        AND stl.sales_reporting_include_ind = TRUE
        {loyalty_filter}
        AND stl.customer_type_code = 'Consumer'
        AND stl.country_code = {country}
        AND i.item_number NOT IN {product_group_items}
        AND i.item_department_name = '300 INDOOR TIMBER AND BOARDS'
),

joined_txns AS (
    SELECT
        pg.week_start,
        c.item_number,
        c.item_description,
        COUNT(*) AS co_purchase_count
    FROM product_group_txn pg
    JOIN copurchase_candidates c
        ON pg.transaction_id = c.transaction_id
        AND pg.week_start = c.week_start
    GROUP BY pg.week_start, c.item_number, c.item_description
),

weekly_txn_totals AS (
    SELECT
        week_start,
        COUNT(DISTINCT transaction_id) AS txn_count
    FROM product_group_txn
    GROUP BY week_start
)

SELECT
    j.week_start,
    j.item_number,
    j.item_description,
    j.co_purchase_count,
    ROUND(j.co_purchase_count * 1.0 / t.txn_count, 4) AS co_purchase_rate
FROM joined_txns j
JOIN weekly_txn_totals t
    ON j.week_start = t.week_start
QUALIFY ROW_NUMBER() OVER (PARTITION BY j.week_start ORDER BY j.co_purchase_count DESC) <= 10
ORDER BY j.week_start, co_purchase_count DESC;
