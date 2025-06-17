-- Cross shop
-- Groups Flybuys transactions by cardholder on the same day
-- Can replace token with commercial account ID for commercial cross shop
-- Just remember to change the Flbyuys ID for the commercial account ID in the where clauses of CTEs a and b 
-- Example is for cleaning and accessories with outdoor timber classes
-- Can be done at any IFT level

-- A transactions
WITH a AS (
    SELECT
        stl.dw_loyalty_flybuys_account_id AS token_a,
        stl.transaction_date AS transaction_date_a,
        i.item_category_name AS category_a,
        i.item_department_name AS department_a,
        i.item_sub_department_name AS sub_department_a,
        i.item_class_name AS class_a,
        stl.item_quantity AS quantity_a,
        stl.total_include_gst_amount AS sales_a
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    INNER JOIN bdwprd_cds.item.item_dim i
        ON i.dw_item_id = stl.dw_item_id
    WHERE 1=1
        AND stl.country_code = 'AU'
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.transaction_date BETWEEN {start_date} AND {end_date}
        AND stl.dw_loyalty_flybuys_account_id != MD5_BINARY(-1)
-- B transactions
), b AS (
    SELECT
        stl.dw_loyalty_flybuys_account_id AS token_b,
        stl.transaction_date AS transaction_date_b,
        i.item_category_name AS category_b,
        i.item_department_name AS department_b,
        i.item_sub_department_name AS sub_department_b,
        i.item_class_name AS class_b,
        stl.item_quantity AS quantity_b,
        stl.total_include_gst_amount AS sales_b
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    INNER JOIN bdwprd_cds.item.item_dim i
        ON i.dw_item_id = stl.dw_item_id
    WHERE 1=1
        AND stl.country_code = 'AU'
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.transaction_date BETWEEN {start_date} AND {end_date}
        AND stl.dw_loyalty_flybuys_account_id != MD5_BINARY(-1)
-- Aggregate stats on A transactions
), a_agg AS (
    SELECT
        category_a,
        department_a,
        sub_department_a,
        class_a,
        SUM(quantity_a) AS total_quantity_a,
        SUM(sales_a) AS total_sales_a,
        COUNT(DISTINCT token_a, transaction_date_a) AS total_trx_a,
        COUNT(DISTINCT token_a) AS total_customers_a
    FROM a
    GROUP BY
        category_a,
        department_a,
        sub_department_a,
        class_a
-- Aggregate stats on B transactions
), b_agg AS (
    SELECT
        category_b,
        department_b,
        sub_department_b,
        class_b,
        SUM(quantity_b) AS total_quantity_b,
        SUM(sales_b) AS total_sales_b,
        COUNT(DISTINCT token_b, transaction_date_b) AS total_trx_b,
        COUNT(DISTINCT token_b) AS total_customers_b
    FROM b
    GROUP BY
        category_b,
        department_b,
        sub_department_b,
        class_b
-- Paired transactions
), ab AS (
    SELECT
        *
    FROM a
    INNER JOIN b ON 1=1
        AND b.token_b = a.token_a
        AND b.transaction_date_b = a.transaction_date_a
        AND b.class_b != a.class_a
    WHERE 1=1
-- Aggregate stats on paired transactions
), ab_agg AS (
    SELECT
        category_a,
        department_a,
        sub_department_a,
        class_a,
        category_b,
        department_b,
        sub_department_b,
        class_b,

        SUM(sales_a) + SUM(sales_b) AS total_sales_ab,
        SUM(sales_a) AS sales_a_ab,
        SUM(sales_b) AS sales_b_ab,
        DIV0(SUM(sales_a), total_sales_ab) AS a_sales_pct,
        DIV0(SUM(sales_b), total_sales_ab) AS b_sales_pct,

        SUM(quantity_a) + SUM(quantity_b) AS total_quantity_ab,
        SUM(quantity_a) AS quantity_a_ab,
        SUM(quantity_b) AS quantity_b_ab,
        DIV0(SUM(quantity_a), total_quantity_ab) AS a_quantity_pct,
        DIV0(SUM(quantity_b), total_quantity_ab) AS b_quantity_pct,

        COUNT(DISTINCT token_a, transaction_date_a, token_b, transaction_date_b) AS total_trx_ab,
        COUNT(DISTINCT token_a, token_b) AS total_customers_ab
    FROM ab
    GROUP BY
        category_a,
        department_a,
        sub_department_a,
        class_a,
        category_b,
        department_b,
        sub_department_b,
        class_b
-- Calculate the total number of transactions
-- ), all_events AS (
--     SELECT
--         COUNT(DISTINCT stl.dw_loyalty_flybuys_account_id, stl.transaction_date) AS total_events
--     FROM bdwprd_cds.sales.sales_transaction_line_fct stl
--     WHERE 1=1
--         AND stl.country_code = 'AU'
--         AND stl.sales_reporting_include_ind = TRUE
--         AND stl.transaction_date BETWEEN {start_date} AND {end_date}
--         AND stl.dw_loyalty_flybuys_account_id != MD5_BINARY(-1)
)
-- Cross-shop results
SELECT
    -- General results
    ab_agg.category_a,
    ab_agg.department_a,
    ab_agg.sub_department_a,
    ab_agg.class_a,
    ab_agg.category_b,
    ab_agg.department_b,
    ab_agg.sub_department_b,
    ab_agg.class_b,
    a_agg.total_trx_a AS single_count_a,
    b_agg.total_trx_b AS single_count_b,
    ab_agg.total_trx_ab AS pair_count,
    -- ae.total_events,

    -- -- Product association metrics
    -- DIV0(single_count_a, ae.total_events) AS support_a,
    -- DIV0(single_count_b, ae.total_events) AS support_b,
    -- DIV0(pair_count, ae.total_events) AS pair_support,

    -- DIV0(pair_count, (single_count_a + single_count_b - pair_count)) AS jaccard,

    -- DIV0(pair_support, support_a) AS confidence_a,
    -- DIV0(pair_support, support_b) AS confidence_b,

    -- DIV0(pair_support, (support_a * support_b)) AS lift_a,
    -- DIV0(pair_support, (support_a * support_b)) AS lift_b,

    -- pair_support - (support_a * support_b) AS leverage,

    -- DIV0((1 - support_a), (1 - confidence_a)) AS conviction_a,
    -- DIV0((1 - support_b), (1 - confidence_b)) AS conviction_b,

    -- -- General basket cross-shop metrics
    -- DIV0(ab_agg.total_trx_ab, a_agg.total_trx_a) AS apen_trx,
    -- DIV0(ab_agg.total_trx_ab, b_agg.total_trx_b) AS bpen_trx,
    -- a_agg.total_customers_a,
    -- b_agg.total_customers_b,
    -- ab_agg.total_customers_ab,

    -- -- Sales metrics
    a_agg.total_sales_a
    -- b_agg.total_sales_b,
    -- ab_agg.total_sales_ab,
    -- ab_agg.sales_a_ab,
    -- ab_agg.sales_b_ab,
    -- DIV0(ab_agg.sales_b_ab, a_agg.total_sales_a) AS sales_b_for_a,
    -- DIV0(ab_agg.sales_a_ab, b_agg.total_sales_b) AS sales_a_for_b,
    -- ab_agg.a_sales_pct,
    -- ab_agg.b_sales_pct,

    -- -- Quantity metrics
    -- a_agg.total_quantity_a,
    -- b_agg.total_quantity_b,
    -- ab_agg.total_quantity_ab,
    -- ab_agg.quantity_a_ab,
    -- ab_agg.quantity_b_ab,
    -- DIV0(ab_agg.quantity_b_ab, a_agg.total_quantity_a) AS quantity_b_for_a,
    -- DIV0(ab_agg.quantity_a_ab, b_agg.total_quantity_b) AS quantity_a_for_b,
    -- ab_agg.a_quantity_pct,
    -- ab_agg.b_quantity_pct
FROM ab_agg
LEFT JOIN a_agg
    ON a_agg.class_a = ab_agg.class_a
LEFT JOIN b_agg
    ON b_agg.class_b = ab_agg.class_b
-- INNER JOIN all_events ae
--     ON 1=1
WHERE 1=1
-- Only include results if transaction thresholds are hit
    AND a_agg.total_trx_a >= 10
    AND b_agg.total_trx_b >= 10
    AND ab_agg.total_trx_ab >= 5
    AND ab_agg.class_a < ab_agg.class_b
ORDER BY
    ab_agg.class_a,
    ab_agg.class_b
;
