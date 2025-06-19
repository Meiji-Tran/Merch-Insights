-- Commercial Segment Indexing by {level} and Industry Segment

WITH comm_seg AS (
    SELECT
        ca.dw_commercial_account_id,
        cisrg.commercial_industry_segment_report_group_code AS industry_segment
    FROM bdwprd_cds.commercial.commercial_account_fct ca
    JOIN bdwprd_cds.commercial.commercial_industry_segment_report_group_dim cisrg
        ON ca.dw_commercial_industry_segment_report_group_id = cisrg.dw_commercial_industry_segment_report_group_id
),

-- Step 1: All transactions for items in range
range_trx AS (
    SELECT
        i.{level},
        cs.industry_segment,
        stl.dw_commercial_account_id AS customer_id,
        stl.total_exclude_gst_amount AS sales,
        stl.item_quantity AS quantity
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    JOIN bdwprd_de.ia_merch_de.{table_name} ir
        ON stl.dw_item_id = ir.dw_item_id
        AND stl.country_code = ir.country_code
    JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    JOIN comm_seg cs
        ON stl.dw_commercial_account_id = cs.dw_commercial_account_id
    WHERE 1=1
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.country_code = '{country}'
        AND stl.transaction_date BETWEEN {start_date} AND {end_date}
        AND stl.customer_type_code = 'Commercial'
),

-- Totals per {level} × industry segment
seg_level AS (
    SELECT
        {level},
        industry_segment,
        SUM(sales) AS segment_sales,
        SUM(quantity) AS segment_quantity,
        COUNT(DISTINCT customer_id) AS segment_customers
    FROM range_trx
    GROUP BY {level}, industry_segment
),

-- Totals per {level}
total_level AS (
    SELECT
        {level},
        SUM(sales) AS total_sales,
        SUM(quantity) AS total_quantity,
        COUNT(DISTINCT customer_id) AS total_customers
    FROM range_trx
    GROUP BY {level}
),

-- Totals per industry segment
seg_total AS (
    SELECT
        industry_segment,
        SUM(sales) AS total_seg_sales,
        SUM(quantity) AS total_seg_quantity,
        COUNT(DISTINCT customer_id) AS total_seg_customers
    FROM range_trx
    GROUP BY industry_segment
),

-- Overall totals
overall AS (
    SELECT
        SUM(sales) AS overall_sales,
        SUM(quantity) AS overall_quantity,
        COUNT(DISTINCT customer_id) AS overall_customers
    FROM range_trx
)

-- Final output with indexes
SELECT
    sl.{level},
    sl.industry_segment,

    sl.segment_sales,
    sl.segment_quantity,
    sl.segment_customers,

    tl.total_sales,
    tl.total_quantity,
    tl.total_customers,

    st.total_seg_sales,
    st.total_seg_quantity,
    st.total_seg_customers,

    ovr.overall_sales,
    ovr.overall_quantity,
    ovr.overall_customers,

    -- Shares in {level}
    sl.segment_sales / NULLIF(tl.total_sales, 0) AS segment_sales_share,
    sl.segment_quantity / NULLIF(tl.total_quantity, 0) AS segment_quantity_share,
    sl.segment_customers / NULLIF(tl.total_customers, 0) AS segment_customer_share,

    -- Shares overall
    st.total_seg_sales / NULLIF(ovr.overall_sales, 0) AS overall_sales_share,
    st.total_seg_quantity / NULLIF(ovr.overall_quantity, 0) AS overall_quantity_share,
    st.total_seg_customers / NULLIF(ovr.overall_customers, 0) AS overall_customer_share,

    -- Indexes
    (sl.segment_sales / NULLIF(tl.total_sales, 0))
        / NULLIF(st.total_seg_sales / NULLIF(ovr.overall_sales, 0), 0) AS sales_index,

    (sl.segment_quantity / NULLIF(tl.total_quantity, 0))
        / NULLIF(st.total_seg_quantity / NULLIF(ovr.overall_quantity, 0), 0) AS quantity_index,

    (sl.segment_customers / NULLIF(tl.total_customers, 0))
        / NULLIF(st.total_seg_customers / NULLIF(ovr.overall_customers, 0), 0) AS customer_index

FROM seg_level sl
JOIN total_level tl
    ON sl.{level} = tl.{level}
JOIN seg_total st
    ON sl.industry_segment = st.industry_segment
JOIN overall ovr
    ON 1=1
ORDER BY
    sl.{level},
    sl.industry_segment;