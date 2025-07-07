-- Industry Segment Profiling (Sub-dept Range Indexing)

WITH top_industry_segments AS (
    SELECT
        cis.commercial_industry_segment_code,
        COUNT(*) AS segment_size
    FROM bdwprd_cds.commercial.commercial_account_fct ca
    JOIN bdwprd_cds.commercial.commercial_industry_segment_dim cis
        ON ca.dw_commercial_industry_segment_id = cis.dw_commercial_industry_segment_id
    GROUP BY cis.commercial_industry_segment_code
    ORDER BY segment_size DESC
),

ind_seg AS (
    SELECT
        ca.dw_commercial_account_id,
        cis.commercial_industry_segment_code AS industry_segment
    FROM bdwprd_cds.commercial.commercial_account_fct ca
    JOIN bdwprd_cds.commercial.commercial_industry_segment_dim cis
        ON ca.dw_commercial_industry_segment_id = cis.dw_commercial_industry_segment_id
    JOIN top_industry_segments tis
        ON cis.commercial_industry_segment_code = tis.commercial_industry_segment_code
),

industry_profile AS (
    SELECT
        iseg.industry_segment,

        -- Item range metrics
        SUM(IFF(({filter}), stl.total_exclude_gst_amount, NULL)) AS range_sales,
        SUM(IFF(({filter}), stl.item_quantity, NULL)) AS range_quantity,
        COUNT(DISTINCT IFF(({filter}), stl.dw_commercial_account_id, NULL)) AS range_customers,

        -- Range shares
        range_sales / SUM(range_sales) OVER () AS range_sales_share,
        range_quantity / SUM(range_quantity) OVER () AS range_quantity_share,
        range_customers / SUM(range_customers) OVER () AS range_customer_share,

        -- Overall metrics
        SUM(stl.total_exclude_gst_amount) AS overall_sales,
        SUM(stl.item_quantity) AS overall_quantity,
        COUNT(DISTINCT stl.dw_commercial_account_id) AS overall_customers,

        -- Overall shares
        overall_sales / SUM(overall_sales) OVER () AS overall_sales_share,
        overall_quantity / SUM(overall_quantity) OVER () AS overall_quantity_share,
        overall_customers / SUM(overall_customers) OVER () AS overall_customer_share,

        -- Indexes
        ROUND(range_sales_share / overall_sales_share, 2) AS sales_index,
        ROUND(range_quantity_share / overall_quantity_share, 2) AS quantity_index,
        ROUND(range_customer_share / overall_customer_share, 2) AS customer_index

    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    JOIN ind_seg iseg
        ON stl.dw_commercial_account_id = iseg.dw_commercial_account_id

    WHERE 1=1
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.country_code = 'NZ'
        AND stl.customer_type_code = 'Commercial'
        AND stl.transaction_date BETWEEN {start_date} AND {end_date}

    GROUP BY
        iseg.industry_segment
)

SELECT 
    ip.*,
    tis.segment_size,
    tis.segment_size / SUM(tis.segment_size) OVER () AS segment_size_share
FROM industry_profile ip
JOIN top_industry_segments tis
    ON ip.industry_segment = tis.commercial_industry_segment_code
WHERE ip.range_sales IS NOT NULL
ORDER BY ip.range_sales DESC;
