-- Demographic Indexing by {level} and Trade Region
WITH

-- 1. All transactions for items in range
range_trx AS (
    SELECT
        i.{level},
        l.trade_region_code,
        stl.dw_loyalty_flybuys_account_id AS customer_id,
        stl.total_exclude_gst_amount AS sales,
        stl.item_quantity AS quantity
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    JOIN bdwprd_de.ia_merch_de.{table_name} ir
        ON stl.dw_item_id = ir.dw_item_id
        AND stl.country_code = ir.country_code
    JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    JOIN bdwprd_cds.location.location_dim l
        ON stl.dw_location_id = l.dw_location_id
    WHERE 1=1
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.country_code = '{country}'
        AND stl.transaction_date BETWEEN {start_date} AND {end_date}
        AND stl.customer_type_code = 'Consumer'
),

-- 2. Totals per {level} × region
group_level AS (
    SELECT
        {level},
        trade_region_code,
        SUM(sales) AS group_sales,
        SUM(quantity) AS group_quantity,
        COUNT(DISTINCT customer_id) AS group_customers
    FROM range_trx
    GROUP BY {level}, trade_region_code
),

-- 3. Totals per {level}
total_level AS (
    SELECT
        {level},
        SUM(sales) AS total_sales,
        SUM(quantity) AS total_quantity,
        COUNT(DISTINCT customer_id) AS total_customers
    FROM range_trx
    GROUP BY {level}
),

-- 4. Totals per region (across all {level})
region_total AS (
    SELECT
        trade_region_code,
        SUM(sales) AS total_region_sales,
        SUM(quantity) AS total_region_quantity,
        COUNT(DISTINCT customer_id) AS total_region_customers
    FROM range_trx
    GROUP BY trade_region_code
),

-- 5. Overall totals
overall AS (
    SELECT
        SUM(sales) AS overall_sales,
        SUM(quantity) AS overall_quantity,
        COUNT(DISTINCT customer_id) AS overall_customers
    FROM range_trx
)

-- 6. Final output
SELECT
    gl.{level},
    gl.trade_region_code,

    gl.group_sales,
    gl.group_quantity,
    gl.group_customers,

    tl.total_sales,
    tl.total_quantity,
    tl.total_customers,

    rt.total_region_sales,
    rt.total_region_quantity,
    rt.total_region_customers,

    ovr.overall_sales,
    ovr.overall_quantity,
    ovr.overall_customers,

    -- Share in {level}
    gl.group_sales / NULLIF(tl.total_sales, 0) AS group_sales_share,
    gl.group_quantity / NULLIF(tl.total_quantity, 0) AS group_quantity_share,
    gl.group_customers / NULLIF(tl.total_customers, 0) AS group_customer_share,

    -- Share in overall
    rt.total_region_sales / NULLIF(ovr.overall_sales, 0) AS overall_sales_share,
    rt.total_region_quantity / NULLIF(ovr.overall_quantity, 0) AS overall_quantity_share,
    rt.total_region_customers / NULLIF(ovr.overall_customers, 0) AS overall_customer_share,

    -- Indexes
    (gl.group_sales / NULLIF(tl.total_sales, 0))
        / NULLIF(rt.total_region_sales / NULLIF(ovr.overall_sales, 0), 0) AS sales_index,

    (gl.group_quantity / NULLIF(tl.total_quantity, 0))
        / NULLIF(rt.total_region_quantity / NULLIF(ovr.overall_quantity, 0), 0) AS quantity_index,

    (gl.group_customers / NULLIF(tl.total_customers, 0))
        / NULLIF(rt.total_region_customers / NULLIF(ovr.overall_customers, 0), 0) AS customer_index

FROM group_level gl
JOIN total_level tl
    ON gl.{level} = tl.{level}
JOIN region_total rt
    ON gl.trade_region_code = rt.trade_region_code
JOIN overall ovr
    ON 1=1
ORDER BY
    gl.{level},
    gl.trade_region_code;
