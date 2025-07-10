-- Demographic Indexing by {level}, Has_Pets, and Trade Region
WITH

-- 1. Latest pet ownership flag
pet_seg AS (
    SELECT
        entity AS flybuys_membership_number_hash,
        LOWER(value) AS has_pets
    FROM BDWPRD_APPS.CUSTOMER.ATTRIBUTE_PIPELINE_RAW_OUTPUT
    WHERE attribute = 'has_pets'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY entity ORDER BY timestamp DESC) = 1
),

-- 2. Latest demographic info with pet flag (no need to keep segment)
dem_seg AS (
    SELECT
        ds.entity AS flybuys_membership_number_hash,
        COALESCE(pet.has_pets, 'unknown') AS has_pets
    FROM bdwprd_apps.customer_attributes.flybuys_household_primary_segment ds
    LEFT JOIN pet_seg pet
        ON ds.entity = pet.flybuys_membership_number_hash
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ds.entity ORDER BY ds.timestamp DESC) = 1
),

-- 3. All transactions for items in range
range_trx AS (
    SELECT
        i.{level},
        ds.has_pets,
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
    JOIN bdwprd_cds.loyalty_master.loyalty_flybuys_account_dim fa
        ON stl.dw_loyalty_flybuys_account_id = fa.dw_loyalty_flybuys_account_id
    JOIN dem_seg ds
        ON fa.flybuys_membership_number_hash = ds.flybuys_membership_number_hash
    WHERE 1=1
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.country_code = '{country}'
        AND stl.transaction_date BETWEEN {start_date} AND {end_date}
        AND stl.customer_type_code = 'Consumer'
),

-- 4. Totals per {level} × pets × region
group_level AS (
    SELECT
        {level},
        has_pets,
        trade_region_code,
        SUM(sales) AS group_sales,
        SUM(quantity) AS group_quantity,
        COUNT(DISTINCT customer_id) AS group_customers
    FROM range_trx
    GROUP BY {level}, has_pets, trade_region_code
),

-- 5. Totals per {level}
total_level AS (
    SELECT
        {level},
        SUM(sales) AS total_sales,
        SUM(quantity) AS total_quantity,
        COUNT(DISTINCT customer_id) AS total_customers
    FROM range_trx
    GROUP BY {level}
),

-- 6. Totals per (pets × region)
region_pet_total AS (
    SELECT
        has_pets,
        trade_region_code,
        SUM(sales) AS total_rp_sales,
        SUM(quantity) AS total_rp_quantity,
        COUNT(DISTINCT customer_id) AS total_rp_customers
    FROM range_trx
    GROUP BY has_pets, trade_region_code
),

-- 7. Overall totals
overall AS (
    SELECT
        SUM(sales) AS overall_sales,
        SUM(quantity) AS overall_quantity,
        COUNT(DISTINCT customer_id) AS overall_customers
    FROM range_trx
)

-- 8. Final output
SELECT
    gl.{level},
    gl.has_pets,
    gl.trade_region_code,

    gl.group_sales,
    gl.group_quantity,
    gl.group_customers,

    tl.total_sales,
    tl.total_quantity,
    tl.total_customers,

    rpt.total_rp_sales,
    rpt.total_rp_quantity,
    rpt.total_rp_customers,

    ovr.overall_sales,
    ovr.overall_quantity,
    ovr.overall_customers,

    -- Share in {level}
    gl.group_sales / NULLIF(tl.total_sales, 0) AS group_sales_share,
    gl.group_quantity / NULLIF(tl.total_quantity, 0) AS group_quantity_share,
    gl.group_customers / NULLIF(tl.total_customers, 0) AS group_customer_share,

    -- Share in overall
    rpt.total_rp_sales / NULLIF(ovr.overall_sales, 0) AS overall_sales_share,
    rpt.total_rp_quantity / NULLIF(ovr.overall_quantity, 0) AS overall_quantity_share,
    rpt.total_rp_customers / NULLIF(ovr.overall_customers, 0) AS overall_customer_share,

    -- Indexes
    (gl.group_sales / NULLIF(tl.total_sales, 0))
        / NULLIF(rpt.total_rp_sales / NULLIF(ovr.overall_sales, 0), 0) AS sales_index,

    (gl.group_quantity / NULLIF(tl.total_quantity, 0))
        / NULLIF(rpt.total_rp_quantity / NULLIF(ovr.overall_quantity, 0), 0) AS quantity_index,

    (gl.group_customers / NULLIF(tl.total_customers, 0))
        / NULLIF(rpt.total_rp_customers / NULLIF(ovr.overall_customers, 0), 0) AS customer_index

FROM group_level gl
JOIN total_level tl
    ON gl.{level} = tl.{level}
JOIN region_pet_total rpt
    ON gl.has_pets = rpt.has_pets
    AND gl.trade_region_code = rpt.trade_region_code
JOIN overall ovr
    ON 1=1
ORDER BY
    gl.{level},
    gl.has_pets,
    gl.trade_region_code;