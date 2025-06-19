-- Demographic Segment Profiling
-- Get most recent demographic segment classification
WITH dem_seg AS (
    SELECT
        entity AS flybuys_membership_number_hash,
        value AS demographic_segment
    FROM bdwprd_apps.customer_attributes.flybuys_household_primary_segment
    WHERE value NOT ILIKE 'unclassifiable%'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY entity ORDER BY timestamp DESC) = 1
),

main_calc AS (
    SELECT
        i.{level},
        ds.demographic_segment,

        -- Item range metrics
        SUM(IFF(ir.item_number IS NOT NULL, stl.total_exclude_gst_amount, NULL)) AS range_sales,
        SUM(IFF(ir.item_number IS NOT NULL, stl.item_quantity, NULL)) AS range_quantity,
        COUNT(DISTINCT IFF(ir.item_number IS NOT NULL, stl.dw_loyalty_flybuys_account_id, NULL)) AS range_customers,

        -- Item range shares
        SUM(IFF(ir.item_number IS NOT NULL, stl.total_exclude_gst_amount, NULL)) 
            / NULLIF(SUM(SUM(IFF(ir.item_number IS NOT NULL, stl.total_exclude_gst_amount, NULL))) OVER (), 0) AS range_sales_share,

        SUM(IFF(ir.item_number IS NOT NULL, stl.item_quantity, NULL)) 
            / NULLIF(SUM(SUM(IFF(ir.item_number IS NOT NULL, stl.item_quantity, NULL))) OVER (), 0) AS range_quantity_share,

        COUNT(DISTINCT IFF(ir.item_number IS NOT NULL, stl.dw_loyalty_flybuys_account_id, NULL)) 
            / NULLIF(SUM(COUNT(DISTINCT IFF(ir.item_number IS NOT NULL, stl.dw_loyalty_flybuys_account_id, NULL))) OVER (), 0) AS range_customer_share,

        -- Overall metrics
        SUM(stl.total_exclude_gst_amount) AS overall_sales,
        SUM(stl.item_quantity) AS overall_quantity,
        COUNT(DISTINCT stl.dw_loyalty_flybuys_account_id) AS overall_customers,

        -- Overall shares
        SUM(stl.total_exclude_gst_amount)
            / NULLIF(SUM(SUM(stl.total_exclude_gst_amount)) OVER (), 0) AS overall_sales_share,

        SUM(stl.item_quantity)
            / NULLIF(SUM(SUM(stl.item_quantity)) OVER (), 0) AS overall_quantity_share,

        COUNT(DISTINCT stl.dw_loyalty_flybuys_account_id)
            / NULLIF(SUM(COUNT(DISTINCT stl.dw_loyalty_flybuys_account_id)) OVER (), 0) AS overall_customer_share,

        -- Indexes
        (SUM(IFF(ir.item_number IS NOT NULL, stl.total_exclude_gst_amount, NULL)) 
            / NULLIF(SUM(SUM(IFF(ir.item_number IS NOT NULL, stl.total_exclude_gst_amount, NULL))) OVER (), 0))
        / NULLIF(SUM(stl.total_exclude_gst_amount) 
            / NULLIF(SUM(SUM(stl.total_exclude_gst_amount)) OVER (), 0), 0) AS sales_index,

        (SUM(IFF(ir.item_number IS NOT NULL, stl.item_quantity, NULL)) 
            / NULLIF(SUM(SUM(IFF(ir.item_number IS NOT NULL, stl.item_quantity, NULL))) OVER (), 0))
        / NULLIF(SUM(stl.item_quantity) 
            / NULLIF(SUM(SUM(stl.item_quantity)) OVER (), 0), 0) AS quantity_index,

        (COUNT(DISTINCT IFF(ir.item_number IS NOT NULL, stl.dw_loyalty_flybuys_account_id, NULL)) 
            / NULLIF(SUM(COUNT(DISTINCT IFF(ir.item_number IS NOT NULL, stl.dw_loyalty_flybuys_account_id, NULL))) OVER (), 0))
        / NULLIF(COUNT(DISTINCT stl.dw_loyalty_flybuys_account_id)
            / NULLIF(SUM(COUNT(DISTINCT stl.dw_loyalty_flybuys_account_id)) OVER (), 0), 0) AS customer_index

    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    INNER JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    INNER JOIN bdwprd_cds.location.location_dim l
        ON stl.dw_location_id = l.dw_location_id
    INNER JOIN bdwprd_cds.loyalty_master.loyalty_flybuys_account_dim fa
        ON stl.dw_loyalty_flybuys_account_id = fa.dw_loyalty_flybuys_account_id
    INNER JOIN dem_seg ds
        ON fa.flybuys_membership_number_hash = ds.flybuys_membership_number_hash
    LEFT JOIN {table_name} ir 
        ON i.item_number = ir.item_number
        AND i.country_code = ir.country_code

    WHERE 1=1
        AND ds.demographic_segment != 'Unknown'
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.country_code = '{country}'
        AND stl.transaction_date BETWEEN {start_date} AND {end_date}
        AND stl.customer_type_code = 'Consumer'

    GROUP BY
        i.{level},
        ds.demographic_segment
)

-- Final selection: filter for only classes in departments represented in the item range
SELECT *
FROM main_calc mc
JOIN (
    SELECT DISTINCT i.{level}
    FROM bdwprd_de.ia_merch_de.{table_name} ir
    JOIN bdwprd_cds.item.item_dim i
        ON ir.item_number = i.item_number
        AND ir.country_code = i.country_code
) valid_level
  ON mc.{level} = valid_level.{level}
ORDER BY
    valid_level.{level},
    demographic_segment;
