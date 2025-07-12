WITH dem_seg AS (
    SELECT     entity,
        value,
   FROM bdwprd_apps.customer_attributes.flybuys_household_primary_segment fhps
   inner join bdwprd_cds.loyalty_master.loyalty_flybuys_account_dim fa
    ON fa.flybuys_membership_number_hash = fhps.entity
    QUALIFY ROW_NUMBER() OVER (PARTITION BY entity ORDER BY timestamp DESC) = 1
)

    SELECT
        ds.value AS demographic_segment,
        brand_code,
        -- coalesce(upper(psp_brand), upper(brand_code)) AS brand_code,
        COUNT(DISTINCT stl.dw_loyalty_flybuys_account_id) AS n_customers,---trxs
        SUM(stl.total_exclude_gst_amount) AS sales,--sales
         count(distinct stl.dw_sales_transaction_id) AS total_trx,
          sum(sales_quantity) as total_units,
          count(distinct i.item_number) as num_items_purchased
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    INNER JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    INNER JOIN bdwprd_cds.loyalty_master.loyalty_flybuys_account_dim fa
        ON stl.dw_loyalty_flybuys_account_id = fa.dw_loyalty_flybuys_account_id
    INNER JOIN dem_seg ds
        ON fa.flybuys_membership_number_hash = ds.entity
        inner join BDWPRD_CDS.LOCATION.LOCATION_DIM loc
        on loc.dw_location_id=stl.dw_location_id
    -- LEFT JOIN bdwprd_de.merch_premium_spend_de.psp_final_item_au psp
    --     ON i.dw_item_id = psp.dw_item_id
    WHERE 1=1
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.country_code = 'AU'
        AND stl.transaction_date BETWEEN {start_date} AND {end_date}
        {target_product_condition}
	GROUP BY all
    ORDER BY
        demographic_segment DESC