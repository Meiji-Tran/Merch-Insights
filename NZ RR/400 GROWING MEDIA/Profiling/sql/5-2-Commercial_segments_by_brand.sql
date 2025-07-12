SELECT
        ds.commercial_industry_segment_code AS demographic_segment,
	coalesce(upper(psp_brand), upper(brand_code)) AS brand_code,
        COUNT(DISTINCT stl.dw_sales_commercial_account_id) AS n_customers,---trxs
        SUM(stl.total_exclude_gst_amount) AS sales,--sales
         count(distinct stl.dw_sales_transaction_id) AS total_trx,
          sum(sales_quantity) as total_units,
  count(distinct i.item_number) as num_items_purchased,
  count(distinct fa.dw_commercial_account_id) as TOTAL_SEGMENT_SIZE
  
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    INNER JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    INNER JOIN bdwprd_cds.commercial.commercial_account_fct fa
        ON stl.dw_sales_commercial_account_id = fa.dw_commercial_account_id
    INNER JOIN  BDWPRD_CDS.COMMERCIAL.COMMERCIAL_INDUSTRY_SEGMENT_DIM ds
        ON fa.dw_commercial_industry_segment_id = ds.dw_commercial_industry_segment_id
        inner join BDWPRD_CDS.LOCATION.LOCATION_DIM loc
        on loc.dw_location_id=stl.dw_location_id
    LEFT JOIN bdwprd_de.merch_premium_spend_de.psp_final_item_au psp
        ON i.dw_item_id = psp.dw_item_id
    WHERE 1=1
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.country_code = 'NZ'
        and dw_sales_commercial_account_id is not null
        and stl.CUSTOMER_TYPE_CODE='Commercial'
        AND stl. transaction_date BETWEEN  {start_date} AND {end_date}
        {target_product_condition}
    GROUP BY all
    ORDER BY
        commercial_industry_segment_code DESC
