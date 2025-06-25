WITH  comm_sales as (
SELECT
        ds.commercial_industry_segment_code AS demographic_segment,
        --i.item_category_name as item_category_name,
        --i.item_department_name as item_department_name,
        --i.item_sub_department_name as item_sub_department_name,
		    --i.item_class_name,
		CASE -- specific to lighting 
          WHEN UPPER(BRAND_CODE) ILIKE '%ARLEC%' THEN 'ARLEC'
          WHEN UPPER(BRAND_CODE) ILIKE '%BRILLIANT%' THEN 'BRILLIANT'
          WHEN UPPER(BRAND_CODE) ILIKE '%LUC%BELLA%' THEN 'LUCE BELLA'
          WHEN UPPER(BRAND_CODE) ILIKE '%MIRABELLA%' THEN 'MIRABELLA'
          WHEN UPPER(BRAND_CODE) ILIKE '%PHIL%IPS%' THEN 'PHILIPS'
          WHEN UPPER(BRAND_CODE) ILIKE '%VERVE%' THEN 'VERVE'
          WHEN UPPER(BRAND_CODE) ILIKE '%SWISS%TECH%' THEN 'SWISS TECH'
          WHEN UPPER(BRAND_CODE) ILIKE '%DETA%' THEN 'DETA'
          ELSE UPPER(BRAND_CODE) 
        END AS BRAND_NAME,

               -- Number of customers who shopped said department by segment
        COUNT(DISTINCT stl.dw_sales_commercial_account_id) AS n_customers,---trxs
        SUM(stl.total_exclude_gst_amount) AS sales,--sales
         count(distinct stl.dw_sales_transaction_id) AS total_trx,
          sum(sales_quantity) as total_units,
  count(distinct i.item_number) as num_items_purchased,
  count(distinct fa.dw_commercial_account_id) as TOTAL_SEGMENT_SIZE
  
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    INNER JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    RIGHT JOIN bdwprd_cds.commercial.commercial_account_fct fa
        ON stl.dw_sales_commercial_account_id = fa.dw_commercial_account_id
    INNER JOIN  BDWPRD_CDS.COMMERCIAL.COMMERCIAL_INDUSTRY_SEGMENT_DIM ds
        ON fa.dw_commercial_industry_segment_id = ds.dw_commercial_industry_segment_id
        inner join BDWPRD_CDS.LOCATION.LOCATION_DIM loc
        on loc.dw_location_id=stl.dw_location_id
    WHERE 1=1
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.country_code = 'AU'
        and dw_sales_commercial_account_id is not null
        and stl.CUSTOMER_TYPE_CODE='Commercial'
        AND stl. transaction_date BETWEEN  {start_date} AND {end_date}
        AND i.item_department_name = {dept}
        {additional_trx_condition}
        {target_item_condition}
    GROUP BY commercial_industry_segment_code,
        -- i.item_category_name,
        -- i.item_department_name,
        -- i.item_sub_department_name,
	    	-- i.item_class_name,
		BRAND_NAME
       
    ORDER BY
        commercial_industry_segment_code DESC
-- Calculate total segment sizes for context in results

)

select * from comm_sales