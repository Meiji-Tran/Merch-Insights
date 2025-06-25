WITH dem_seg AS (
    SELECT     entity,
        value,
        (FLOOR(DATEDIFF(MONTH, IFF(birth_date < DATEADD(YEAR, -100, CURRENT_DATE()), NULL, birth_date), CURRENT_DATE()) / 12)) as age
   FROM bdwprd_apps.customer_attributes.flybuys_household_primary_segment fhps
   inner join bdwprd_cds.loyalty_master.loyalty_flybuys_account_dim fa
    ON fa.flybuys_membership_number_hash = fhps.entity
    QUALIFY ROW_NUMBER() OVER (PARTITION BY entity ORDER BY timestamp DESC) = 1
), results_all AS (
    SELECT
        ds.value AS demographic_segment,
        i.item_category_name as item_category_name,
        i.item_department_name as item_department_name,
        i.item_sub_department_name as item_sub_department_name,
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
        COUNT(DISTINCT stl.dw_loyalty_flybuys_account_id) AS n_customers,---trxs
        SUM(stl.total_exclude_gst_amount) AS sales,--sales
         count(distinct stl.dw_sales_transaction_id) AS total_trx,
          sum(sales_quantity) as total_units,
          count(distinct i.item_number) as num_items_purchased
        

    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    INNER JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    RIGHT JOIN bdwprd_cds.loyalty_master.loyalty_flybuys_account_dim fa
        ON stl.dw_loyalty_flybuys_account_id = fa.dw_loyalty_flybuys_account_id
    RIGHT JOIN dem_seg ds
        ON fa.flybuys_membership_number_hash = ds.entity
        inner join BDWPRD_CDS.LOCATION.LOCATION_DIM loc
        on loc.dw_location_id=stl.dw_location_id
    WHERE 1=1
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.country_code = 'AU'
        AND stl. transaction_date BETWEEN  {start_date} AND {end_date}
        AND i.item_department_name = {dept}
        {additional_trx_condition}
        {target_item_condition}
	GROUP BY all
    ORDER BY
        demographic_segment DESC
), segment_sizes AS (
    SELECT
        value AS demographic_segment,
        COUNT(*) AS total_segment_size,
        sum(case when age<35 then 1 else 0 end) as U35seg
        
    FROM dem_seg
    GROUP BY ALL
)
SELECT
    ra.demographic_segment,
    ss.total_segment_size,
    ss.U35seg,
    ra.item_category_name,
    ra.item_department_name,
    ra.item_sub_department_name,
    --ra.item_class_name,
    ra.BRAND_NAME,
    ra.sales as sales,
    ra.n_customers AS n_customers,
    ra.total_trx as total_trx,
    ra.total_units as total_units,
    ra.num_items_purchased
    
FROM  results_all ra
left join segment_sizes ss
    ON ra.demographic_segment = ss.demographic_segment
ORDER BY
    ra.demographic_segment
    DESC