WITH TRAN AS (
    SELECT stl.dw_sales_transaction_id,
    stl.dw_loyalty_flybuys_account_id,
    stl.dw_commercial_account_id,
    stl.customer_type_code,
    stl.total_exclude_gst_amount,
    stl.sales_quantity,
    i.item_number
    from bdwprd_cds.sales.sales_transaction_line_fct stl
    INNER JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    INNER JOIN bdwprd_de.ia_merch_de.{target_item_table} ir ON 1=1
        AND i.item_number = ir.item_number
        AND i.country_code = ir.country_code
    WHERE 1=1
    AND stl.country_code = 'NZ'
    AND stl. transaction_date BETWEEN {start_date} AND {end_date}
    AND stl.sales_reporting_include_ind = TRUE
),
distinct_consumer_customers as (
    select dw_loyalty_flybuys_account_id,
    SUM(total_exclude_gst_amount) AS SALES,
    count(distinct dw_sales_transaction_id) AS TRX_CNT,
    sum(sales_quantity) as SALES_QTY,
    count(distinct item_number) as UNIQ_ITEMS_PURCHASED,
    from TRAN 
    where customer_type_code != 'Commercial'
    AND dw_loyalty_flybuys_account_id  != MD5_BINARY(-1)
    GROUP BY dw_loyalty_flybuys_account_id
    ),
distinct_commercial_customers as (
    select dw_commercial_account_id,
    SUM(total_exclude_gst_amount) AS SALES,
    count(distinct dw_sales_transaction_id) AS TRX_CNT,
    sum(sales_quantity) as SALES_QTY,
    count(distinct item_number) as UNIQ_ITEMS_PURCHASED,
    from TRAN 
    where customer_type_code = 'Commercial'
    AND dw_commercial_account_id != MD5_BINARY(-1)
    GROUP BY ALL
)
,consumer_demo_info AS (
    SELECT  fa.dw_loyalty_flybuys_account_id,
        value AS PRIMARY_SEGMENT,
        CASE 
        WHEN birth_date IS NULL THEN NULL
        WHEN birth_date < DATEADD(YEAR, -100, CURRENT_DATE()) THEN NULL
        WHEN birth_date > CURRENT_DATE() THEN NULL
        ELSE FLOOR(DATEDIFF(MONTH, birth_date, CURRENT_DATE()) / 12)
    END as age,
        psp.PSP_CUST_PREMIUMNESS
   FROM bdwprd_apps.customer_attributes.flybuys_household_primary_segment fhps
   inner join bdwprd_cds.loyalty_master.loyalty_flybuys_account_dim fa
    ON fa.flybuys_membership_number_hash = fhps.entity
   left join BDWPRD_DE.MERCH_PREMIUM_SPEND_DE.PSP_FINAL_CUSTOMER_AU psp
    on psp.CUSTOMER_ACCOUNT_ID = fa.dw_loyalty_flybuys_account_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY entity ORDER BY timestamp DESC) = 1
) 
, commercial_demo_info as (
    SELECT pp.dw_commercial_account_id,
     coalesce(ds.commercial_industry_segment_code,'Unclassified') AS PRIMARY_SEGMENT,
    psp.PSP_CUST_PREMIUMNESS
	from bdwprd_cds.commercial.commercial_account_dim pp
	INNER join BDWPRD_CDS.COMMERCIAL.COMMERCIAL_ACCOUNT_FCT fa
	   ON pp.dw_commercial_account_id = fa.dw_commercial_account_id
   LEFT JOIN  BDWPRD_CDS.COMMERCIAL.COMMERCIAL_INDUSTRY_SEGMENT_DIM ds
        ON fa.dw_commercial_industry_segment_id = ds.dw_commercial_industry_segment_id
    LEFT JOIN BDWPRD_DE.MERCH_PREMIUM_SPEND_DE.PSP_FINAL_CUSTOMER_AU psp
        on psp.CUSTOMER_ACCOUNT_ID = PP.dw_commercial_account_id
    where PP.country_code='AU'
),
joined_result as (
    SELECT 
    'Consumer' as CUSTOMER_TYPE,
    IFF(UPPER(PRIMARY_SEGMENT) ILIKE '%UNCLASSIFIABLE%' or primary_segment is null,'Unclassified',PRIMARY_SEGMENT) AS SEGMENT,
    COUNT(DISTINCT dcc.dw_loyalty_flybuys_account_id) AS N_CUSTOMERS,
    AVG(AGE) AS AVG_AGE,
    AVG(PSP_CUST_PREMIUMNESS) AS AVG_PSP,
    SUM(SALES) AS sales,
    SUM(TRX_CNT) AS total_trx,
    SUM(SALES_QTY) AS total_units,
    AVG(UNIQ_ITEMS_PURCHASED) AS AVG_num_items_purchased,
    
    FROM distinct_consumer_customers dcc
    left join consumer_demo_info cdi
    on dcc.dw_loyalty_flybuys_account_id = cdi.dw_loyalty_flybuys_account_id
    GROUP BY SEGMENT
    
    UNION 
    
    SELECT 
    'Consumer' as CUSTOMER_TYPE,
    'Overall_Flybuys' AS SEGMENT,
    COUNT(DISTINCT dcc.dw_loyalty_flybuys_account_id) AS N_CUSTOMERS,
    AVG(AGE) AS AVG_AGE,
    AVG(PSP_CUST_PREMIUMNESS) AS AVG_PSP,
    SUM(SALES) AS sales,
    SUM(TRX_CNT) AS total_trx,
    SUM(SALES_QTY) AS total_units,
    AVG(UNIQ_ITEMS_PURCHASED) AS AVG_num_items_purchased,
    
    FROM distinct_consumer_customers dcc
    left join consumer_demo_info cdi
    on dcc.dw_loyalty_flybuys_account_id = cdi.dw_loyalty_flybuys_account_id
    
    UNION
    
    SELECT 
    'Commercial' as CUSTOMER_TYPE,
    PRIMARY_SEGMENT AS SEGMENT,
    COUNT(DISTINCT dcc.dw_commercial_account_id) AS N_CUSTOMERS,
    null AS AVG_AGE,
    AVG(PSP_CUST_PREMIUMNESS) AS AVG_PSP,
    SUM(SALES) AS sales,
    SUM(TRX_CNT) AS total_trx,
    SUM(SALES_QTY) AS total_units,
    AVG(UNIQ_ITEMS_PURCHASED) AS AVG_num_items_purchased,
    
    FROM distinct_commercial_customers dcc
    left join commercial_demo_info cdi
    on dcc.dw_commercial_account_id = cdi.dw_commercial_account_id
    GROUP BY SEGMENT
    
    UNION 
    
    SELECT 
    'Commercial' as CUSTOMER_TYPE,
    'Overall' AS SEGMENT,
    COUNT(DISTINCT dcc.dw_commercial_account_id) AS N_CUSTOMERS,
    null AS AVG_AGE,
    AVG(PSP_CUST_PREMIUMNESS) AS AVG_PSP,
    SUM(SALES) AS sales,
    SUM(TRX_CNT) AS total_trx,
    SUM(SALES_QTY) AS total_units,
    AVG(UNIQ_ITEMS_PURCHASED) AS AVG_num_items_purchased,
    FROM distinct_commercial_customers dcc
    left join commercial_demo_info cdi
    on dcc.dw_commercial_account_id = cdi.dw_commercial_account_id
    
    UNION 
    
    SELECT 
    'Overall' as CUSTOMER_TYPE,
    'Overall' AS SEGMENT,
    NULL AS N_CUSTOMERS,
    null AS AVG_AGE,
    null AS AVG_PSP,
    SUM(total_exclude_gst_amount) AS SALES,
    count(distinct dw_sales_transaction_id) AS total_trx,
    sum(sales_quantity) as total_units,
    null AS AVG_num_items_purchased,
    FROM TRAN
    
    UNION 
    
    SELECT 
    'Consumer' as CUSTOMER_TYPE,
    'Overall' AS SEGMENT, -- includes no flybuys
    NULL AS N_CUSTOMERS,
    null AS AVG_AGE,
    null AS AVG_PSP,
    SUM(total_exclude_gst_amount) AS SALES,
    count(distinct dw_sales_transaction_id) AS total_trx,
    sum(sales_quantity) as total_units,
    null AS AVG_num_items_purchased,
    FROM TRAN
    where customer_type_code = 'Consumer'
)
select 
    CUSTOMER_TYPE || SEGMENT AS LOOKUP_VALUE,
    *,
    SALES / NULLIF(TOTAL_TRX, 0) AS SALES_PER_TRX,
    SALES / NULLIF(N_CUSTOMERS, 0) AS SALES_PER_ACTIVE_CUST,
    TOTAL_TRX / NULLIF(N_CUSTOMERS, 0) AS TRX_PER_ACTIVE_CUST,
    TOTAL_UNITS / NULLIF(TOTAL_TRX, 0) AS UNITS_PER_TRX,
    SALES / NULLIF(TOTAL_UNITS, 0) AS PRICE_PER_ITEM
    FROM JOINED_RESULT
