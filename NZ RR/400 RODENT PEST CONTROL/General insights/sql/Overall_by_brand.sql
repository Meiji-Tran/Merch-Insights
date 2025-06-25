WITH BRAND_BY_CUSTOMER_TYPE AS (
    SELECT
        stl.customer_type_code,
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
        SUM(stl.total_exclude_gst_amount) AS sales,--sales
        count(distinct stl.dw_sales_transaction_id) AS total_trx,
        sum(sales_quantity) as total_units,
        count(distinct i.item_number) as num_items_purchased
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    INNER JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    WHERE 1=1
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.country_code = 'AU'
        AND stl.transaction_date BETWEEN {start_date} AND {end_date}
        and i.item_department_name = {dept}
        {additional_trx_condition}
        {target_item_condition}
    GROUP BY 
        stl.customer_type_code,
        BRAND_NAME
),
BRAND_SALES_OVERALL AS (
    SELECT
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
        SUM(stl.total_exclude_gst_amount) AS sales,--sales
        count(distinct stl.dw_sales_transaction_id) AS total_trx,
        sum(sales_quantity) as total_units,
        count(distinct i.item_number) as num_items_purchased
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    INNER JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    WHERE 1=1
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.country_code = 'AU'
        AND stl.transaction_date BETWEEN {start_date} AND {end_date}
        and i.item_department_name = {dept}
        {additional_trx_condition}
        {target_item_condition}
    GROUP BY 
        BRAND_NAME
)
SELECT A.*,B.SALES AS OVERALL_SALES, B.TOTAL_TRX AS OVERALL_TRX FROM BRAND_BY_CUSTOMER_TYPE A
INNER JOIN BRAND_SALES_OVERALL B
ON A.BRAND_NAME = B.BRAND_NAME
WHERE OVERALL_SALES > 10000