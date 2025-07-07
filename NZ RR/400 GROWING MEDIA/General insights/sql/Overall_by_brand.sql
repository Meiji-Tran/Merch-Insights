WITH BRAND_BY_CUSTOMER_TYPE AS (
    SELECT
        stl.customer_type_code,
        CASE
            WHEN UPPER(BRAND_CODE) ILIKE '%CATCH''EM%'      THEN 'CATCHEM'
            WHEN UPPER(BRAND_CODE) ILIKE '%CATCHEM%'        THEN 'CATCHEM'
            WHEN UPPER(BRAND_CODE) ILIKE '%THE BIG CHEESE%' THEN 'BIG CHEESE'
            WHEN UPPER(BRAND_CODE) ILIKE '%BIG CHEESE%'     THEN 'BIG CHEESE'
            WHEN UPPER(BRAND_CODE) ILIKE '%SAXON%'          THEN 'SAXON'
            WHEN UPPER(BRAND_CODE) ILIKE '%TRAP%EASE%'      THEN 'TRAP EASE'
            WHEN UPPER(BRAND_CODE) ILIKE '%TIMES UP%'       THEN 'TIMES UP'
            WHEN UPPER(BRAND_CODE) ILIKE '%NOOSKI%'         THEN 'NOOSKI'
            WHEN UPPER(BRAND_CODE) ILIKE '%TOMCAT%'         THEN 'TOMCAT'
            WHEN UPPER(BRAND_CODE) ILIKE '%STONEVANTAGE%'   THEN 'STONEVANTAGE'
            WHEN UPPER(BRAND_CODE) ILIKE '%YATES%'          THEN 'YATES'
            WHEN UPPER(BRAND_CODE) ILIKE '%EASYTRAP%'       THEN 'EASYTRAP'
            WHEN UPPER(BRAND_CODE) ILIKE '%UNBRANDED%'      THEN 'UNBRANDED'
            WHEN UPPER(BRAND_CODE) ILIKE '%ENVIROSAFE%'     THEN 'ENVIROSAFE'
            WHEN UPPER(BRAND_CODE) ILIKE '%STRATAGEM%'      THEN 'STRATAGEM'
            WHEN UPPER(BRAND_CODE) ILIKE '%GRAHAMS%'        THEN 'GRAHAMS'
            WHEN UPPER(BRAND_CODE) ILIKE '%BRUNNINGS%'      THEN 'BRUNNINGS'
            WHEN UPPER(BRAND_CODE) ILIKE '%CLIX%'           THEN 'CLIX'
            WHEN UPPER(BRAND_CODE) ILIKE '%EASE%'           THEN 'EASE'
            WHEN UPPER(BRAND_CODE) ILIKE '%RACUMIN%'        THEN 'RACUMIN'
            WHEN UPPER(BRAND_CODE) ILIKE '%TRAPPED!%'       THEN 'TRAPPED!'
            WHEN UPPER(BRAND_CODE) ILIKE '%KIWICARE%'       THEN 'KIWICARE'
            WHEN UPPER(BRAND_CODE) ILIKE '%GO TRAP%'        THEN 'GO TRAP'
            WHEN UPPER(BRAND_CODE) ILIKE '%GOTCHA%'         THEN 'GOTCHA'
            WHEN UPPER(BRAND_CODE) ILIKE '%VANGUARD%'       THEN 'VANGUARD'
            WHEN UPPER(BRAND_CODE) ILIKE '%MR FOTHERGILL%''S%' THEN 'MR FOTHERGILL'
            WHEN UPPER(BRAND_CODE) ILIKE '%GARDMAN%'        THEN 'GARDMAN'
            WHEN UPPER(BRAND_CODE) ILIKE '%RATSAK%'         THEN 'RATSAK'
            WHEN UPPER(BRAND_CODE) ILIKE '%TALON%'          THEN 'TALON'
            ELSE UPPER(BRAND_CODE)
        END AS BRAND_NAME,
        SUM(stl.total_exclude_gst_amount) AS sales,
        count(distinct stl.dw_sales_transaction_id) AS total_trx,
        sum(sales_quantity) as total_units,
        count(distinct i.item_number) as num_items_purchased
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    INNER JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    WHERE 1=1
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.country_code = 'NZ'
        AND stl.transaction_date BETWEEN {start_date} AND {end_date}
        and i.item_sub_department_name = {sub_dept}
    GROUP BY 
        stl.customer_type_code,
        BRAND_NAME
),
BRAND_SALES_OVERALL AS (
    SELECT
        CASE 
            WHEN UPPER(BRAND_CODE) ILIKE '%CATCH''EM%'      THEN 'CATCHEM'
            WHEN UPPER(BRAND_CODE) ILIKE '%CATCHEM%'        THEN 'CATCHEM'
            WHEN UPPER(BRAND_CODE) ILIKE '%THE BIG CHEESE%' THEN 'BIG CHEESE'
            WHEN UPPER(BRAND_CODE) ILIKE '%BIG CHEESE%'     THEN 'BIG CHEESE'
            WHEN UPPER(BRAND_CODE) ILIKE '%SAXON%'          THEN 'SAXON'
            WHEN UPPER(BRAND_CODE) ILIKE '%TRAP%EASE%'      THEN 'TRAP EASE'
            WHEN UPPER(BRAND_CODE) ILIKE '%TIMES UP%'       THEN 'TIMES UP'
            WHEN UPPER(BRAND_CODE) ILIKE '%NOOSKI%'         THEN 'NOOSKI'
            WHEN UPPER(BRAND_CODE) ILIKE '%TOMCAT%'         THEN 'TOMCAT'
            WHEN UPPER(BRAND_CODE) ILIKE '%STONEVANTAGE%'   THEN 'STONEVANTAGE'
            WHEN UPPER(BRAND_CODE) ILIKE '%YATES%'          THEN 'YATES'
            WHEN UPPER(BRAND_CODE) ILIKE '%EASYTRAP%'       THEN 'EASYTRAP'
            WHEN UPPER(BRAND_CODE) ILIKE '%UNBRANDED%'      THEN 'UNBRANDED'
            WHEN UPPER(BRAND_CODE) ILIKE '%ENVIROSAFE%'     THEN 'ENVIROSAFE'
            WHEN UPPER(BRAND_CODE) ILIKE '%STRATAGEM%'      THEN 'STRATAGEM'
            WHEN UPPER(BRAND_CODE) ILIKE '%GRAHAMS%'        THEN 'GRAHAMS'
            WHEN UPPER(BRAND_CODE) ILIKE '%BRUNNINGS%'      THEN 'BRUNNINGS'
            WHEN UPPER(BRAND_CODE) ILIKE '%CLIX%'           THEN 'CLIX'
            WHEN UPPER(BRAND_CODE) ILIKE '%EASE%'           THEN 'EASE'
            WHEN UPPER(BRAND_CODE) ILIKE '%RACUMIN%'        THEN 'RACUMIN'
            WHEN UPPER(BRAND_CODE) ILIKE '%TRAPPED!%'       THEN 'TRAPPED!'
            WHEN UPPER(BRAND_CODE) ILIKE '%KIWICARE%'       THEN 'KIWICARE'
            WHEN UPPER(BRAND_CODE) ILIKE '%GO TRAP%'        THEN 'GO TRAP'
            WHEN UPPER(BRAND_CODE) ILIKE '%GOTCHA%'         THEN 'GOTCHA'
            WHEN UPPER(BRAND_CODE) ILIKE '%VANGUARD%'       THEN 'VANGUARD'
            WHEN UPPER(BRAND_CODE) ILIKE '%MR FOTHERGILL%''S%' THEN 'MR FOTHERGILL'
            WHEN UPPER(BRAND_CODE) ILIKE '%GARDMAN%'        THEN 'GARDMAN'
            WHEN UPPER(BRAND_CODE) ILIKE '%RATSAK%'         THEN 'RATSAK'
            WHEN UPPER(BRAND_CODE) ILIKE '%TALON%'          THEN 'TALON'
            ELSE UPPER(BRAND_CODE) 
        END AS BRAND_NAME,
        SUM(stl.total_exclude_gst_amount) AS sales,
        count(distinct stl.dw_sales_transaction_id) AS total_trx,
        sum(sales_quantity) as total_units,
        count(distinct i.item_number) as num_items_purchased
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    INNER JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    WHERE 1=1
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.country_code = 'NZ'
        AND stl.transaction_date BETWEEN {start_date} AND {end_date}
        and i.item_sub_department_name = {sub_dept}
    GROUP BY 
        BRAND_NAME
)
SELECT A.*,B.SALES AS OVERALL_SALES, B.TOTAL_TRX AS OVERALL_TRX FROM BRAND_BY_CUSTOMER_TYPE A
INNER JOIN BRAND_SALES_OVERALL B
ON A.BRAND_NAME = B.BRAND_NAME
-- WHERE OVERALL_SALES > 10000