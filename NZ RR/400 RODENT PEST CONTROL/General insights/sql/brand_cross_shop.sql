WITH OVERALL_ELEC_SALES AS (
select BRAND_CODE,
SUM(stl.total_exclude_gst_amount) AS OVERALL_SALES,--sales
SUM(stl.total_exclude_gst_amount) / SUM(SUM(stl.total_exclude_gst_amount)) OVER () AS OVERALL_SALES_SHARE,
count(distinct stl.dw_sales_transaction_id) AS OVERALL_TRX
from bdwprd_cds.sales.sales_transaction_line_fct stl
    INNER JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
WHERE 1=1
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.country_code = 'AU'
        AND stl.transaction_date BETWEEN {start_date} AND {end_date}
        and i.item_department_name = '301 ELECTRICAL'
        and i.item_sub_department_name not in ('400 ENERGY', '400 HEALTHY HOMES',
        '400 POWER SYSTEMS','401 AIR CONDITIONING','401 BATTERIES','401 ELECTRICAL COOLING')
        -- excluding above sub-departments as those are very specific products, creating bias towards certain brands
        AND customer_type_code = {brand_xshop_cust_type}
Group by BRAND_CODE
HAVING OVERALL_SALES >0
),
-- lighting customers
lighting_customers as (
select {id_col} from
bdwprd_cds.sales.sales_transaction_line_fct stl
    INNER JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
WHERE 1=1
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.country_code = 'AU'
        AND stl.transaction_date BETWEEN  {start_date} AND {end_date}
        and i.item_department_name = '300 LIGHTING'
        and UPPER(BRAND_CODE) ILIKE {target_brand}
        and {id_col} != MD5_BINARY(-1)
GROUP BY ALL 
HAVING count(distinct stl.dw_sales_transaction_id) >= 10 -- remove random shoppers
),
CROSS_SHOP_BY_BRAND AS (
select BRAND_CODE,
SUM(stl.total_exclude_gst_amount) AS sales,--sales
SUM(stl.total_exclude_gst_amount) / SUM(SUM(stl.total_exclude_gst_amount)) OVER () AS SALES_SHARE,
count(distinct stl.dw_sales_transaction_id) AS total_trx
from bdwprd_cds.sales.sales_transaction_line_fct stl
    INNER JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    INNER JOIN lighting_customers LC
        ON STL.{id_col} = LC.{id_col}
WHERE 1=1
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.country_code = 'AU'
        AND stl.transaction_date BETWEEN {start_date} AND {end_date}
        and i.item_department_name = '301 ELECTRICAL'
        and i.item_sub_department_name not in ('400 ENERGY', '400 HEALTHY HOMES',
        '400 POWER SYSTEMS','401 AIR CONDITIONING','401 BATTERIES','401 ELECTRICAL COOLING')
Group by BRAND_CODE
HAVING SALES >0
)
SELECT CSBB.*,
OVERALL_SALES,
OVERALL_SALES_SHARE,
OVERALL_TRX,
CSBB.SALES_SHARE / OVERALL_SALES_SHARE as sales_index
FROM CROSS_SHOP_BY_BRAND CSBB LEFT JOIN OVERALL_ELEC_SALES OES ON 
CSBB.BRAND_CODE = OES.BRAND_CODE
WHERE OVERALL_SALES_SHARE > 0.001

