WITH BRAND_BY_CUSTOMER_TYPE AS (
    SELECT
        stl.customer_type_code,
        upper(brand_code)) as brand_code,
        --coalesce(upper(psp_brand), upper(brand_code)) as brand_code,
        SUM(stl.total_exclude_gst_amount) AS sales,--sales
        count(distinct stl.dw_sales_transaction_id) AS total_trx,
        sum(sales_quantity) as total_units,
        count(distinct i.item_number) as num_items_purchased,
        count(distinct iff(sales_quantity<0,dw_sales_transaction_id,null)) as return_trx_cnt
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    INNER JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    INNER JOIN bdwprd_de.ia_merch_de.{target_item_table} ir ON 1=1
        AND i.item_number = ir.item_number
        AND i.country_code = ir.country_code
    -- LEFT JOIN bdwprd_de.merch_premium_spend_de.psp_final_item_au psp
    --     ON i.dw_item_id = psp.dw_item_id
    WHERE 1=1
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.country_code = 'NZ'
        AND stl.transaction_date BETWEEN {start_date} AND {end_date}
    GROUP BY ALL
),
BRAND_SALES_OVERALL AS (
    SELECT
        upper(brand_code)) as brand_code,
        -- coalesce(upper(psp_brand), upper(brand_code)) as brand_code,
        SUM(stl.total_exclude_gst_amount) AS sales,
        count(distinct stl.dw_sales_transaction_id) AS total_trx,
        sum(sales_quantity) as total_units,
        count(distinct i.item_number) as num_items_purchased
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    INNER JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    INNER JOIN bdwprd_de.ia_merch_de.{target_item_table} ir ON 1=1
        AND i.item_number = ir.item_number
        AND i.country_code = ir.country_code
    -- LEFT JOIN bdwprd_de.merch_premium_spend_de.psp_final_item_au psp
    --     ON i.dw_item_id = psp.dw_item_id
    WHERE 1=1
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.country_code = 'NZ'
        AND stl.transaction_date BETWEEN {start_date} AND {end_date}
    GROUP BY ALL
)
SELECT A.*,B.SALES AS OVERALL_SALES, B.TOTAL_TRX AS OVERALL_TRX FROM BRAND_BY_CUSTOMER_TYPE A
INNER JOIN BRAND_SALES_OVERALL B
ON A.BRAND_CODE = B.BRAND_CODE