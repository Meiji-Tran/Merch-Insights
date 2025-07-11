WITH results_all AS (
    SELECT
        i.item_category_name as item_category_name,
        i.item_department_name as item_department_name,
        i.item_sub_department_name as item_sub_department_name,
        i.item_class_name,
        customer_type_code,
        -- Number of customers who shopped said department by segment
        SUM(stl.total_exclude_gst_amount) AS sales,--sales
         count(distinct stl.dw_sales_transaction_id) AS total_trx,
          sum(sales_quantity) as total_units,
          count(distinct i.item_number) as num_items_purchased

    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    INNER JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    INNER JOIN bdwprd_de.ia_merch_de.{target_item_table} ir ON 1=1
        AND i.item_number = ir.item_number
        AND i.country_code = ir.country_code
   inner join BDWPRD_CDS.LOCATION.LOCATION_DIM loc
        on loc.dw_location_id=stl.dw_location_id
    WHERE 1=1
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.country_code = 'NZ'
        AND stl. transaction_date BETWEEN {start_date} AND {end_date}
    GROUP BY all
 
-- Calculate total segment sizes for context in results
)
SELECT
     ra.item_category_name,
    ra.item_department_name,
    ra.item_sub_department_name,
    ra.item_class_name,
    customer_type_code,
    ra.sales as sales,
    ra.total_trx as total_trx,
    ra.total_units as total_units,
    ra.num_items_purchased,
    RA.SALES / NULLIF(RA.TOTAL_TRX, 0) AS SALES_PER_TRX,
    NULL AS SALES_PER_ACTIVE_CUST, -- To remove in future
    NULL AS TRX_PER_ACTIVE_CUST, -- To remove in future
    RA.TOTAL_UNITS / NULLIF(RA.TOTAL_TRX, 0) AS UNITS_PER_TRX,
    RA.SALES / NULLIF(RA.TOTAL_UNITS, 0) AS PRICE_PER_ITEM

    
FROM  results_all ra
