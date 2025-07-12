-- Change line 28 - 33 for different seed departments

WITH seed_trxs AS (
    SELECT
        i.item_class_name,
        stl.dw_sales_transaction_id,
        SUM(stl.total_exclude_gst_amount) AS seed_sales,
        SUM(stl.item_quantity) AS seed_quantity,
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    INNER JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    INNER JOIN bdwprd_de.ia_merch_de.{target_item_table} ir ON 1=1
        AND i.item_number = ir.item_number
        AND i.country_code = ir.country_code
    WHERE 1=1
        AND stl.country_code = 'NZ'
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.transaction_date BETWEEN {start_date} AND {end_date}
    GROUP BY ALL
)
, halo_trxs AS (
    SELECT
        customer_type_code,
        i.item_class_name AS halo_class,
        SUM(stl.total_exclude_gst_amount) AS halo_sales,
        SUM(stl.item_quantity) AS halo_quantity,
        halo_sales / SUM(halo_sales) OVER () AS halo_sales_share,
        halo_quantity / SUM(halo_quantity) OVER () AS halo_quantity_share
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    INNER JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    INNER JOIN seed_trxs t
        ON stl.dw_sales_transaction_id = t.dw_sales_transaction_id
    WHERE 1=1
        AND stl.country_code = 'NZ'
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.transaction_date BETWEEN {start_date} AND {end_date}
        AND i.item_number not in  (SELECT item_number FROM bdwprd_de.ia_merch_de.{target_item_table})
        AND i.item_department_name != '.Unk'
    GROUP BY all
    ORDER BY
        halo_class
)
select * from halo_trxs;