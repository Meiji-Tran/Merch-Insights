WITH range_trx AS (
    SELECT
        i.{level},
        stl.total_exclude_gst_amount AS sales,
        stl.item_quantity AS quantity
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    JOIN bdwprd_de.ia_merch_de.{table_name} ir
        ON stl.dw_item_id = ir.dw_item_id
        AND stl.country_code = ir.country_code
    JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    WHERE 1=1
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.country_code = '{country}'
        AND stl.transaction_date BETWEEN {start_date} AND {end_date}
        AND stl.customer_type_code != 'Commercial'
),

-- Totals per {level}
total_level AS (
    SELECT
        {level},
        SUM(sales) AS total_sales,
        SUM(quantity) AS total_quantity
    FROM range_trx
    GROUP BY {level}
),

-- Overall totals
overall AS (
    SELECT
        SUM(sales) AS overall_sales,
        SUM(quantity) AS overall_quantity
    FROM range_trx
)

-- Final output with share columns
SELECT
    tl.{level},
    tl.total_sales,
    tl.total_quantity,
    tl.total_sales / NULLIF(ovr.overall_sales, 0) AS sales_share,
    tl.total_quantity / NULLIF(ovr.overall_quantity, 0) AS quantity_share
FROM total_level tl
CROSS JOIN overall ovr
ORDER BY tl.{level};