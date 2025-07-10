WITH potting_data AS (
    SELECT
        -- Clean and uppercase brand (remove collation)
        i.brand_code, --UPPER(REGEXP_REPLACE(CAST(i.brand_code AS VARCHAR), '[^A-Z0-9 ]', '')) AS cleaned_brand,
        i.item_number,
        i.item_description,

        -- Extract numeric value with unit (e.g., '30L' or '1.5m3')
        REGEXP_SUBSTR(CAST(i.item_description AS VARCHAR), '\\d{{1,3}}(\\.\\d+)?(L|m3)') AS raw_volume,

        COUNT(DISTINCT stl.dw_sales_transaction_id) AS total_trx,
        SUM(stl.item_quantity) AS total_qty,
        SUM(stl.total_exclude_gst_amount) AS total_sales
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    WHERE 1 = 1
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.country_code = '{country}'
        AND stl.transaction_date BETWEEN {start_date} AND {end_date}
        AND i.item_class_name = '500 POTTING MIXES'
    GROUP BY
        i.brand_code,
        i.item_number,
        i.item_description
),

volume_estimates AS (
    SELECT
        brand_code, --cleaned_brand,
        item_number,
        item_description,
        raw_volume,

        -- Final parsed litres
        CASE
            WHEN raw_volume ILIKE '%m3' THEN TRY_TO_NUMBER(REGEXP_SUBSTR(raw_volume, '\\d{{1,3}}(\\.\\d+)?')) * 1000
            ELSE TRY_TO_NUMBER(REGEXP_SUBSTR(raw_volume, '\\d{{1,3}}(\\.\\d+)?'))
        END AS package_litre,

        total_trx,
        total_qty,
        total_sales,

        -- Avg quantity and litres per transaction
        DIV0(total_qty, total_trx) AS avg_qty_per_trx,
        DIV0(total_qty, total_trx) * 
            CASE
                WHEN raw_volume ILIKE '%m3' THEN TRY_TO_NUMBER(REGEXP_SUBSTR(raw_volume, '\\d{{1,3}}(\\.\\d+)?')) * 1000
                ELSE TRY_TO_NUMBER(REGEXP_SUBSTR(raw_volume, '\\d{{1,3}}(\\.\\d+)?'))
            END AS estimated_litres_per_trx,

        -- Pricing
        DIV0(total_sales, total_qty) AS avg_price_per_unit,
        DIV0(total_sales, DIV0(total_qty, total_trx) * total_trx) AS avg_price_per_litre
    FROM potting_data
)

SELECT *
FROM volume_estimates
--WHERE package_litre IS NOT NULL
ORDER BY total_sales DESC;
