-- Item range temp table
CREATE OR REPLACE TABLE bdwprd_de.ia_merch_de.{table_name} AS (
    SELECT
        item_number,
        country_code,
        dw_item_id
    FROM bdwprd_cds.item.item_dim
    WHERE 1=1
        {filter}
    )
;