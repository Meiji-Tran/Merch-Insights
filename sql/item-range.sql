-- Item range temp table
CREATE TABLE bdwprd_de.ia_merch_de.{table_name} AS (
    SELECT
        item_number,
        country_code
    FROM bdwprd_de.ia_merch_de.ev_items
    )
;