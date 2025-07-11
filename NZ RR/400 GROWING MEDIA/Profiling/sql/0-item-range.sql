CREATE TABLE IF NOT EXISTS bdwprd_de.ia_merch_de.{target_item_table} AS (
    SELECT
        item_number,
        country_code,
        dw_item_id
    FROM bdwprd_cds.item.item_dim i-- Can be custom list from bdwprd_de.ia_merch_de instead
    WHERE 1=1
        and country_code = 'NZ'
        {target_product_condition}
    )
;
