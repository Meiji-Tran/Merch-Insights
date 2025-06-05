-- Item range temp table
CREATE TABLE bdwprd_de.ia_merch_de.{table_name} AS (
    SELECT
        item_number,
        country_code,
        dw_item_id
    FROM bdwprd_cds.item.item_dim -- Can be custom list from bdwprd_de.ia_merch_de instead
    WHERE 1=1
        AND item_sub_department_name = '400 DECKING TIMBER' -- Change here as required
        AND item_class_name IN ('500 EKODECK DESIGNER', '500 EKODECK CLASSIC') -- Can determine range in multiple WHERE clauses
    )
;