SELECT DISTINCT 
    ITEM_SUB_DEPARTMENT_NAME
            FROM bdwprd_cds.item.item_dim i
        WHERE 1=1
                and i.item_category_name = '200 GARDENING'
                and i.item_sub_department_name != '400 GROWING MEDIA'