SELECT DISTINCT 
    ITEM_SUB_DEPARTMENT_NAME
            FROM bdwprd_cds.item.item_dim i
        WHERE 1=1
                and i.item_department_name = '300 GREENLIFE'; 