SELECT DISTINCT 
    --ITEM_SUB_DEPARTMENT_NAME
    ITEM_CLASS_NAME
            FROM bdwprd_cds.item.item_dim i
        WHERE 1=1
                and i.item_class_name in (
                '500 ROOF WINDOWS',
                '500 SKYLIGHT ACCESSORIES'
                ); 