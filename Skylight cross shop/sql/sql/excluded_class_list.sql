SELECT DISTINCT 
    --ITEM_SUB_DEPARTMENT_NAME
    ITEM_CLASS_NAME
            FROM bdwprd_cds.item.item_dim i
        WHERE 1=1
                and i.item_sub_department_name = '400 SKYLIGHTS AND ROOF WINDOWS'; 