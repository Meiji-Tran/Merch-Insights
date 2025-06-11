SELECT DISTINCT
    item_number,
    item_category_name,
    item_department_name,
    item_sub_department_name,
    item_class_name,
    item_sub_class_name
FROM BDWPRD_CDS.ITEM.ITEM_DIM
ORDER BY item_category_name, item_department_name, item_sub_department_name, item_class_name, item_sub_class_name;