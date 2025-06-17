
SELECT DISTINCT 
    --ITEM_SUB_DEPARTMENT_NAME
    ITEM_CLASS_NAME
            FROM bdwprd_cds.item.item_dim i
        WHERE 1=1
                and i.item_department_name = '300 LIGHTING'
                and ITEM_CLASS_NAME not in (
                '503 IND LIGHTING INSTALLATION',
                '502 FAN INSTALLATION'
                ) 
    -- change below to 'not in' for functional lighting. Or 'in' for decorative lighting. Comment whole block out for overall lighting
     --           and ITEM_CLASS_NAME in (
     --           '502 DECORATIVE',
     --           '502 G SERIES',
     --           '502 FLOOR LAMPS',
     --           '502 MIX AND MATCH LAMPS',
     --           '502 MIX AND MATCH SHADES',
     --           '502 TABLE LAMPS',
     --           '502 CHANDELIER',
     --           '502 COMPLETE 240V PENDANT',
     --           '502 DIY PENDANT',
     --           '502 SUSPENSION',
     --           '502 SOLAR DECORATIVE')