WITH dem_seg AS (
    SELECT     entity,
        value,
        (FLOOR(DATEDIFF(MONTH, IFF(birth_date < DATEADD(YEAR, -100, CURRENT_DATE()), NULL, birth_date), CURRENT_DATE()) / 12)) as age
   FROM bdwprd_apps.customer_attributes.flybuys_household_primary_segment fhps
   inner join bdwprd_cds.loyalty_master.loyalty_flybuys_account_dim fa
    ON fa.flybuys_membership_number_hash = fhps.entity
    QUALIFY ROW_NUMBER() OVER (PARTITION BY entity ORDER BY timestamp DESC) = 1
), results_all AS (
    SELECT
        ds.value AS demographic_segment,
        -- Number of customers who shopped said department by segment
        COUNT(DISTINCT stl.dw_loyalty_flybuys_account_id) AS n_customers,---trxs
        SUM(stl.total_exclude_gst_amount) AS sales,--sales
         count(distinct stl.dw_sales_transaction_id) AS total_trx,
          sum(sales_quantity) as total_units,
          count(distinct i.item_number) as num_items_purchased
        

    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
    INNER JOIN bdwprd_cds.item.item_dim i
        ON stl.dw_item_id = i.dw_item_id
    RIGHT JOIN bdwprd_cds.loyalty_master.loyalty_flybuys_account_dim fa
        ON stl.dw_loyalty_flybuys_account_id = fa.dw_loyalty_flybuys_account_id
    RIGHT JOIN dem_seg ds
        ON fa.flybuys_membership_number_hash = ds.entity
        inner join BDWPRD_CDS.LOCATION.LOCATION_DIM loc
        on loc.dw_location_id=stl.dw_location_id
    WHERE 1=1
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.country_code = 'AU'
        --and flybuys_cardholder_hash is not null
        AND stl. transaction_date BETWEEN {start_date} AND {end_date}
    GROUP BY demographic_segment
    ORDER BY
        demographic_segment DESC
-- Calculate total segment sizes for context in results
), segment_sizes AS (
    SELECT
        value AS demographic_segment,
        COUNT(*) AS total_segment_size,
        sum(case when age<35 then 1 else 0 end) as U35seg
        
    FROM dem_seg
    GROUP BY ALL
)
SELECT
    ra.demographic_segment,
   -- r.age,
    ss.total_segment_size,
    ss.U35seg,
    ra.sales as sales,
    ra.n_customers AS n_customers,
    ra.total_trx as total_trx,
    ra.total_units as total_units,
    ra.num_items_purchased
    
FROM  results_all ra
left join segment_sizes ss
    ON ra.demographic_segment = ss.demographic_segment
ORDER BY
    ra.demographic_segment
    DESC