-- Consumer purchase paths
-- The parameters are either set to '' or '-- ', depending on the granularity
-- of the query and whether that line should be included or not

-- Find customers who purhcased sinks and the first date of purchase (>95% purchased once or purchased on the same date)
WITH base AS (
    SELECT
        dw_loyalty_flybuys_account_id,
        min(transaction_date) as first_purchase_date
    From bdwprd_cds.sales.sales_transaction_line_fct stl
        INNER JOIN bdwprd_cds.item.item_dim i
            ON i.dw_item_id = stl.dw_item_id
    where 1=1
    and i.item_sub_department_name = '400 GROWING MEDIA'
    group by dw_loyalty_flybuys_account_id
-- What did these customers purchase, centring time around their project start date
), base_purchase_track AS (
    SELECT
        i.item_category_name,
        i.item_department_name,
        {pp_sub_dept}i.item_sub_department_name,
        {pp_class}i.item_class_name,
        {pp_sub_class}i.item_sub_class_name,
        DATEDIFF(WEEK, b.first_purchase_date, stl.transaction_date) AS weeks_since_purchase,
        (SELECT COUNT(DISTINCT dw_loyalty_flybuys_account_id) FROM base)  AS irrigation_projects_consumer,
        COUNT(DISTINCT stl.dw_loyalty_flybuys_account_id) AS customers,
        SUM(stl.total_include_gst_amount) AS sales
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
        INNER JOIN bdwprd_cds.item.item_dim i
            ON i.dw_item_id = stl.dw_item_id
        INNER JOIN base b
        	ON b.dw_loyalty_flybuys_account_id = stl.dw_loyalty_flybuys_account_id
    WHERE 1=1
        AND stl.country_code = 'AU'
        AND stl.sales_reporting_include_ind = TRUE
        AND transaction_date BETWEEN $start_date AND $end_date
        AND stl.dw_loyalty_flybuys_account_id != MD5_BINARY(-1)
    	AND DATEDIFF(WEEK, b.first_purchase_date, stl.transaction_date) >= -26
    	AND DATEDIFF(WEEK, b.first_purchase_date, stl.transaction_date) <= 26
        AND i.item_department_name != '.Unk'
    GROUP BY
    	i.item_category_name,
        i.item_department_name,
        {pp_sub_dept}i.item_sub_department_name,
        {pp_class}i.item_class_name,
        {pp_sub_class}i.item_sub_class_name,
        weeks_since_purchase
-- Get customer counts separately
), weekly_customer_counts AS (
    SELECT
    	DATEDIFF(WEEK, b.first_purchase_date, stl.transaction_date) AS weeks_since_purchase,
        COUNT(DISTINCT stl.dw_loyalty_flybuys_account_id) AS total_customers
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
        INNER JOIN bdwprd_cds.item.item_dim i
            ON i.dw_item_id = stl.dw_item_id
        INNER JOIN base b
        	ON b.dw_loyalty_flybuys_account_id = stl.dw_loyalty_flybuys_account_id
    WHERE 1=1
        AND stl.country_code = 'AU'
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.transaction_date BETWEEN $start_date AND $end_date
        AND stl.dw_loyalty_flybuys_account_id != MD5_BINARY(-1)
        AND i.item_department_name != '.Unk'
    GROUP BY
    	weeks_since_purchase
    ORDER BY
    	weeks_since_purchase
-- Calculate the usual sales/customer counts at each PFT level for indexing
), shares AS (
    SELECT
    	i.item_category_name,
        i.item_department_name,
        {pp_sub_dept}i.item_sub_department_name,
        {pp_class}i.item_class_name,
        {pp_sub_class}i.item_sub_class_name,
        COUNT(DISTINCT stl.dw_loyalty_flybuys_account_id) AS customers,
        SUM(stl.total_include_gst_amount) AS sales,

        SUM(sales) OVER () AS total_sales,

        sales / total_sales AS sales_share
    FROM bdwprd_cds.sales.sales_transaction_line_fct stl
        INNER JOIN bdwprd_cds.item.item_dim i
            ON i.dw_item_id = stl.dw_item_id
    WHERE 1=1
        AND stl.country_code = 'AU'
        AND stl.sales_reporting_include_ind = TRUE
        AND stl.transaction_date BETWEEN $start_date AND $end_date
        AND stl.dw_loyalty_flybuys_account_id != MD5_BINARY(-1)
        AND i.item_department_name != '.Unk'
    GROUP BY
        i.item_department_name,
        {pp_sub_dept}i.item_sub_department_name,
        {pp_class}i.item_class_name,
        {pp_sub_class}i.item_sub_class_name,
        i.item_category_name
-- Usual shares to use for comparison when indexing
), full_shares AS (
    SELECT
        *,
        (SELECT COUNT(flybuys_cardholder_hash) FROM bdwprd_cds.loyalty_master.loyalty_flybuys_account_dim) AS total_flybuys_customers,
        customers / total_flybuys_customers AS customer_share
    FROM shares
-- Purchase path results
), results AS (
    SELECT
        bpt.item_category_name,
        bpt.item_department_name,
        {pp_sub_dept}bpt.item_sub_department_name,
        {pp_class}bpt.item_class_name,
        {pp_sub_class}bpt.item_sub_class_name,
        bpt.weeks_since_purchase,
        bpt.irrigation_projects_consumer,

        bpt.customers,
        bpt.sales,

        wcc.total_customers,
        SUM(bpt.sales) OVER (PARTITION BY bpt.weeks_since_purchase) AS total_sales,

        bpt.customers / wcc.total_customers AS customer_share,
        bpt.sales / total_sales AS sales_share,

        s.customer_share AS base_customer_share,
        s.sales_share AS base_sales_share

    FROM base_purchase_track bpt
    INNER JOIN full_shares s ON 1=1
    	AND s.item_category_name = bpt.item_category_name
        AND s.item_department_name = bpt.item_department_name
        {pp_sub_dept}AND s.item_sub_department_name = bpt.item_sub_department_name
        {pp_class}AND s.item_class_name = bpt.item_class_name
        {pp_sub_class}AND s.item_sub_class_name = bpt.item_sub_class_name
        AND s.item_category_name IS NOT NULL
    INNER JOIN weekly_customer_counts wcc
    	ON wcc.weeks_since_purchase = bpt.weeks_since_purchase
    ORDER BY
    	weeks_since_purchase
)
-- Final results with rankings
SELECT
	item_category_name,
    item_department_name,
    {pp_sub_dept}item_sub_department_name,
    {pp_class}item_class_name,
    {pp_sub_class}item_sub_class_name,

    weeks_since_purchase,
    irrigation_projects_consumer,

    customers,
    sales,

    total_customers,
    total_sales,

    customer_share,
    sales_share,

    base_customer_share,
    base_sales_share,

    DIV0(customer_share, base_customer_share) AS customer_index,
    DIV0(sales_share, base_sales_share) AS sales_index,

    DENSE_RANK () OVER (PARTITION BY weeks_since_purchase ORDER BY customer_index desc) AS total_customers_rank,
    DENSE_RANK () OVER (PARTITION BY weeks_since_purchase ORDER BY sales_index desc) AS sales_rank,

    DENSE_RANK () OVER (PARTITION BY weeks_since_purchase ORDER BY customers desc) AS total_customers_raw_rank,
    DENSE_RANK () OVER (PARTITION BY weeks_since_purchase ORDER BY sales desc) AS sales_raw_rank
FROM results
WHERE customers >= 20
ORDER BY
	weeks_since_purchase,
    sales_index DESC
;
