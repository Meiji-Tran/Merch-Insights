
CREATE OR REPLACE TEMP TABLE {temp_schema_path}.SALES_TRANSACTION cluster by (transaction_month, transaction_date, primary_id, secondary_id) AS (
    WITH    RELEVANT_TRX AS (select 
        DISTINCT dw_sales_transaction_id
        FROM bdwprd_cds.sales.sales_transaction_line_fct stl
            INNER JOIN bdwprd_cds.item.item_dim i
                ON stl.dw_item_id = i.dw_item_id
        WHERE 1=1
                AND stl.country_code = 'NZ'
                AND stl.sales_reporting_include_ind = TRUE
                AND stl.transaction_date BETWEEN '{start_date}' AND '{end_date}'
				and i.item_sub_department_name = '400 GROWING MEDIA'
    )
	SELECT DISTINCT
		dw_sales_transaction_id,
		CASE
			WHEN customer_type_code='Consumer' AND dw_loyalty_flybuys_account_id <> md5_binary(-1) THEN 'Consumer_flybuys'
			WHEN customer_type_code='Consumer' AND dw_loyalty_flybuys_account_id = md5_binary(-1) THEN 'Consumer_no_flybuys'
			WHEN customer_type_code='Commercial' AND dw_commercial_account_id <> md5_binary(-1) THEN 'Commercial'
		END AS customer_type,
		CASE
			WHEN customer_type = 'Consumer_flybuys' THEN  dw_loyalty_flybuys_account_id
			WHEN customer_type = 'Consumer_no_flybuys' THEN NULL
			WHEN customer_type = 'Commercial' THEN dw_commercial_account_id
		END AS dw_loyalty_id,
		transaction_date,
		date_trunc('month', transaction_date) AS transaction_month,
		p1.{primary_pft}::VARCHAR  AS primary_id,
        p2.{secondary_pft}::VARCHAR  AS secondary_id,
		stl.country_code
	FROM bdwprd_cds.sales.sales_transaction_line_fct stl
	join {pft_table} p1 on 1 = 1
		AND p1.dw_item_id = stl.dw_item_id
	join {pft_table} p2 on 1 = 1
		AND p2.dw_item_id = stl.dw_item_id
	WHERE 1 = 1
	    AND stl.transaction_date >= dateadd('day', - {win_size}, '{start_date}')
		AND stl.transaction_date <  dateadd('day', {win_size}, '{end_date}')
		AND stl.sales_quantity > 0
		AND stl.sales_type_code ='Sale'
		AND sales_reporting_include_ind = TRUE
		AND customer_type IS NOT NULL
		AND primary_id IS NOT NULL
		AND primary_id <> '.Unk'
		AND secondary_id IS NOT NULL
		AND secondary_id <> '.Unk'
        AND dw_sales_transaction_id IN (SELECT dw_sales_transaction_id FROM RELEVANT_TRX)
);

-- delete existing data in the result table for given period
DELETE FROM {association_table}
WHERE
		transaction_month >= '{start_date}'
		AND transaction_month < '{end_date}'
		AND  model_name = '{model_name}'
		AND  model_type	 = '{model_type}'
		AND {is_ao_run};

select count(*) from  {temp_schema_path}.SALES_TRANSACTION