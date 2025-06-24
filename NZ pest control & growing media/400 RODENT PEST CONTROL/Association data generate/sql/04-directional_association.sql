
--- calculate directional association data over given window_size. 

CREATE OR REPLACE TEMP TABLE {temp_schema_path}.pair_directional_trx AS (
	SELECT  DISTINCT
			trx1.dw_sales_transaction_id,
			trx1.primary_id,
			trx2.secondary_id,
			trx1.transaction_month,
			trx1.customer_type,
			trx1.country_code
		FROM {temp_schema_path}.sales_transaction  trx1
		INNER JOIN {temp_schema_path}.sales_transaction trx2	ON 1 = 1 
			AND trx1.dw_sales_transaction_id <> trx2.dw_sales_transaction_id 
			AND trx1.customer_type = trx2.customer_type
			AND trx2.transaction_date BETWEEN DATEADD('day', 1 , trx1.transaction_date) AND  DATEADD('day', {win_size} , trx1.transaction_date)
			-- AND (trx1.primary_id <> trx2.primary_id OR trx1.secondary_id <> trx2.secondary_id) 
			AND trx1.dw_loyalty_id = trx2.dw_loyalty_id
			AND trx1.transaction_month >= '{start_date}'
			AND trx1.transaction_month < '{end_date}'
			AND trx1.country_code = trx2.country_code 
		WHERE  trx1.customer_type IN ('Consumer flybuys', 'Commercial')
);


CREATE OR REPLACE TEMP TABLE  {temp_schema_path}.pair_directional  AS (
 	SELECT 
		primary_id,
		secondary_id,
		transaction_month,
		customer_type,
		country_code,
		count(DISTINCT dw_sales_transaction_id) AS pair_transactions
		FROM  {temp_schema_path}.pair_directional_trx
 	GROUP BY all
);

CREATE OR REPLACE TEMP TABLE {temp_schema_path}.single_primary_directional AS (
	SELECT
		primary_id,
		transaction_month,
		customer_type,
		country_code,
		count(DISTINCT dw_sales_transaction_id) AS primary_transactions,
	FROM {temp_schema_path}.sales_transaction 
	GROUP BY all
);

CREATE OR REPLACE TEMP TABLE {temp_schema_path}.single_secondary_directional AS (
	SELECT
		secondary_id,
		transaction_month,
		customer_type,
		country_code,
		count(DISTINCT dw_sales_transaction_id) AS secondary_transactions,
	FROM {temp_schema_path}.sales_transaction 
	GROUP BY all
);

CREATE OR REPLACE TEMP TABLE {temp_schema_path}.total_event AS 	(
		SELECT 
			transaction_month,
			customer_type,
			country_code,
			count(DISTINCT dw_sales_transaction_id) AS  total_transactions,
		FROM {temp_schema_path}.sales_transaction 
		GROUP BY ALL 
	);

INSERT INTO {association_table} (
	SELECT 
		p.primary_id,
		p.secondary_id,
		s1.primary_transactions,
		s2.secondary_transactions,
		p.pair_transactions,
		t.total_transactions,
		p.transaction_month,
		p.customer_type,
		p.country_code,
		'{model_type}' AS model_type,
		'{model_name}' AS model_name,
		'{win_size}' AS time_window_size,
		'{process_ts}' as process_ts,
		'{dw_airflow_dag_run_id}' as dw_airflow_dag_run_id
	FROM {temp_schema_path}.pair_directional p
	JOIN {temp_schema_path}.single_primary_directional s1 ON 1 = 1
		AND p.primary_id = s1.primary_id
		AND p.customer_type = s1.customer_type
		AND p.transaction_month = s1.transaction_month
		AND p.country_code = s1.country_code
	JOIN {temp_schema_path}.single_secondary_directional s2 ON 1 = 1
		AND p.secondary_id = s2.secondary_id
		AND p.customer_type = s2.customer_type
		AND p.transaction_month = s2.transaction_month 	
		AND p.country_code = s2.country_code
	JOIN {temp_schema_path}.total_event t ON 1 = 1
		AND p.transaction_month = t.transaction_month
		AND p.customer_type = t.customer_type 
		AND p.country_code = t.country_code 
);