-- count primary pft
CREATE OR REPLACE TEMP TABLE {temp_schema_path}.basket_primary_count AS
(	
	SELECT  
		primary_id,
		customer_type,
		transaction_month,
		country_code,
		COUNT(DISTINCT dw_sales_transaction_id) AS pft_transactions
	FROM {temp_schema_path}.sales_transaction st
	GROUP BY ALL
);

-- count secondary pft
CREATE OR REPLACE TEMP TABLE {temp_schema_path}.basket_secondary_count AS
(	
	SELECT  
		secondary_id,
		customer_type,
		transaction_month,
		country_code,
		COUNT(DISTINCT dw_sales_transaction_id) AS pft_transactions
	FROM {temp_schema_path}.sales_transaction st
	GROUP BY ALL
);

-- count total transactions
CREATE OR REPLACE TEMP TABLE {temp_schema_path}.basket_total_transactions AS
(	
	SELECT 
		customer_type,
		transaction_month,
		country_code,
		COUNT(DISTINCT dw_sales_transaction_id) AS total_transactions
	FROM {temp_schema_path}.sales_transaction
	GROUP BY ALL
);

-- deduplicated pair join (fixing overcounting)
CREATE OR REPLACE TEMP TABLE {temp_schema_path}.basket_pair_join AS (
	SELECT DISTINCT
		trx1.dw_sales_transaction_id,
		trx1.primary_id,
		trx2.secondary_id,
		trx1.customer_type,
		trx1.transaction_month,
		trx1.country_code
	FROM {temp_schema_path}.sales_transaction trx1
	JOIN {temp_schema_path}.sales_transaction trx2
		ON trx1.dw_sales_transaction_id = trx2.dw_sales_transaction_id
		AND trx1.primary_id <> trx2.secondary_id
);

-- count distinct trx for each (primary, secondary) pair
CREATE OR REPLACE TEMP TABLE {temp_schema_path}.basket_pair_transaction AS (
	SELECT  
		primary_id,
		secondary_id,
		customer_type,
		transaction_month,
		country_code,
		COUNT(DISTINCT dw_sales_transaction_id) AS pair_transactions
	FROM {temp_schema_path}.basket_pair_join
	GROUP BY ALL
);

-- insert into final association table
INSERT INTO {association_table} (
	SELECT 			
		p.primary_id,
		p.secondary_id,
		s1.pft_transactions AS primary_transactions,
		s2.pft_transactions AS secondary_transactions,
		p.pair_transactions,
		t.total_transactions,
		p.transaction_month,
		p.customer_type,
		p.country_code,
		'{model_type}' AS model_type,
		'{model_name}' AS model_name,
		'{win_size}' AS win_size,
		'{process_ts}' AS process_ts,
		'{dw_airflow_dag_run_id}' AS dw_airflow_dag_run_id
	FROM {temp_schema_path}.basket_pair_transaction p
	JOIN {temp_schema_path}.basket_primary_count s1 ON
		p.primary_id = s1.primary_id
		AND p.customer_type = s1.customer_type
		AND p.transaction_month = s1.transaction_month
		AND p.country_code = s1.country_code
	JOIN {temp_schema_path}.basket_secondary_count s2 ON
		p.secondary_id = s2.secondary_id
		AND p.customer_type = s2.customer_type
		AND p.transaction_month = s2.transaction_month
		AND p.country_code = s2.country_code
	JOIN {temp_schema_path}.basket_total_transactions t ON
		p.transaction_month = t.transaction_month
		AND p.customer_type = t.customer_type
		AND p.country_code = t.country_code
);