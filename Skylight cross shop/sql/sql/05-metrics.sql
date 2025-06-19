-- Generate metrics for models
CREATE OR REPLACE TEMP TABLE {temp_schema_path}.pair_aggregation AS(
	SELECT
		primary_id,
		secondary_id,
		model_name,
		customer_type,
		model_type,
		win_size,
		country_code,
		MIN(transaction_month) AS trx_start_month,
		MAX(transaction_month) AS trx_end_month,
		COUNT(DISTINCT CONCAT_WS('|', transaction_month, primary_id, secondary_id, customer_type, country_code)) AS pair_trx --Deduplicate
	FROM {association_table} a
	WHERE  1 = 1
		AND transaction_month  >= '{start_date}'
		AND transaction_month < '{end_date}'
	GROUP BY ALL
);

CREATE OR REPLACE TEMP TABLE {temp_schema_path}.single_primary as (
	WITH 
	distinct_records AS 
	(
		SELECT DISTINCT
			primary_id,
			customer_type,
			model_name,
			model_type,
			win_size,
			transaction_month,
			country_code,
			primary_transactions as primary_trx,
			total_transactions AS total_trx,
		FROM {association_table}
		WHERE  1 = 1
			AND transaction_month  >= '{start_date}'
			AND transaction_month < '{end_date}'
	)
	SELECT 
		primary_id,
		customer_type,
		model_name,
		model_type,
		win_size,
		country_code,
		SUM(primary_trx) as primary_transactions,
		SUM(total_trx) as total_transactions	
	FROM  distinct_records
	GROUP BY ALL
	UNION ALL
	SELECT 
		primary_id,
		'Consumer' AS customer_type,
		model_name,
		model_type,
		win_size,
		country_code,
		SUM(primary_trx) as primary_transactions,
		SUM(total_trx) as total_transactions	
	FROM  distinct_records
	WHERE customer_type IN ('Consumer_flybuys', 'Consumer_no_flybuys')
	GROUP BY ALL
	UNION ALL
	SELECT 
		primary_id,
		'All' AS customer_type,
		model_name,
		model_type,
		win_size,
		country_code,
		SUM(primary_trx) as primary_transactions,
		SUM(total_trx) as total_transactions	
	FROM  distinct_records
	GROUP BY ALL
);

CREATE OR REPLACE TEMP TABLE {temp_schema_path}.single_secondary as (
	WITH 
	distinct_records AS 
	(
		SELECT DISTINCT
			secondary_id,
			customer_type,
			model_name,
			model_type,
			win_size,
			transaction_month,
			country_code,
			secondary_transactions as secondary_trx,
		FROM {association_table}
		WHERE  1 = 1
			AND transaction_month  >= '{start_date}'
			AND transaction_month < '{end_date}'
	)
	SELECT 
		secondary_id,
		customer_type,
		model_name,
		model_type,
		win_size,
		country_code,
		SUM(secondary_trx) as secondary_transactions
	FROM distinct_records
	GROUP BY ALL
	UNION ALL
	SELECT 
		secondary_id,
		'Consumer' AS customer_type,
		model_name,
		model_type,
		win_size,
		country_code,
		SUM(secondary_trx) as secondary_transactions,
	FROM  distinct_records
	WHERE customer_type IN ('Consumer_flybuys', 'Consumer_no_flybuys')
	GROUP BY ALL
	UNION ALL
	SELECT 
		secondary_id,
		'All' AS customer_type,
		model_name,
		model_type,
		win_size,
		country_code,
		SUM(secondary_trx) as secondary_transactions,
	FROM  distinct_records
	GROUP BY ALL
);


CREATE OR REPLACE TEMP TABLE {temp_schema_path}.aggregated_by_customer as (
	WITH main_customer_types AS(
		Select
			p.primary_id,
			p.secondary_id,
			p.customer_type,
			p.model_name,
			p.model_type,
			p.win_size,
			p.country_code,
			p.trx_start_month,
			p.trx_end_month,
			p.pair_trx AS pair_transactions,
		FROM {temp_schema_path}.pair_aggregation p	
	)
	SELECT * FROM main_customer_types
	UNION ALL
	SELECT 
		primary_id,
		secondary_id,
		'Consumer' AS customer_type,
		model_name,
		model_type,
		win_size,
		country_code,
		MIN(trx_start_month) AS trx_start_month,
		MAX(trx_end_month) AS trx_end_month,
		SUM(pair_transactions) AS pair_transactions,
	FROM main_customer_types
	WHERE customer_type IN ('Consumer_flybuys', 'Consumer_no_flybuys')
	-- AND model_type = 'Basket' -- for time-window models Consumer are same as Consumer_flybuys
	GROUP BY ALL
	UNION ALL
	SELECT 
		primary_id,
		secondary_id,
		'All' as customer_type,
		model_name,
		model_type,
		win_size,
		country_code,
		MIN(trx_start_month) AS trx_start_month,
		MAX(trx_end_month) AS trx_end_month,
		SUM(pair_transactions) AS pair_transactions,
	FROM main_customer_types
	GROUP BY ALL
);

CREATE OR REPLACE TABLE {metric_table} CLUSTER BY (country_code, model_name, model_type, primary_id ) AS (
	WITH
	metrics as(
		SELECT
			p.primary_id,
			p.secondary_id,
			p.customer_type,
			p.model_name,
			p.model_type,
			p.win_size,	
			p.country_code,
			p.trx_start_month,
		    p.trx_end_month,
			s1.primary_transactions,
			s2.secondary_transactions,
			p.pair_transactions,
			s1.total_transactions,
			primary_transactions / total_transactions ::FLOAT AS primary_support,
			secondary_transactions / total_transactions ::FLOAT AS secondary_support,
			pair_transactions / total_transactions ::FLOAT AS pair_support,	
			pair_support / primary_support AS confidence,
			pair_support/(primary_support * secondary_support) AS lift,
			pair_support - (primary_support * secondary_support) AS leverage,
		    primary_support * (1.0 - secondary_support) / NULLIFZERO(primary_support - pair_support) AS conviction,
			pair_support / NULLIFZERO(primary_support + secondary_support - pair_support) AS jaccard,
			leverage / NULLIFZERO(GREATEST((pair_support * (1-primary_support)) , (primary_support * (secondary_support - pair_support)) )) AS zhang,
			pair_transactions AS a,
			primary_transactions - a AS b,
			secondary_transactions - a AS c,
			total_transactions - a - b - c AS d,
			((a * d) - (b * c)) / NULLIFZERO((a * d) + (b * c)) AS yulesq,
			NULLIFZERO((a+b) * (a+c) / total_transactions) ::FLOAT AS ae,
			NULLIFZERO((a+b) * (b+d) / total_transactions) ::FLOAT AS be,
			NULLIFZERO((d+c) * (a+c) / total_transactions) ::FLOAT AS ce,
			NULLIFZERO((d+c) * (b+d) / total_transactions) ::FLOAT AS de,
			(pow((a - ae),2) / ae) + (pow((b - be),2) / be) + (pow((c - ce),2) / ce) + (pow((d - de),2) / de) AS chi
	FROM {temp_schema_path}.aggregated_by_customer p
	JOIN {temp_schema_path}.single_primary s1 ON 1 = 1
			AND p.primary_id = s1.primary_id
			AND p.customer_type = s1.customer_type
			AND p.model_type = s1.model_type
			AND p.model_name = s1.model_name
			AND p.country_code = s1.country_code
	JOIN {temp_schema_path}.single_secondary s2 ON 1 = 1
			AND p.secondary_id = s2.secondary_id
			AND p.customer_type = s2.customer_type
			AND p.model_type = s2.model_type
			AND p.model_name = s2.model_name
			AND p.country_code = s2.country_code
	WHERE NOT (p.customer_type = 'Consumer_no_flybuys' AND p.country_code = 'NZ') -- NZ doesn't have flybuys so we exclude this and only keep Consumer
	)
	SELECT 
		primary_id,
		secondary_id,
		customer_type,
		model_type,
		win_size,
		country_code,
		model_name,
		trx_start_month,
		trx_end_month,
		primary_transactions,
		secondary_transactions,
		pair_transactions,
		total_transactions,
		confidence,
		lift,
		leverage,
		conviction,
		jaccard, 
		zhang,
		yulesq,
		chi,
		'{process_ts}' as process_ts
	FROM metrics 
);