select primary_id, secondary_id, primary_transactions,SECONDARY_TRANSACTIONS,TRX_START_MONTH,TRX_END_MONTH,pair_transactions,LIFT,JACCARD 
from BDWPRD_DE.IA_MERCH_DE.PEST_CONTROL_DEPARTMENT_TO_DEPARTMENT_ASSOCIATION_METRICS 
where 1=1
and customer_type = 'All'
and country_code ='NZ'
AND MODEL_TYPE = 'Basket'
QUALIFY ROW_NUMBER() OVER (PARTITION BY concat(primary_id, secondary_id,customer_type,model_type,country_code) ORDER BY TRX_START_MONTH DESC) = 1