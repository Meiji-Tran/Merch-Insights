message('Loading libraries/sourcing...')
suppressMessages({
  library(stringdist)
  library(stringi)
  library(dplyr)
  library(readxl)
  library(writexl)
  library(openxlsx) # to apply styles
  library(data.table)
  library(DT)
  library(DBI)
  library(stringr)
  library(assertthat)
  library(Hmisc)   # escapeRegex()
  library(tictoc)
  library(ngram)
  library(hunspell)
  library(AzureStor)
  library(arrow)  # write_parquet()
  library(parallel)  #mclapply()
  library(glue)
  library(prophet)
})



source('utils/utilsDB.R')
source('utils/utilsUnicode.R')
source('utils/get_snowflake_con.R')


CONFIG = list(
  SCHEMA                   = 'BDWPRD_DE.IA_MERCH_DE',
  SNOWFLAKE_ROLE           = 'INSIGHT_ANALYST_MERCH_DE_GENERAL_PRD',
  SNOWFLAKE_WAREHOUSE      = 'INSIGHT_ANALYST_WH'
  
)

con_sf = get_snowflake_con(CONFIG)


current_ph =  setDT(dbGetQuery(con_sf, paste0("
             
select count(*) as obs
from
bdwprd_cds.item.item_dim
where country_code = 'AU'

;      
 ")))




# table_id <- Id(database="BDWPRD_DE", schema='AD_MERCH_DE', table='PSP_ADHOC_20241024_REFRESH')
# system.time(dbWriteTable(con_sf,table_id, append_10, overwrite = TRUE,batch_rows=10000))


