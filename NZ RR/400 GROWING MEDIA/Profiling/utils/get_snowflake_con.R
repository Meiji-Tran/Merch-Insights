#download https://github.com/Bunnings-Data-and-Analytics/utils-get-snowflake-token/blob/main/get_snowflake_token.py
#then in terminal:
#pip install snowflake
#python get_snowflake_token.py  --role DATA_SCIENTIST_MERCH_GENERAL_PRD

library(DBI)
library(odbc)
library(data.table)
library(assertthat)
library(stringr)




get_snowflake_con = function(CONFIG)
{
  Sys.setenv(SNOWFLAKE_OKTA_ROLE = CONFIG$SNOWFLAKE_ROLE)
  
  pw = readline(str_c("Enter OKTA password for ", Sys.getenv("USER"), ": "))
  ret = system('python utils/get_snowflake_token.py --scope "SESSION:ROLE-ANY" --username $USER',
               input = stri_c(pw, '\n'), intern = T, ignore.stderr = T)
  rm(pw)
  #browser()
  tok = grep('access_token', ret, value = T)
  
  if (length(tok) != 1) { print(ret); stop() }
  
  token = str_extract(tok, ': \"(.+)\"', 1)
  message('Token: ', token)
  
  
  con_sf <- dbConnect(
    odbc::odbc(),
    Driver="SnowflakeDSIIDriver",
    Server="bunnings.australia-east.privatelink.snowflakecomputing.com",
    authenticator="oauth",
    token=token,
    role=CONFIG$SNOWFLAKE_ROLE
    #,encoding='UTF-8'
  )
  
  dbGetQuery(con_sf, paste0('use warehouse ', CONFIG$SNOWFLAKE_WAREHOUSE, ';'))
  
  dbGetQuery(con_sf, paste0("ALTER SESSION SET TIMEZONE = 'Australia/Melbourne';"))
  
  return(con_sf)
}








#sample_sql = "select * FROM BDWPRD_CDS.LOCATION.LOCATION_DIM where location_name ilike '%brunswick%'"
#print(snowflake(sample_sql))