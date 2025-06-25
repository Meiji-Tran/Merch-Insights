# Set up connection to azure storage

connectToADLS = function(){
  
  AZURE_CLIENT_SECRET=Sys.getenv("AZURE_CLIENT_SECRET")
  AZURE_TENANT_ID=Sys.getenv("AZURE_TENANT_ID")
  AZURE_CLIENT_ID=Sys.getenv("AZURE_CLIENT_ID")
  token <- AzureAuth::get_azure_token("https://storage.azure.com",
                                      tenant=AZURE_TENANT_ID,
                                      app=AZURE_CLIENT_ID,
                                      password=AZURE_CLIENT_SECRET)
  AZURE_STORAGE_ACCOUNT_URL = Sys.getenv("AZURE_STORAGE_ACCOUNT_URL")
  ad_endp_tok2 <- storage_endpoint(AZURE_STORAGE_ACCOUNT_URL, token=token)
  cont <- storage_container(ad_endp_tok2, "reference")
  
  return(cont)
  
}

writecsvToSnowflake = function(location, stage_csv_name , snowflakeTableName){
  
  temp <- read.csv(location)
  
  up2 = storage_upload(connectToADLS(), location, paste0("sandpit/staging/",stage_csv_name,".csv"))
  
  datMeta = as.data.frame(sapply(temp, class))
  datMeta$ColumnName = row.names(datMeta)
  names(datMeta)[1] = "format"
  row.names(datMeta) = 1:nrow(datMeta)
  
  datMeta = datMeta[,c("ColumnName","format")]
  
  datMeta$cleanedFormat = ifelse(datMeta$format == "character","string","varchar (1024)")
  datMeta$outToSQL = paste0(datMeta$ColumnName, " " ,datMeta$cleanedFormat)
  
  chk = paste0(datMeta[,c("outToSQL")], collapse = '', sep = " ,")
  chk1 = substr(chk,1,nchar(chk)-1)
  
  
  rs <- dbGetQuery(con,paste0("drop table if exists DSS_PRIVATE_DESIGN.ADHOC.",snowflakeTableName," (
      );"))
  rs <- dbGetQuery(con,paste0("create table DSS_PRIVATE_DESIGN.ADHOC.",snowflakeTableName," (
        ",chk1,");"))
  
  
  rs2 <- dbGetQuery(con,paste0("COPY INTO DSS_PRIVATE_DESIGN.ADHOC.",snowflakeTableName,"
        FROM 'azure://bgldnashrsydstaedpde.blob.core.windows.net/reference/sandpit/staging/",stage_csv_name,".csv'
        storage_integration = SHP_DSS_PRIVATE_DESIGN_ADLS_STORAGE_INTG
       FILE_FORMAT = (TYPE = CSV 
       FIELD_DELIMITER = ','
       SKIP_HEADER = 1)
       TRUNCATECOLUMNS = TRUE
       ON_ERROR = CONTINUE;"))
  
  return(NULL)
  
}