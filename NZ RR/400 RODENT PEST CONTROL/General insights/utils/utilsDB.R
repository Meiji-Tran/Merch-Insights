library(DBI)
library(arrow)  # for write_parquet()
library(AzureStor)  # for storage_upload()

#con_sf <- dbConnect(odbc::odbc(), "snowflake", timeout = 10)



LOCAL_TEMP_FILE_DIR = '/tmp/PSP/'

# Set up connection to azure storage

connectToADLS = function(){
  # Get env vars
  azure_client_secret = Sys.getenv("AZURE_CLIENT_SECRET")
  azure_tenant_id = Sys.getenv("AZURE_TENANT_ID")
  azure_client_id = Sys.getenv("AZURE_CLIENT_ID")
  
  if (Sys.getenv('PSP_OVERRIDE_BLOB') == '') AZURE_STORAGE_ACCOUNT_URL = Sys.getenv("AZURE_STORAGE_ACCOUNT_URL")
  else                                       AZURE_STORAGE_ACCOUNT_URL = Sys.getenv('PSP_OVERRIDE_BLOB')
  
  token = AzureAuth::get_azure_token("https://storage.azure.com",
                                     tenant = azure_tenant_id,
                                     app = azure_client_id,
                                     password = azure_client_secret)
  
  ad_endp_tok2 = storage_endpoint(AZURE_STORAGE_ACCOUNT_URL, token = token)
  cont = storage_container(ad_endp_tok2, "design")
  
  return(cont)
}


query_data = function(con, query){
  # works for SF
  return(setDT(dbGetQuery(con, query)))
}


# Define the dbGetQueryFromFile function with param argument
query_data_from_file = function(con, file, params = NULL) {
  # Check if the file exists and is readable
  if (!file.exists(file) || !file.access(file, 4) == 0) {
    stop("File does not exist or is not readable")
  }
  # Read the file content as a character string
  query = readLines(file, warn = FALSE)
  # Remove any comment lines starting with --
  query = query[!grepl("^\\s*--", query)]
  # Collapse the query into a single line
  query = paste(query, collapse = " ")
  # Use glue_sql to interpolate the param values into the query if not null
  # print(query)
  if (!is.null(params)) {
    query = glue_sql(query, .con = con, .envir = params)
  }
  # print(query)
  # Execute the query using dbGetQuery
  return(query_data(con, query))
}



create_dir_if_not_exist = function(dir_path){
  
  if (!dir.exists(dir_path)) {
    # Create the directory if it does not exist
    dir.create(dir_path, recursive = TRUE)
    # Print a message
    message(paste("Directory created:", dir_path))
    
  } else { # be quiet
    #message("Directory already exists:", dir_path)
  }
  
}


upload_datatable_sf = function(con, data, sf_table_name){
  
  # save locally first
  path_src = stri_c(LOCAL_TEMP_FILE_DIR, "/temp.parquet")
  create_dir_if_not_exist(LOCAL_TEMP_FILE_DIR)
  
  message('Writing ', path_src, '...')
  #browser()
  
  # Get column names/types
  col_classes = as.data.frame(sapply(data, function(x) {first(class(x))} ))  # NOTE: this breaks with columns that are binary/blob like DW_ITEM_ID.  Use HEX_ENCODE() when fetching from sf
  col_classes$column_name = row.names(col_classes)
  names(col_classes)[1] = "format"
  row.names(col_classes) = 1:nrow(col_classes)
  col_classes = setDT(col_classes[, c("column_name", "format")])
  
  # Fix for the way snowflake loads timestamps with timezones.  Write as character into parquet, but get snowflake to load as timestamp
  posixct_cols = col_classes[format == 'POSIXct']$column_name
  data2 = data
  data2[, posixct_cols] = lapply(data2[, posixct_cols, with = F], as.character)
  
  write_parquet(data2, path_src)
  
  # write to SF
  write_parquet_to_snowflake(
    con,
    src = path_src,
    stage_filename = "temp_parquet",
    snowflake_table_name = sf_table_name,
    data = data
  )
  
  file.remove(path_src)
  message(paste("Saved", sf_table_name)) 
}





save_azfile = function(con, src, dest, quiet = T){
  # uploads to azure storage
  if (!quiet) { message(paste("Uploading", dest, '...')) }
  
  storage_upload(con, src, dest)
  
  if (!quiet) { message(paste("Uploaded", dest)) }
  
  return(dest)
}




# requires fully qualified table name
write_parquet_to_snowflake = function(con, src, stage_filename, snowflake_table_name, data){
  
  
  if (Sys.getenv('PSP_OVERRIDE_BLOB') == '') AZURE_STORAGE_ACCOUNT_URL = Sys.getenv("AZURE_STORAGE_ACCOUNT_URL")
  else                                       AZURE_STORAGE_ACCOUNT_URL = Sys.getenv('PSP_OVERRIDE_BLOB')
  
  
  message('Writing file to azure (', stage_filename, ')...')
  save_azfile(connectToADLS(), src, stri_c("dataiku/PSP/SNOWFLAKE_STAGING/", stage_filename, ".parquet"), quiet = F) 
  
  
  message('Ingesting file from azure into snowflake... ', appendLF = F)
  
  # Get column names/types
  col_classes = as.data.frame(sapply(data, function(x) {first(class(x))} ))  # NOTE: this breaks with columns that are binary/blob like DW_ITEM_ID.  Use HEX_ENCODE() when fetching from sf
  col_classes$column_name = row.names(col_classes)
  names(col_classes)[1] = "format"
  row.names(col_classes) = 1:nrow(col_classes)
  col_classes = setDT(col_classes[, c("column_name", "format")])
  
  #browser()
  # only support these types.  'integer' and 'Date' are valid snowflake types as well.
  assert_that(nrow(col_classes[format %in% c('character', 'numeric', 'integer', 'Date', 'POSIXct')]) == nrow(col_classes))
  col_classes[, format := ifelse(format == "character", "varchar (1024)", format)]
  col_classes[, format := ifelse(format == "numeric", "double", format)]
  col_classes[, format := ifelse(format == "POSIXct", "timestamp_ntz", format)]  # stores in UTC but displays in local tz
  
  sf_column_list = stri_c(col_classes$column_name, " ", col_classes$format)
  sf_column_list = stri_c(sf_column_list, collapse = ', ')
  
  #browser()
  message('drop ', appendLF = F)
  rs = dbGetQuery(con, stri_c("drop table if exists ", snowflake_table_name, ";"))
  
  message('create ', appendLF = F)
  #browser()
  
  rs = dbGetQuery(con, stri_c("create table ", snowflake_table_name, " (
        ", sf_column_list, ");"))
  
  
  url = sub('https://', 'azure://', AZURE_STORAGE_ACCOUNT_URL, ignore.case = T)
  url = paste0(url, "/design/dataiku/PSP/SNOWFLAKE_STAGING/", stage_filename, ".parquet")
  #FROM 'azure://bgldnashpsydstaedpdsspv1.blob.core.windows.net/design/dataiku/PSP/SNOWFLAKE_STAGING/", stage_filename, ".parquet'
  
  
  message('copy', appendLF = F)
  rs = dbGetQuery(con, stri_c("COPY INTO ", snowflake_table_name, "
        FROM '", url, "'
        storage_integration = SHP_DSS_PRIVATE_DESIGN_ADLS_STORAGE_INTG
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE 
        TRUNCATECOLUMNS = TRUE 
        ON_ERROR = CONTINUE;"))
  
  message('\nFile loaded into ', snowflake_table_name, '.')
  
  return(NULL)
}