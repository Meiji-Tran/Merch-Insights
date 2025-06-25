###################### Loading library & functions ######################
#########################################################################

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
  library(lubridate)
  library(ggplot2)
  library(tidyr)
  library(glue)
  library(dplyr)
  library(tidyr)
  library(ggplot2)
  library(scales)
  
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

read_data <- function(sql_file, params = list(), parquet_path, con) {
  if (file.exists(parquet_path)) {
    return(setDT(read_parquet(parquet_path)))
  } else {
    # Read and interpolate SQL query
    query <- glue(paste(readLines(sql_file), collapse = "\n"),
                  .envir = list2env(params, parent = environment()))
    
    result <- setDT(dbGetQuery(con, query))
    write_parquet(result, parquet_path)
    return(result)
  }
}

########################### Setting Parameters ##########################
#########################################################################
end_date<-"'2025-04-30'"
start_date<-"'2024-05-01'"
dept = "'300 LIGHTING'"
min_seg_size<-5000
save_output = FALSE

# below are parameters specific for the lighting insight
additional_trx_condition = 
"and ITEM_CLASS_NAME not in (
    '503 IND LIGHTING INSTALLATION',
    '502 FAN INSTALLATION'
    )
    "

decorative_condition = "
and ITEM_CLASS_NAME in (
    '502 DECORATIVE',
    '502 G SERIES',
    '502 FLOOR LAMPS',
    '502 MIX AND MATCH LAMPS',
    '502 MIX AND MATCH SHADES',
    '502 TABLE LAMPS',
    '502 CHANDELIER',
    '502 COMPLETE 240V PENDANT',
    '502 DIY PENDANT',
    '502 SUSPENSION',
    '502 SOLAR DECORATIVE')
"

functional_condition = "
and ITEM_CLASS_NAME not in (
    '502 DECORATIVE',
    '502 G SERIES',
    '502 FLOOR LAMPS',
    '502 MIX AND MATCH LAMPS',
    '502 MIX AND MATCH SHADES',
    '502 TABLE LAMPS',
    '502 CHANDELIER',
    '502 COMPLETE 240V PENDANT',
    '502 DIY PENDANT',
    '502 SUSPENSION',
    '502 SOLAR DECORATIVE')
"
conditions <- list(
  "Decorative" = decorative_condition,
  "Functional" = functional_condition,
  "Overall" = ""
)






############################# Sales Overview ############################
#########################################################################


### 1 - by brand
decorative_brand       <- read_data("sql/Overall_by_brand.sql", list(target_item_condition = decorative_condition), "Parquet/decorative_brand.parquet", con_sf)
functional_brand        <- read_data("sql/Overall_by_brand.sql", list(target_item_condition = functional_condition), "Parquet/functional_brand.parquet", con_sf)
overall_brand           <- read_data("sql/Overall_by_brand.sql", list(target_item_condition = ""), "Parquet/overall_brand.parquet", con_sf)

decorative_con_vs_comm <- read_data("sql/Overall_consumer_vs_commercial.sql", list(target_item_condition = decorative_condition), "Parquet/decorative_con_vs_comm.parquet", con_sf)
functional_con_vs_comm <- read_data("sql/Overall_consumer_vs_commercial.sql", list(target_item_condition = functional_condition), "Parquet/functional_con_vs_comm.parquet", con_sf)
Overall_con_vs_comm    <- read_data("sql/Overall_consumer_vs_commercial.sql", list(target_item_condition = ""), "Parquet/overall_con_vs_comm.parquet", con_sf)


dfs <- list(
  decorative_con_vs_comm = decorative_con_vs_comm,
  functional_con_vs_comm = functional_con_vs_comm,
  Overall_con_vs_comm = Overall_con_vs_comm
)

dfs <- lapply(dfs, function(df) {
  df %>%
    mutate(
      LOOKUP_VALUE = paste0(CUSTOMER_TYPE, SEGMENT),
      SALES_PER_TRX = SALES / TOTAL_TRX,
      SALES_PER_ACTIVE_CUST = SALES / N_CUSTOMERS,
      TRX_PER_ACTIVE_CUST = TOTAL_TRX / N_CUSTOMERS,
      UNITS_PER_TRX = TOTAL_UNITS / TOTAL_TRX,
      PRICE_PER_ITEM = SALES / TOTAL_UNITS
    ) %>%
    relocate(LOOKUP_VALUE, .before = 1)
})

decorative_con_vs_comm <- dfs$decorative_con_vs_comm
functional_con_vs_comm <- dfs$functional_con_vs_comm
Overall_con_vs_comm    <- dfs$Overall_con_vs_comm

rm(dfs)

### 2 - by pft

overall <- read_data("sql/Overall_by_pft.sql", list(), "Parquet/overall_by_pft.parquet", con_sf)

overall$SALES_PER_TRX<-overall$SALES/overall$TOTAL_TRX
overall$SALES_PER_ACTIVE_CUST<-overall$SALES/overall$N_CUSTOMERS
overall$TRX_PER_ACTIVE_CUST<-overall$TOTAL_TRX/overall$N_CUSTOMERS
overall$UNITS_PER_TRX<-overall$TOTAL_UNITS/overall$TOTAL_TRX
overall$PRICE_PER_ITEM<-overall$SALES/overall$TOTAL_UNITS

# write output file
if (save_output) {
  wb <- createWorkbook()
  addWorksheet(wb, "by_brand")
  writeData(wb, "by_brand", overall_brand)
  addWorksheet(wb, "decorative_by_brand")
  writeData(wb, "decorative_by_brand", decorative_brand)
  addWorksheet(wb, "functional_by_brand")
  writeData(wb, "functional_by_brand", functional_brand)
  addWorksheet(wb, "by_pft")
  writeData(wb, "by_pft", overall)
  addWorksheet(wb, "by_customer")
  writeData(wb, "by_customer", Overall_con_vs_comm)
  addWorksheet(wb, "decorative_by_customer")
  writeData(wb, "decorative_by_customer", decorative_con_vs_comm)
  addWorksheet(wb, "functional_by_customer")
  writeData(wb, "functional_by_customer", functional_con_vs_comm)
  dir.create("Output", showWarnings = FALSE)
  saveWorkbook(wb, "Output/overall_metrics.xlsx", overwrite = TRUE)
}






##################### Electrical brand cross shop #########################
#####################(specific request for lighting)#######################

brand_xshop_cust_type = "'Commercial'"  # change this variable for different customers

if (brand_xshop_cust_type == "'Consumer'"){
  id_col = "dw_loyalty_flybuys_account_id"
} else if (brand_xshop_cust_type == "'Commercial'") {
  id_col <- "dw_commercial_account_id"
}
brands = c("'%ARLEC%'","'%BRILLIANT%'","'%PHIL%IPS%'","'%OSRAM%'","'%CLICK%'","'%DETA%'")
brand_names = c('Arlec','Brilliant','Philips','Osram','Click','Deta')
brand_results = list()

for (i in seq_along(brands)) {
  target_brand = brands[i]
  brand_name = brand_names[i]
  df <- read_data("sql/brand_cross_shop.sql", list(), paste0("Parquet/brand_cross_shop_",brand_xshop_cust_type,"_",brand_name,".parquet"), con_sf)
  brand_results[[brand_name]] = df
}

wb <- createWorkbook()
for (name in names(brand_results)) {
  addWorksheet(wb, name)
  writeData(wb, sheet = name, brand_results[[name]])
}
saveWorkbook(wb, file = paste0("Output/brand_cross_shop_", brand_xshop_cust_type, ".xlsx"), overwrite = TRUE)






############################# Consumer view ###########################
#######################################################################


### 1 - All consumer sales by segments
all_con_sales =  read_data("sql/All_consumer_sales.sql", list(), "Parquet/all_consumer_sales.parquet", con_sf)

all_con_sales$SALES_PER_TRX<-all_con_sales$SALES/all_con_sales$TOTAL_TRX
all_con_sales$SALES_PER_ACTIVE_CUST<-all_con_sales$SALES/all_con_sales$N_CUSTOMERS
all_con_sales$TRX_PER_ACTIVE_CUST<-all_con_sales$TOTAL_TRX/all_con_sales$N_CUSTOMERS
all_con_sales$UNITS_PER_TRX<-all_con_sales$TOTAL_UNITS/all_con_sales$TOTAL_TRX
all_con_sales$PRICE_PER_ITEM<-all_con_sales$SALES/all_con_sales$TOTAL_UNITS


### 2 - Target consumer sales by segments
wb <- createWorkbook()
merged_data_list <- list()

for (name in names(conditions)) {
  segments <-  read_data("sql/Consumer_segment_sales.sql", list(target_item_condition = conditions[[name]]), glue("Parquet/all_consumer_sales_{name}.parquet"), con_sf)

  merged_data <- merge(all_con_sales, segments, by = "DEMOGRAPHIC_SEGMENT", suffixes = c("_ALL", "_TARGET"))
  
  merged_data$TOTAL_SALES_ALL <- sum(merged_data$SALES_ALL, na.rm = TRUE)
  merged_data$TOTAL_SALES_TARGET <- sum(merged_data$SALES_TARGET, na.rm = TRUE)
  merged_data$TARGET_SHARE <- merged_data$TOTAL_SALES_TARGET / merged_data$TOTAL_SALES_ALL
  
  merged_data$SEGMENT_TARGET_SHARE <- merged_data$SALES_TARGET / merged_data$SALES_ALL
  merged_data$SALES_INDEX_SEGMENT <- merged_data$SEGMENT_TARGET_SHARE / merged_data$TARGET_SHARE
  merged_data$SALES_PER_TRX_TARGET <- merged_data$SALES_TARGET / merged_data$TOTAL_TRX_TARGET
  merged_data$SALES_PER_TRX_TARGET_GST <- merged_data$SALES_PER_TRX_TARGET * 1.1
  
  merged_data_list[[name]] <- merged_data
  # Add a worksheet and write the data
  addWorksheet(wb, sheetName = name)
  writeData(wb, sheet = name, x = merged_data)
}

if (save_output) {
  saveWorkbook(wb, "Output/Consumer_segment_index.xlsx", overwrite = TRUE)
}


### 3 - consumer sales by brand and segment (for heatmap)
con_seg_brand =  read_data("sql/Consumer_segments_by_brand.sql", list(target_item_condition = conditions[['Overall']]), glue("Parquet/consumer_segments_by_brand.parquet"), con_sf)

##details on segments
con_seg_brand$SALES_PER_TRX<-con_seg_brand$SALES/con_seg_brand$TOTAL_TRX
con_seg_brand$SALES_PER_ACTIVE_CUST<-con_seg_brand$SALES/con_seg_brand$N_CUSTOMERS
con_seg_brand$TRX_PER_ACTIVE_CUST<-con_seg_brand$TOTAL_TRX/con_seg_brand$N_CUSTOMERS
con_seg_brand$UNITS_PER_TRX<-con_seg_brand$TOTAL_UNITS/con_seg_brand$TOTAL_TRX
con_seg_brand$PRICE_PER_ITEM<-con_seg_brand$SALES/con_seg_brand$TOTAL_UNITS
con_seg_brand$SALES_PER_TRX_GST<-con_seg_brand$SALES_PER_TRX+con_seg_brand$SALES_PER_TRX*0.1

con_seg_brand <- con_seg_brand[!grepl("^unclassifiable", con_seg_brand$DEMOGRAPHIC_SEGMENT), ]

# Calculate total sales per brand across all segments (overall brand share)
overall_brand_sales <- con_seg_brand %>%
  group_by(BRAND_NAME) %>%
  summarise(OVERALL_BRAND_SALES = sum(SALES, na.rm = TRUE))

total_class_sales <- sum(overall_brand_sales$OVERALL_BRAND_SALES, na.rm = TRUE)

overall_brand_sales <- overall_brand_sales %>%
  mutate(OVERALL_BRAND_SHARE = OVERALL_BRAND_SALES / total_class_sales)

# Calculate brand share within each segment
segment_brand_sales <- con_seg_brand %>%
  group_by(DEMOGRAPHIC_SEGMENT, BRAND_NAME) %>%
  summarise(SEGMENT_BRAND_SALES = sum(SALES, na.rm = TRUE), .groups = "drop")

segment_totals <- con_seg_brand %>%
  group_by(DEMOGRAPHIC_SEGMENT) %>%
  summarise(SEGMENT_TOTAL_SALES = sum(SALES, na.rm = TRUE), .groups = "drop")

# Join and calculate segment brand share
segment_brand_share <- segment_brand_sales  %>%
  left_join(segment_totals, by = "DEMOGRAPHIC_SEGMENT") %>%
  left_join(overall_brand_sales, by = "BRAND_NAME") %>%
  mutate(SEGMENT_BRAND_SHARE = SEGMENT_BRAND_SALES / OVERALL_BRAND_SALES,BRAND_SEGMENT_SHARE = SEGMENT_BRAND_SALES / SEGMENT_TOTAL_SALES ) %>% select(-OVERALL_BRAND_SHARE,-OVERALL_BRAND_SALES)

brand_totals <- con_seg_brand %>%
  group_by(BRAND_NAME) %>%
  summarise(TOTAL_BRAND_SALES = sum(SALES, na.rm = TRUE)) %>%
  mutate(SHARE_LABEL = paste0(BRAND_NAME, ' (', percent(TOTAL_BRAND_SALES / sum(TOTAL_BRAND_SALES), accuracy = 0.1), ')'))

# Join with overall brand share and calculate index
segments_indexed <- segment_brand_share %>%
  left_join(overall_brand_sales, by = "BRAND_NAME") %>%
  left_join(brand_totals, by = "BRAND_NAME") %>%
  mutate(
    SALES_INDEX = BRAND_SEGMENT_SHARE / OVERALL_BRAND_SHARE,
    SHARE_LABEL = paste0(BRAND_NAME, ' (', percent(TOTAL_BRAND_SALES / total_class_sales, accuracy = 0.1), ')'),
    ANNOTATION = percent(SEGMENT_BRAND_SHARE, accuracy = 0.1)
  )

if (save_output) {
  write_csv(segments_indexed, "Output/consumer_brand_index.csv")
}

### 4- Consumer brand index heatmap 
top_segments <- segments_indexed %>%
  group_by(DEMOGRAPHIC_SEGMENT) %>%
  summarise(TOTAL_SEGMENT_SALES = sum(SEGMENT_BRAND_SALES, na.rm = TRUE)) %>%
  pull(DEMOGRAPHIC_SEGMENT)


filtered_data <- segments_indexed %>%
  filter(
    DEMOGRAPHIC_SEGMENT %in% top_segments,
    OVERALL_BRAND_SHARE > 0.2/100, # keeping brands with sales >0.2% otherwise graph is too busy
    BRAND_NAME != "NA"
  ) %>%
  mutate(SALES_INDEX = pmin(SALES_INDEX, 2.5))  # cap index at 2.5

# Order brands by total sales
brand_order <- brand_totals %>%
  arrange(desc(TOTAL_BRAND_SALES)) %>%
  pull(SHARE_LABEL)

# Plot heatmap with segment share per brand as annotation
ggplot(filtered_data, aes(x = factor(SHARE_LABEL, levels = brand_order),
                          y = DEMOGRAPHIC_SEGMENT,
                          fill = SALES_INDEX)) +
  geom_tile(color =  "white") +
  geom_text(aes(label = ANNOTATION), size = 3) +  scale_y_discrete(label = function(x) str_wrap(str_trunc(x, 20), width = 30))+
  scale_fill_gradient2(low = "white", high = "red", midpoint = 1) +
  theme_minimal() +
  labs(
    title = "Indexed Sales Performance by Brand and Top Demographic Segments",
    x = "Brand (Total Share)",
    y = "Demographic Segment",
    fill = "Sales Index"
  ) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)
  )




############################# Commercial view ###########################
#########################################################################


### 1 - All Commercial sales by segments
comm_all_sales = read_data("sql/All_commercial_sales.sql", list(target_item_condition = conditions[[name]]), glue("Parquet/all_commercial_sales.parquet"), con_sf)

### 2 - Target Commercial sales by segments

wb <- createWorkbook()
merged_comm_data_list <- list()

for (name in names(conditions)) {
  comm_seg <- read_data("sql/Commercial_segment_sales.sql", list(target_item_condition = conditions[[name]]), glue("Parquet/all_commercial_sales_{name}.parquet"), con_sf)
  comm_seg$SALES_PER_TRX<-comm_seg$SALES/comm_seg$TOTAL_TRX
  comm_seg$SALES_PER_ACTIVE_CUST<-comm_seg$SALES/comm_seg$N_CUSTOMERS
  comm_seg$TRX_PER_ACTIVE_CUST<-comm_seg$TOTAL_TRX/comm_seg$N_CUSTOMERS
  comm_seg$UNITS_PER_TRX<-comm_seg$TOTAL_UNITS/comm_seg$TOTAL_TRX
  comm_seg$PRICE_PER_ITEM<-comm_seg$SALES/comm_seg$TOTAL_UNITS
  comm_seg$SALES_PER_TRX_GST<-comm_seg$SALES_PER_TRX * 1.1
  
  merged_data <- merge(comm_all_sales, comm_seg, by = "DEMOGRAPHIC_SEGMENT", suffixes = c("_ALL", "_TARGET"))
  merged_data$TOTAL_SALES_ALL <- sum(merged_data$SALES_ALL)
  merged_data$TOTAL_SALES_TARGET <- sum(merged_data$SALES_TARGET)
  merged_data$TARGET_SHARE <- merged_data$TOTAL_SALES_TARGET / merged_data$TOTAL_SALES_ALL
  merged_data$SEGMENT_TARGET_SHARE <- merged_data$SALES_TARGET / merged_data$SALES_ALL
  merged_data$SALES_INDEX_SEGMENT <- merged_data$SEGMENT_TARGET_SHARE / merged_data$TARGET_SHARE
  
  merged_comm_data_list[[name]] <- merged_data
  # Add a worksheet and write the data
  addWorksheet(wb, sheetName = name)
  writeData(wb, sheet = name, x = merged_data)
}

if (save_output) {
  saveWorkbook(wb, "Output/Commercial_segment_index.xlsx", overwrite = TRUE)
}


### 3 - Commercial sales by brand and segment (for heatmap)
comm_seg_brand = read_data("sql/Commercial_segments_by_brand.sql", list(target_item_condition = conditions[['Overall']]), glue("Parquet/commercial_segment_by_brand.parquet"), con_sf)

# Calculate total sales per brand across all segments (overall brand share)
overall_brand_sales <- comm_seg_brand %>%
  group_by(BRAND_NAME) %>%
  summarise(OVERALL_BRAND_SALES = sum(SALES, na.rm = TRUE))

total_class_sales <- sum(overall_brand_sales$OVERALL_BRAND_SALES, na.rm = TRUE)

overall_brand_sales <- overall_brand_sales %>%
  mutate(OVERALL_BRAND_SHARE = OVERALL_BRAND_SALES / total_class_sales)

# Calculate brand share within each segment
segment_brand_sales <- comm_seg_brand %>%
  group_by(DEMOGRAPHIC_SEGMENT, BRAND_NAME) %>%
  filter(SALES > 0) %>%
  summarise(SEGMENT_BRAND_SALES = sum(SALES, na.rm = TRUE), .groups = "drop")

segment_totals <- comm_seg_brand %>%
  group_by(DEMOGRAPHIC_SEGMENT) %>%
  summarise(SEGMENT_TOTAL_SALES = sum(SALES, na.rm = TRUE), .groups = "drop")

# Join and calculate segment brand share
segment_brand_share <- segment_brand_sales  %>%
  left_join(segment_totals, by = "DEMOGRAPHIC_SEGMENT") %>%
  left_join(overall_brand_sales, by = "BRAND_NAME") %>%
  mutate(SEGMENT_BRAND_SHARE = SEGMENT_BRAND_SALES / OVERALL_BRAND_SALES,BRAND_SEGMENT_SHARE = SEGMENT_BRAND_SALES / SEGMENT_TOTAL_SALES ) %>% select(-OVERALL_BRAND_SHARE,-OVERALL_BRAND_SALES)

brand_totals <- comm_seg_brand %>%
  group_by(BRAND_NAME) %>%
  summarise(TOTAL_BRAND_SALES = sum(SALES, na.rm = TRUE)) %>%
  mutate(SHARE_LABEL = paste0(BRAND_NAME, ' (', percent(TOTAL_BRAND_SALES / sum(TOTAL_BRAND_SALES), accuracy = 0.1), ')'))

# Join with overall brand share and calculate index
segments_indexed <- segment_brand_share %>%
  left_join(overall_brand_sales, by = "BRAND_NAME") %>%
  left_join(brand_totals, by = "BRAND_NAME") %>%
  mutate(
    SALES_INDEX = BRAND_SEGMENT_SHARE / OVERALL_BRAND_SHARE,
    SHARE_LABEL = paste0(BRAND_NAME, ' (', percent(TOTAL_BRAND_SALES / total_class_sales, accuracy = 0.1), ')'),
    ANNOTATION = percent(SEGMENT_BRAND_SHARE, accuracy = 0.1)
  )

if (save_output) {
  write_csv(segments_indexed, "Output/commercial_brand_index.csv")
}

### 4- Commercial brand index heatmap 

# Filter for top segments (below are top 5 by sales sahre union top 5 by sales index)
top_segments <- c('Electrical Services','Accommodation and Food Services','Health and Residential Care Services','Repair and Maintenance',
                  'Personal and Other Services','Retail and Wholesale Trade','Professional Computer and Scientific Services','Residential Builder')

# Filter to top segments
filtered_data <- segments_indexed %>%
  filter(
    DEMOGRAPHIC_SEGMENT %in% top_segments,
    OVERALL_BRAND_SHARE > 0.2/100, # keeping brands with sales >0.2% otherwise graph is too busy
    BRAND_NAME != "NA"
  ) %>%
  mutate(SALES_INDEX = pmin(SALES_INDEX, 2.5)) # cap index at 2.5

# Order brands by total sales
brand_order <- brand_totals %>%
  arrange(desc(TOTAL_BRAND_SALES)) %>%
  pull(SHARE_LABEL)

# Plot heatmap with segment share per brand as annotation
ggplot(filtered_data, aes(x = factor(SHARE_LABEL, levels = brand_order),
                          y = DEMOGRAPHIC_SEGMENT,
                          fill = SALES_INDEX)) +
  geom_tile(color = "white") +
  geom_text(aes(label = ANNOTATION), size = 3) +
  scale_fill_gradient2(low = "white", high = "red", midpoint = 1) +
  theme_minimal() +
  labs(
    title = "Indexed Sales Performance by Brand and Top Demographic Segments",
    x = "Brand (Total Share)",
    y = "Demographic Segment",
    fill = "Sales Index"
  ) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
