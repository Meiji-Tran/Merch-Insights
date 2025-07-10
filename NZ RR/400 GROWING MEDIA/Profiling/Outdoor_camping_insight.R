###################### Loading library & functions ######################
#########################################################################
source("set_up.R")


########################### Setting Parameters ##########################
#########################################################################
target_item_table = "OUTDOOR_CAMPING_ITEM_LIST"
end_date<-"'2025-06-30'"
start_date<-"'2024-07-01'"
# need to revisit below given pft changes
target_product_condition = "and ( i.item_sub_department_name in 
                            (
                            '400 PICNIC AND CAMP ACC',      '401 PICNIC AND CAMP ACC',
                            '400 RECREATION SHADE',         '402 RECREATION SHADE',
                            '401 PORTABLE OUTDOOR SEATING', '401 PORTABLE OUTDOOR SEATING',
                            '401 COOLERS AND WATER',        '402 COOLERS AND WATER'
                            )
                            or i.item_class_name in ( '501 PORTABLE BBQ', '500 OUTDOOR PORTABLE TABLES'))"
save_output = TRUE

dbGetQuery(con_sf, glue(paste(readLines('sql/0-item-range.sql'), collapse = "\n")))

############################# Sales Overview ############################
#########################################################################

### 1 - by customer type
Overall_customer_type   <- read_data("sql/Overall_by_cust_type.sql", list(), "Parquet/overall_by_customer_type.parquet", con_sf)

### 2 - by brand

overall_brand <- read_data("sql/Overall_by_brand.sql", list(), "Parquet/overall_brand.parquet", con_sf)

### 3 - by pft
overall <- read_data("sql/Overall_by_pft.sql", list(), "Parquet/overall_by_pft.parquet", con_sf)

### 4 - halo sales
halo_sales_overall  <- read_data("sql/8-halo_sale.sql", list(), "Parquet/overall_halo.parquet", con_sf)

### 5 - regional / metro
regional_metro_overall  <- read_data("sql/9-location-metro-regional.sql", list(), "Parquet/overall_region.parquet", con_sf)


# write output file
if (save_output) {
  wb <- createWorkbook()
  addWorksheet(wb, "by_brand")
  writeData(wb, "by_brand", overall_brand)
  addWorksheet(wb, "by_pft")
  writeData(wb, "by_pft", overall)
  addWorksheet(wb, "by_customer")
  writeData(wb, "by_customer", Overall_customer_type)
  addWorksheet(wb, "halo_sales")
  writeData(wb, "halo_sales", halo_sales_overall)
  addWorksheet(wb, "metro_regional")
  writeData(wb, "metro_regional", regional_metro_overall)
  dir.create("Output", showWarnings = FALSE)
  saveWorkbook(wb, "Output/overall_metrics.xlsx", overwrite = TRUE)
}



############################# Consumer view ###########################
#######################################################################

consumer_segment_index <- read_data("sql/1-demographic-segments.sql", list(), "Parquet/consumer_segment_index.parquet", con_sf)

DIY_segment_index <- read_data("sql/2-diy-proficiency.sql", list(), "Parquet/DIY_segment_index.parquet", con_sf)

RFM_index <- read_data("sql/3-rfm.sql", list(), "Parquet/RFM_index.parquet", con_sf)

if (save_output) {
  write_csv(consumer_segment_index, "Output/Consumer_segment_index.csv")
  write_csv(DIY_segment_index, "Output/DIY_segment_index.csv")
  write_csv(RFM_index, "Output/RFM_index.csv")
}

############################# Commercial view ###########################
#######################################################################
n_commercial_segments = 50   # top 50 segments
commercial_ind_seg_index <- read_data("sql/4-commercial-industry-segment.sql", list(), "Parquet/commercial_industry_segment_index.parquet", con_sf)

commercial_lifecycle_index <- read_data("sql/6-Lifecycle_stage.sql", list(), "Parquet/commercial_lifecycle_index.parquet", con_sf)

commercial_spend_tier_index <- read_data("sql/7-Spend_tier.sql", list(), "Parquet/commercial_spend_tier_index.parquet", con_sf)

if (save_output) {
  write_csv(commercial_ind_seg_index, "Output/Commercial_segment_index.csv")
  write_csv(commercial_lifecycle_index, "Output/Commercial_lifecycle_index.csv")
  write_csv(commercial_spend_tier_index, "Output/Commercial_spend_tier_index.csv")
}

############################## Brand view ############################
#######################################################################


consumer_segment_by_brand <- read_data("sql/5-1-Consumer_segments_by_brand.sql", list(), "Parquet/Consumer_segment_by_brand.parquet", con_sf)

plot_heatmap( data = consumer_segment_by_brand,
              sales_column = SALES,
              segment_col = DEMOGRAPHIC_SEGMENT,
              brand_col = BRAND_CODE,
              plot_title = "Indexed Sales Performance by Brand and consumer segments",
              x_axis_label = "Outdoor / camping brand",
              y_axis_label = "",
              index_on_brand = TRUE
)

commercial_segment_by_brand <- read_data("sql/5-2-Commercial_segments_by_brand.sql", list(), "Parquet/Commercial_segment_by_brand.parquet", con_sf)

# top 6 segments (by target sales)
top_segments <- commercial_ind_seg_index %>%
  arrange(desc(RANGE_SALES)) %>%
  slice_head(n = 6) %>%
  pull(COMMERCIAL_INDUSTRY_SEGMENT_CODE)


plot_heatmap( data = commercial_segment_by_brand,
              sales_column = SALES,
              segment_col = DEMOGRAPHIC_SEGMENT,
              brand_col = BRAND_CODE,
              plot_title = "Indexed Sales Performance by Brand and commercial segments",
              x_axis_label = "Outdoor / camping brand",
              y_axis_label = "",
              top_segments = top_segments,
              index_on_brand = TRUE
)


