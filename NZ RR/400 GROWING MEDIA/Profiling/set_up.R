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


### creating functions ###

# read data from sql and save in parquet
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

# plot heatmap for segment by brand 
plot_heatmap <- function(
    data,  # dataframe
    sales_column, # the sales column which the index will be calculated on
    segment_col, # the segment column to be profiled
    brand_col, # the brand column to be profiled
    plot_title = "Indexed Sales Performance by Brand and Segment",
    x_axis_label = "Brand (Total Share)",
    y_axis_label = "Segment",
    top_segments = NULL, # list of segments to be included (display all if null)
    exclude_unclassified = TRUE, # exclude unclassified segment
    min_brand_share = 0.01, # threshold on minimum brand share
    index_on_brand = TRUE
) {
  sales_sym <- ensym(sales_column)
  segment_sym <- ensym(segment_col)
  brand_sym <- ensym(brand_col)
  
  total_sales <- sum(data[[as_string(sales_sym)]], na.rm = TRUE)
  
  brand_totals <- data %>%
    group_by(!!brand_sym) %>%
    summarise(TOTAL_BRAND_VAL = sum(!!sales_sym, na.rm = TRUE), .groups = "drop") %>%
    mutate(OVERALL_BRAND_SHARE = TOTAL_BRAND_VAL / total_sales)
  
  segment_totals <- data %>%
    group_by(!!segment_sym) %>%
    summarise(SEGMENT_TOTAL_VAL = sum(!!sales_sym, na.rm = TRUE), .groups = "drop") %>%
    mutate(OVERALL_SEGMENT_SHARE = SEGMENT_TOTAL_VAL / sum(SEGMENT_TOTAL_VAL))
  
  segment_brand <- data %>%
    group_by(!!segment_sym, !!brand_sym) %>%
    summarise(SEGMENT_BRAND_VAL = sum(!!sales_sym, na.rm = TRUE), .groups = "drop")
  
  merged <- segment_brand %>%
    left_join(segment_totals, by = as_string(segment_sym)) %>%
    left_join(brand_totals, by = as_string(brand_sym)) %>%
    mutate(
      brand_share_by_segment = SEGMENT_BRAND_VAL / SEGMENT_TOTAL_VAL,  # % of brand in each segment
      segment_share_by_brand = SEGMENT_BRAND_VAL / TOTAL_BRAND_VAL,    # % of segment in each brand
      SHARE_LABEL = paste0(!!brand_sym, " (", percent(OVERALL_BRAND_SHARE, accuracy = 0.1), ")")
    )
  
  # calculating sales index
  if (index_on_brand) {
    merged$SALES_INDEX <-  merged$brand_share_by_segment / merged$OVERALL_BRAND_SHARE
  } else {
    merged$SALES_INDEX <- merged$segment_share_by_brand / merged$OVERALL_SEGMENT_SHARE
  }
  
  # annotation for tiles
  if (index_on_brand) {
    merged$ANNOTATION <- percent(merged$brand_share_by_segment, accuracy = 0.1)
  } else {
    merged$ANNOTATION <- percent(merged$segment_share_by_brand, accuracy = 0.1)
  }
  
  # Filter for top segments only
  if (!is.null(top_segments)) {
    merged <- merged %>% filter(!!segment_sym %in% top_segments)
  }
  
  # apply minimal share threshold for brand
  if (!is.null(min_brand_share)) {
    merged <- merged %>% filter(OVERALL_BRAND_SHARE >= min_brand_share)
  }
  
  # exclude unclassified segments
  if (exclude_unclassified) {
    merged <- merged %>% filter(!str_detect(tolower(!!segment_sym), "unclassifiable|unknown"))
  }
  
  brand_order <- merged %>%
    distinct(!!brand_sym, SHARE_LABEL, TOTAL_BRAND_VAL) %>%
    arrange(desc(TOTAL_BRAND_VAL)) %>%
    pull(SHARE_LABEL)
  
  print(merged)
  
  ggplot(merged, aes(x = factor(SHARE_LABEL, levels = brand_order),
                     y = !!segment_sym,
                     fill = SALES_INDEX)) +
    geom_tile(color = "white") +
    geom_text(aes(label = ANNOTATION), size = 3) +
    scale_y_discrete(label = function(x) str_wrap(str_trunc(x, 20), width = 30)) +
    scale_fill_gradient2(low = "white", mid = "lightblue", high = "red", midpoint = 1) +
    theme_minimal() +
    labs(
      title = plot_title,
      x = x_axis_label,
      y = y_axis_label,
      fill = "Index"
    ) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
}
