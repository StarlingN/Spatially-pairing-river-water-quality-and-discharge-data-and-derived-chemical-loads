#function for logging why sites wouldn't run
log_error <- function(parameter, spec, set, Qsite, df_name, msg,
                      logfile = "error_log.txt") {
  write(
    paste(
      format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      parameter,
      spec,
      set,
      Qsite,
      df_name,
      msg,
      sep = " | "
    ),
    file = logfile,
    append = TRUE
  )
}
#install libraries
remotes::install_github("datastreamapp/datastreamr")
library(datastreamr)
library(readr)
library(tidyverse)
library(tidyhydat)
library(EGRET)
library(EGRETci)
library(dplyr)

#pull from DataStream
setAPIKey('###########################')
call <- list(
  `$select` = "Id,DOI,LocationId,CharacteristicName,ResultUnit,ResultValue,ResultDetectionQuantitationLimitMeasure,ResultAnalyticalMethodID,ResultSampleFraction,ActivityStartDate",
  `$filter` =
    "CharacteristicName in ('Organic carbon','Total Phosphorus,mixed forms','Total Nitrogen, mixed forms','Chloride','Magnesium','inorganic nitrogen','Ammonia','Total Hardness') and MonitoringLocationType eq 'River/Stream'",
  `$top` = NULL,
  `$count` = "false"
)

results2data <- observations(call)

l <- list(
  `$select` = "Id,DOI,ID,Name,Latitude,Longitude",
  `$filter` =
    "CharacteristicName in ('Organic carbon','Total Phosphorus,mixed forms','Total Nitrogen, mixed forms','Chloride','Magnesium','inorganic nitrogen','Ammonia','Total Hardness') and MonitoringLocationType eq 'River/Stream'",
  `$top` = NULL,
  `$count` = "false"
)
results2loc <- locations(l)

m<- list(
  `$select` = "DOI,Version,DatasetName,DataCollectionOrganization,DataUploadOrganization,Citation",
  `$filter` =
    "CharacteristicName in ('Organic carbon','Total Phosphorus,mixed forms','Total Nitrogen, mixed forms','Chloride','Magnesium','inorganic nitrogen','Ammonia','Total Hardness') and MonitoringLocationType eq 'River/Stream'",
  `$top` = NULL,
  `$count` = "false"
)
results2meta<-metadata(m)

#merge hydat
index <- read_csv("canada_datastream_hydat_matchups+RHBN.csv")
results2loc$LocationId<-results2loc$Id
#summarize available data
resultssummary<-results2data%>%
  group_by(LocationId,
           CharacteristicName,
           ResultAnalyticalMethodID,
           ResultSampleFraction,
           ResultUnit,
           DOI)%>%
  summarise(
    samples=n(),
    start=min(ActivityStartDate),
    end=max(ActivityStartDate)
  )
#merge in location
resultssummary <- resultssummary %>%
  left_join(
    results2loc %>% select(LocationId, Latitude, Longitude),
    by = "LocationId"
  )

index$Latitude<-index$MonitoringLocationLatitude
index$Longitude<-index$MonitoringLocationLongitude
#associate lat long to the pairied HYDAT station
resultsWhydat <- resultssummary %>%
  inner_join(
    index %>% select(Latitude, Longitude, StationNum),
    by = c("Latitude", "Longitude")
  )
#merge in  metadata
resultsWmeta<- resultsWhydat %>%
  inner_join(results2meta,by = c("DOI"))
dataf<- results2data %>%
  inner_join(resultsWmeta,
             by = c("LocationId",
                    "CharacteristicName",
                    "ResultAnalyticalMethodID",
                    "ResultSampleFraction",
                    "ResultUnit",
                    "DOI"
             ))
dataf<-dataf%>%
  inner_join(
    results2loc %>% select(LocationId,Latitude, Longitude, Name),
    by = c("LocationId")
  )
#check samples
data<-dataf%>%
  drop_na(ResultValue)
data$year<-year(data$ActivityStartDate)
data$month<-month(data$ActivityStartDate)
data$ym<-paste0(data$year,data$month)
data_list<-split(data,list(data$StationNum,
                           data$CharacteristicName,
                           data$ResultSampleFraction,
                           data$ResultUnit,
                           data$ResultAnalyticalMethodID,
                           data$LocationId,
                           data$Name),
                 drop=TRUE)

for(i in seq_along(data_list)){
  data_list[[i]] <- data_list[[i]] %>%
    arrange(year, month) %>%
    mutate(
      month_index = year*12 + month,
      gap_months = month_index - lag(month_index)
    )
  
}
#divide when there is a sample gap >4 months
new_list <- list()  # Initialize new list

for (df_name in names(data_list)) {
  df <- data_list[[df_name]]
  
  # Create group index: increment every time gap >= 5
  group_id <- cumsum(!is.na(df$gap_months) & df$gap_months >= 5)
  
  # Split into list of dataframes
  split_dfs <- split(df, group_id)
  
  # Assign letters A, B, C, ...
  letters_vec <- LETTERS[seq_along(split_dfs)]
  
  for (i in seq_along(split_dfs)) {
    sub_df <- split_dfs[[i]]
    sub_df$set <- letters_vec[i]
    
    new_list[[paste0(df_name, letters_vec[i])]] <- sub_df
  }
}
#keep only 40+ samples and 4+ years
new_list <- Filter(function(df) nrow(df) >= 40, new_list)
new_list <- Filter(function(df) length(unique(df$year)) >= 4, new_list)


for (i in seq_along(new_list)) {
  new_list[[i]]$samples_u <- unique(nrow(new_list[[i]]))
  new_list[[i]]$samples <- nrow(new_list[[i]])
  new_list[[i]]$years   <- length(unique(new_list[[i]]$year))
  new_list[[i]]$npery   <- unique(new_list[[i]]$samples) / new_list[[i]]$years
  new_list[[i]]$start   <- min( new_list[[i]]$ActivityStartDate)
  new_list[[i]]$end     <- max( new_list[[i]]$ActivityStartDate)
  
}


dir.create("samples", showWarnings = FALSE)

for (df in new_list) {
  
  # meta
  parameter <- df$CharacteristicName[1]
  spec <- df$ResultSampleFraction[1]
  Qsite <- df$StationNum[1]
  DOI <- df$DOI[1]
  unit <- df$ResultUnit[1]
  method <- df$ResultAnalyticalMethodID[1]
  set <- df$set[1]
  Name<-df$Name[1]
  
  df$ActivityStartDate <- as.Date(df$ActivityStartDate)
  
  n <- length(unique(df$ActivityStartDate))
  datemin <- min(df$ActivityStartDate, na.rm = TRUE)
  datemax <- max(df$ActivityStartDate, na.rm = TRUE)
  
  years <- as.numeric(difftime(datemax, datemin, units = "days")) / 365.25
  nperyear <- n / years
  
  if (years < 4) next
  if (nperyear < 10) next
  
  ms <- month(datemin)
  ys <- year(datemin)
  me <- month(datemax)
  ye <- year(datemax)
  #set dates for discharge to be pulled
  Qstart <- as.Date(sprintf("%04d-%02d-01", ys, ms))
  Qend <- ceiling_date(as.Date(sprintf("%04d-%02d-01", ye, me)), "month") - days(1)
  
  # ---------------- Q download ----------------
  Q <- tryCatch(
    hy_daily_flows(
      station_number = Qsite,
      start_date = Qstart,
      end_date = Qend
    ),
    error = function(e) {
      log_error(parameter, spec, set, Qsite, "NA",
                paste0("HYDAT error: ", e$message))
      return(NULL)
    }
  )
  
  if (is.null(Q)) {
    log_error(parameter, spec, set, Qsite, "NA", "Q is NULL")
    next
  }
  
  if (nrow(Q) == 0) {
    log_error(parameter, spec, set, Qsite, "NA", "Q has 0 rows")
    next
  }
  
  Q <- data.frame(Date = Q$Date, Value = Q$Value)
  
  all_dates <- data.frame(Date = seq(Qstart, Qend, by = "day"))
  
  Q_full <- all_dates %>%
    left_join(Q, by = "Date")
  
  cp <- sum(is.na(Q_full$Value)) / nrow(Q_full)
  
  if (cp > 0.2) {
    log_error(parameter, spec, set, Qsite, "NA",
              paste0("Missing Q proportion = ", round(cp, 3)))
    next
  }
  
  # ---------------- merge ----------------
  samples <- df %>%
    left_join(Q_full, by = c("ActivityStartDate" = "Date"))
  
  # ---------------- safe filename ----------------
  safe_name <- paste0(
    Qsite, "_",
    gsub("[^A-Za-z0-9_]", "_", Name), "_",
    gsub("[^A-Za-z0-9_]", "_", parameter), "_",
    gsub("[^A-Za-z0-9_]", "_", spec), "_",
    set,
    method
  )
  
  # save
  write.csv(
    samples,
    file = file.path("samples", paste0(safe_name, ".csv")),
    row.names = FALSE
  )
  
  # plot
  p <- ggplot(samples, aes(x = ActivityStartDate, y = ResultValue)) +
    geom_point(alpha = 0.7, colour = "red") +
    geom_smooth(alpha = 0.5, colour = "blue") +
    theme_bw() +
    labs(
      title = safe_name,
      x = "Date",
      y = "ResultValue"
    )
  
  ggsave(
    file.path("samples", paste0(safe_name, "_Q_vs_ResultValue.png")),
    plot = p,
    width = 6,
    height = 4,
    dpi = 300
  )
}

folder_path <- ""
#export data
combined_df <- list.files(
  "samples",
  pattern = "\\.csv$",
  full.names = TRUE
) %>%
  set_names() %>%
  map_dfr(
    ~ read_csv(.x, col_types = cols(.default = col_character())),
    .id = "source_file"
  )

write.csv(combined_df,"samplesall.csv")
