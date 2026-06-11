###############pull data#############################
remotes::install_github("datastreamapp/datastreamr")
library(datastreamr)
library(readr)
library(tidyverse)
library(tidyhydat)
library(EGRET)
library(EGRETci)
library(dplyr)

#import data index
index <- read_csv(path to "hydat_datastream_pairs.csv")

setAPIKey('############################')
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
resultssummary$years <- year(resultssummary$end) - year(resultssummary$start) + 1
resultssummary$npery<-resultssummary$samples/resultssummary$years

resultssummaryf<-resultssummary%>%
  filter(npery>=10)%>%
  filter(years>=4)

results2loc$LocationId<-results2loc$Id

resultssummaryf <- resultssummaryf %>%
  left_join(
    results2loc %>% select(LocationId, Latitude, Longitude),
    by = "LocationId"
  )

index$Latitude<-index$MonitoringLocationLatitude
index$Longitude<-index$MonitoringLocationLongitude
resultsWhydat <- resultssummaryf %>%
  inner_join(
    index %>% select(Latitude, Longitude, StationNum),
    by = c("Latitude", "Longitude")
  )

#merge in  metadata
resultsWmeta<- resultsWhydat %>%
  inner_join(results2meta,by = c("DOI"))

setwd("D:")
write.csv(resultsWmeta,"pairingoverview.csv",row.names=FALSE)

dataf<- results2data %>%
  inner_join(resultsWmeta,
             by = c("LocationId",
                    "CharacteristicName",
                    "ResultAnalyticalMethodID",
                    "ResultSampleFraction",
                    "ResultUnit",
                    "DOI"
             ))

write.csv(dataf,"data_filtered.csv",row.names=FALSE)

data<-datafnames%>%
  drop_na(ResultValue)


#In each year, must have records in at least 8 consecutive months
data$year<-year(data$ActivityStartDate)
data$month<-month(data$ActivityStartDate)
data$ym<-paste0(data$year,data$month)

data_list<-split(data,list(data$StationNum,
                           data$CharacteristicName,
                           data$ResultSampleFraction,
                           data$ResultUnit,
                           data$ResultAnalyticalMethodID,
                           data$LocationId),
                 drop=TRUE)

for(i in seq_along(data_list)){
  
  data_list[[i]] <- data_list[[i]] %>%
    arrange(year, month) %>%
    mutate(
      month_index = year*12 + month,
      gap_months = month_index - lag(month_index)
    )
  
}

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
new_list <- Filter(function(df) df$npery[1] >= 10, new_list)

setwd("")
save(new_list, file = "data.RData")


###################WRTDS###################
#install lib
library(readr)
library(tidyhydat)
library(EGRET)
library(EGRETci)
library(lubridate)
library(dplyr)
library(zoo)
#load in summary file if re-starting to prevent duplicates and overwritting
summary <- read_csv("summary.csv")

#build info template
INFO_template <- list(
  shortName        = NA_character_,
  stationID        = NA_character_,
  paramShortName   = NA_character_,
  paramName        = NA_character_,
  param.units      = NA_character_,
  lat              = NA_real_,
  long             = NA_real_,
  drainSqKm        = NA_real_,
  drainSqMiles     = NA_real_,
  state            = NA_character_,
  county           = NA_character_,
  siteType         = NA_character_,
  agency           = NA_character_,
  timeStep         = "day",
  tz               = "UTC"
)

#set directory
setwd("")
#build Q hosting file
dir.create("Q", showWarnings = FALSE)
#import data pulled from Datastream
load("data.RData") #dataset pulled from datastream API see: https://github.com/datastreamapp/api-docs
data_list<-new_list #list of grouped datasets stored in data.RData

#remove sites previously run if re-starting
sitesrun<-c(unique(summary$hydat))
codes_to_remove <- sitesrun

for (df in data_list){
  #meta
  df$Latitude<-df$Latitude.x
  df$Longitude<-df$Longitude.x
  parameter <- df$CharacteristicName[1]
  spec <- df$ResultSampleFraction[1]
  Qsite     <- df$StationNum[1]
  Csite     <- df$Name[1]
  DOI       <- df$DOI[1]
  unit<-df$ResultUnit[1]
  method <- df$ResultAnalyticalMethodID[1]
  df$ActivityStartDate <- as.Date(df$ActivityStartDate)
  n <-as.numeric(length(unique(df$ActivityStartDate)))
  datemin <- min(df$ActivityStartDate, na.rm = TRUE)
  datemax <- max(df$ActivityStartDate, na.rm = TRUE)
  years   <- as.numeric(difftime(datemax, datemin, units = "days")) / 365.25
  lat<-df$Latitude[1]
  long<-df$Longitude[1]
  cite<-df$Citation[1]
  set<-df$set[1]
  ms<-month(datemin)
  ys<-year(datemin)
  me<-month(datemax)
  ye<-year(datemax)
  nperyear<-(n/years)
  if (years<4) next
  if (nperyear<10) next
  #get Q 
  Qstart <- as.Date(sprintf("%04d-%02d-01", ys, ms))
  
  Qend <- ceiling_date(
    as.Date(sprintf("%04d-%02d-01", ye, me)),"month") - days(1)
  Q<- tryCatch(
    hy_daily_flows(
      station_number = paste0(Qsite),
      start_date = Qstart,
      end_date = Qend
    ),
    error = function(e) {
      message("Skipping site ", Qsite, ": ", e$message)
      return(NULL)
    }
  )
  if (is.null(Q)) next
  if (nrow(Q) == 0) next
  Q<-data.frame(Date=Q$Date,Value=Q$Value)
  Qstart<- as.Date(Qstart)
  Qend<- as.Date(Qend)
  
  all_dates <- data.frame(
    Date = seq(Qstart, Qend, by = "day")
  )
  Q_full <- all_dates %>%
    left_join(Q, by = "Date")
  
  cp<-(sum(is.na(Q_full$Value)))/nrow(Q_full)
  if (cp>0.2) next
  
  avg <- mean(Q_full$Value[Q_full$Value != 0], na.rm = TRUE)
  
  if (!is.na(Q_full$Value[1]) && Q_full$Value[1] == 0) {
    Q_full$Value[1] <- avg
  }
  
  n_Q <- nrow(Q_full)
  
  if (!is.na(Q_full$Value[n_Q]) && Q_full$Value[n_Q] == 0) {
    Q_full$Value[n_Q] <- avg
  }
  Q_full$Value<-Q_full$Value <- na.approx(Q_full$Value, x = Q_full$Date, na.rm = FALSE)
  Q<-Q_full
  write.csv(Q, file.path("", paste0(Qsite, ".csv")),
            row.names = FALSE)
  filepathQ<-"D:/nonRNpairing/Q"
  filenameQ<-paste0(Qsite,".csv")
  Q_WRTDS<-readUserDaily(filepathQ,filenameQ,qUnit=2)
  fullpathQ <- file.path(filepathQ, filenameQ)
  if (file.exists(fullpathQ)) file.remove(fullpathQ)
  
  C <- data.frame(
    Date = df$ActivityStartDate,
    Comment = ifelse(
      is.na(df$ResultDetectionQuantitationLimitMeasure),
      NA,
      ifelse(df$ResultValue > df$ResultDetectionQuantitationLimitMeasure, NA, "<")
    ),
    Sample = df$ResultValue
  )
  write.csv(C,"tempC.csv",row.names=FALSE)
  #import file
  filepathC<-""
  filenameConc<-"tempC.csv"
  C_WRTDS<-readUserSample(filepathC,filenameConc)
  if (file.exists("tempC.csv")) file.remove("tempC.csv")
  
  #info
  info_vars <- list(
    shortName       = Csite,
    stationID       = Qsite,
    paramShortName  = parameter,
    paramName       = paste0(parameter,spec),
    param.units     = unit,
    lat             = lat,
    long            = long,
    drainSqKm       = NA
  )
  INFO <- modifyList(INFO_template, info_vars)
  INFO$timeStep <- "day"
  INFO$tz <- "UTC"
  
  #merge
  eList<-mergeReport(INFO,Q_WRTDS,C_WRTDS)
  
  if (years > 10) {
    WRTDS <- tryCatch(
      modelEstimation(
        eList,windowY = 7,windowQ = 2,windowS = 0.5,
        minNumObs = 40,minNumUncen = 30,edgeAdjust = TRUE
      ),
      error = function(e) {
        message("WRTDS failed: ", Qsite, " ", parameter)
        return(NULL)
      }
    )
    
    if (is.null(WRTDS)) next
    
  } else {
    WRTDS <- tryCatch(
      modelEstimation(
        eList,windowY = 100,windowQ = 2,windowS = 0.5,
        minNumObs = 40,minNumUncen = 30,edgeAdjust = TRUE
      ),
      error = function(e) {
        message("WRTDS failed: ", Qsite, " ", parameter)
        return(NULL)
      }
    )
    
    if (is.null(WRTDS)) next
  }
  
  
  WRTDS_K <- WRTDSKalman(WRTDS)
  
  WRTDS_Kd<-WRTDS_K$Daily
  
  Sample <- getSample(WRTDS)
  fluxBias <- fluxBiasStat(Sample)
  
  dailyBoot <- tryCatch(
    genDailyBoot(WRTDS_K, nBoot=25, nKalman=5, rho=0.9),
    error=function(e) NULL
  )
  if (is.null(dailyBoot)) next
  
  #make daily PI
  PIdaily<-makeDailyPI(dailyBoot,WRTDS)
  
  #make results
  results<-data.frame(date=PIdaily$flux$Date,
                      Q=WRTDS_Kd$Q,
                      L5=PIdaily$flux$p5,L25=PIdaily$flux$p25,L50=PIdaily$flux$p50,L75=PIdaily$flux$p75,L95=PIdaily$flux$p95,
                      C5=PIdaily$conc$p5,C25=PIdaily$conc$p25,C50=PIdaily$conc$p50,C75=PIdaily$conc$p75,C95=PIdaily$conc$p95,
                      LFN=WRTDS_Kd$FNFlux,
                      CFN=WRTDS_Kd$FNConc)
  #make summary
  summary_out<-data.frame(
    hydat=Qsite,
    Csite=Csite,
    set=set,
    DOI=DOI,
    parameter=parameter,
    spec=spec,
    unit=unit,
    method=method,
    ystart=ys,
    yend=ye,
    Nyears=years,
    n=n,
    cite=cite,
    cp=cp,
    AvgC50=mean(PIdaily$conc$p50,na.rm = TRUE),
    AvgC25=mean(PIdaily$conc$p25,na.rm = TRUE),
    AvgC75=mean(PIdaily$conc$p75,na.rm = TRUE),
    AvgL50=mean(PIdaily$flux$p50,na.rm = TRUE),
    AvgL25=mean(PIdaily$flux$p25,na.rm = TRUE),
    AvgL75=mean(PIdaily$flux$p75,na.rm = TRUE),
    AvgFNC=mean(WRTDS_Kd$FNConc,na.rm = TRUE),
    AvgFNL=mean(WRTDS_Kd$FNFlux,na.rm = TRUE),
    sdC50=sd(PIdaily$conc$p50,na.rm = TRUE),
    sdL50=sd(PIdaily$flux$p50,na.rm = TRUE),
    sdFNC=sd(WRTDS_Kd$FNConc,na.rm = TRUE),
    sdFNL=sd(WRTDS_Kd$FNFlux,na.rm = TRUE),
    FBS=fluxBias,
    lat=lat,
    long=long
  )
  #export
  safe_param <- gsub("[^A-Za-z0-9_]", "_", parameter)
  safe_spec <- gsub("[^A-Za-z0-9_]", "_", spec)
  safe_unit<- gsub("[^A-Za-z0-9_]", "_", unit)
  safe_Csite<- gsub("[^A-Za-z0-9_]", "_", Csite)
  filename<-paste0(Qsite,safe_Csite,safe_spec,safe_param,method,set)
  filename<-filename <- gsub("[/:*?\"<>|]", "_", filename)
  write.csv(results,paste0(filename,".csv"),row.names=FALSE)
  #APPEND SUMMARY
  if (!file.exists("summary.csv")) {
    write.csv(summary_out, "summary.csv", row.names = FALSE)
  } else {
    write.table(summary_out, "summary.csv",
                sep = ",",
                row.names = FALSE,
                col.names = FALSE,
                append = TRUE)} 
}

