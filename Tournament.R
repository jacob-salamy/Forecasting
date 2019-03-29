# Tournament for Rossmann data #
# Note: Double-Seasonal Holt-Winters is the optimal forecasting method for this dataset
{
  #devtools::install_github("twitter/AnomalyDetection")
  #install.packages('xfun')
  xfun::pkg_attach2(c('forecast', 'data.table', 'parallel', 'AnomalyDetection'))
  
  # Import and subset the data to 50 stores for demonstration purposes
  
  input <- fread("~\train.csv")
  input <- input[, c(1,3,4)]
  input <- input[Store %in% unique(Store)[1:50]]
  
  # Format data
  
  input[, Date := as.Date(Date)]
  setorder(input, Store, Date)
  input[, Sales := Sales + 1]
  
  # Define forecast horizon and validation window
  
  forecast_period <- 28
  validation <- 14
  
  valid_start <- max(input[['Date']]) - validation - forecast_period
  valid_end <-   max(input[['Date']]) - forecast_period
  fcast_end <-   max(input[['Date']])
  
  # Fill possible gaps in sales data with zero
  # Note: infill begins with first day of sales for each Store
  #       and ends with the end of the forecast period
  
  input <- input[, merge(.SD,
                         data.table(Date = seq.Date(
                           from = min(Date),
                           to = fcast_end,
                           by = 'day'
                         )),
                         by = "Date",
                         all = T), by = Store]
  
  input[is.na(Sales), Sales := 1]
  
  # Remove any possible peak anomalies using the AnomalyDetection package
  # Note: the maximum percentage of anomalies per time series is set to 5%
  #       the direction of possible anomalies is positive
  
  Anomaly <- input[Date < valid_start,
                   AnomalyDetectionVec(
                     Sales,
                     max_anoms = .05,
                     direction = 'pos',
                     period = forecast_period,
                     e_value = T,
                     plot = F
                   )[['anoms']],
                   by = Store][expected_value > 0]
  
  input[, index := 1:.N, by = Store]
  input[Anomaly, Sales := i.expected_value, on = c('Store', 'index')][, index := NULL]
  rm(Anomaly)
  gc()
  
  # Set aside store and date information
  
  Date <- input[Date > valid_end, Date]
  Store <- input[Date > valid_end, Store]
  
  # Define different models
  
  models <- list(
    arima  = function(data, period) {
      arima_model <<- auto.arima(data, lambda = "auto")
      forecast(arima_model, h = period)[['mean']] %>% as.vector(.)
    },
    
    ses    = function(data, period) {
      ses_model <<- ses(data, h = period, lambda = "auto")
      forecast(ses_model, h = period)[['mean']] %>% as.vector(.)
    },
    
    dshw   = function(data, period) {
      dshw_model <<- dshw(data, period1 = 7, period2 = 14)
      forecast(dshw_model, h = period)[['mean']] %>% as.vector(.)
    },
    
    tbats  = function(data, period) {
      tbats_model <<- tbats(data, use.parallel = F)
      forecast(tbats_model, h = period)[['mean']] %>% as.vector(.)
    }
  )
  
  # Function that finds  most accurate forecasting method over the validation window
  
  Tournament <- function(input, models) {
    
    Winner <- list(
      'arima' = NA_real_,
      'ses'   = NA_real_,
      'dshw'  = NA_real_,
      'tbats' = NA_real_
    )
    
    valid_start <- which(input[['Date']] == valid_start)
    valid_end <- which(input[['Date']] == valid_end)
    
    for (model in seq_along(models)) {
      Winner[[model]] <-
        sum(abs(
          .subset(input)[['Sales']][valid_start:(valid_start + validation - 1)] -
           models[[model]](data = .subset(input)[['Sales']][1:(valid_start - 1)], period = validation)
        ))
    }
    
    Winner <- unlist(Winner)
    Winner <- names(Winner)[Winner == min(Winner)][1]
    
    if (Winner == 'dshw') {
      Forecast <- dshw(y = .subset(input)[['Sales']][1:valid_end],
                       h = forecast_period,
                       model = dshw_model)
    } else {
      Forecast <- forecast(object = .subset(input)[['Sales']][1:valid_end],
                           h = forecast_period,
                           model = get(paste0(Winner, '_model')),
                           use.initial.values = T)
    }
    
    return(data.table(Forecast = as.vector(Forecast[['mean']]),
                      Method = as.character(Forecast[['method']])))
  }
  
  # Run the Tournament function in parallel
  
  cl <- makeCluster(detectCores() - 1)
  clusterExport(
    cl,
    varlist = list(
      # forecasting inputs
      'valid_start',
      'valid_end',
      'validation',
      'forecast_period',
      'models',
      # forecasting functions
      'Tournament',
      'forecast',
      'auto.arima',
      'ses',
      'dshw',
      'tbats',
      # misc. functions
      '%>%',
      'data.table'
    ),
    env = environment()
  )
  input <- split(input, input[['Store']]) %>% parLapply(cl, ., Tournament, models) %>% rbindlist(.)
  stopCluster(cl)
  
  # Finalize forecast results
  
  input[Forecast < 1, Forecast := 1]
  input[, Forecast := floor(Forecast - 1)]
  input[['Date']] <- Date
  input[['Store']] <- Store
  
  # Remove everything but the input data.table and run garbage collection
  
  rm(list = ls()[!ls() %in% 'input'])
  gc()
}
