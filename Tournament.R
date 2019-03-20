# Tournament for Rossmann data #
{
  options(scipen = 999)
  xfun::pkg_attach2(c('forecast','data.table','parallel'))

  input <- fread("~/Tournament/train.csv")
  input <- input[, c(1,3,4)]
  input <- input[Store %in% unique(Store)[1:11]]
  
  # Format data
  
  input[, Date := as.Date(Date)]
  input <- input[order(Store, Date)]
  input[, Sales := Sales + 1]
  
  # Define forecast horizon and validation window
  
  forecast_period <- 28
  validation <- 14
  
  valid_start <- max(input[['Date']]) - validation - forecast_period
  valid_end <-   max(input[['Date']]) - forecast_period
  
  # Set aside store and date information
  
  Date <- input[Date > valid_end, Date]
  Store <- input[Date > valid_end, Store]
  
  # Define different models 
  
  models <- list(
    
    arima  = function(data, period) {arima_model <<- auto.arima(data, lambda = "auto")
                                      forecast(arima_model, h = period) %>% .[['mean']] %>% as.vector(.)},
    
    ses    = function(data, period) {ses_model <<- ses(data, h = period, lambda = "auto")
                                      forecast(ses_model, h = period)[['mean']] %>% as.vector(.)},
    
    dshw   = function(data, period) {dshw_model <<- dshw(data, period1 = 7, period2 = 14)
                                      forecast(dshw_model, h = period)[['mean']] %>% as.vector(.)},
    
    tbats  = function(data, period) {tbats_model <<- tbats(data, use.parallel = F)
                                      forecast(tbats_model, h = period)[['mean']] %>% as.vector(.)}
  )
  
  # Function that finds  most accurate forecasting method over the validation window
  
  Tournament <- function(input, models){
    
    Winner <- list('arima' = NA_real_,
                   'ses'   = NA_real_,
                   'dshw'  = NA_real_,
                   'tbats' = NA_real_)
    
    valid_start <- which(input[['Date']] == valid_start)
    valid_end <- which(input[['Date']] == valid_end)
    
    for (model in seq_along(models)){
      Winner[[model]] <- sum(abs(.subset(input)[['Sales']][valid_start:(valid_start + validation - 1)] - 
                                   models[[model]](data = .subset(input)[['Sales']][1:(valid_start - 1)],
                                   period = validation)))
    }
    
    Winner <- unlist(Winner)
    Winner <- names(Winner)[Winner == min(Winner)][1]
    
    if(Winner == 'dshw') {
      
      Forecast <- dshw(y = .subset(input)[['Sales']][1:valid_end],
                       h = forecast_period,
                       model = dshw_model)
    } else {
      
      Forecast <- forecast(object = .subset(input)[['Sales']][1:valid_end],
                           h = forecast_period, 
                           model = get(paste0(Winner, '_model')),
                           use.initial.values=TRUE)
    } 
    
    Method <- as.character(Forecast[['method']])
    Forecast <- as.data.frame(as.vector(Forecast[['mean']]))
    Forecast[Forecast < 1] <- 1
    Forecast[['Method']] <- Method
    return(Forecast)
  }
  
  # Run the Tournament function in parallel
  
  cl <- makeCluster(detectCores() - 1)
  clusterExport(cl, varlist = list('valid_start','valid_end','validation','forecast_period','models', # forecasting inputs
                                   'Tournament','forecast','auto.arima','ses','dshw','tbats', # forecasting functions
                                   '%>%' # misc. functions
  ), env = environment())
  system.time(input <- split(input, input[['Store']]) %>% parLapply(cl, ., Tournament, models) %>% rbindlist(.))
  stopCluster(cl)
  
  # Finalize forecast results
  
  names(input)[1] <- 'Forecast'
  setDT(input)[, Forecast := Forecast - 1]
  input[['Date']] <- Date
  input[['Store']] <- Store
}
