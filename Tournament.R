# Tournament for rossmann Data #

library(forecast)
library(data.table)
library(parallel)

input <- fread("~/Desktop/Forecasting/rossmann-store-sales/train.csv")
input <- input[, c(1,3,4)]

# Format data
input[, Date := as.Date(Date)]
input <- input[order(Store, Date)]
input[, Sales := ts(Sales), by = Store]

# Define forecast horizon and validation window
forecast <- 30
validation <- 30
start <- max(input$Date) - validation - forecast

# Define different models 
models <- list(
  arima  = function(data, period) auto.arima(data) %>% forecast(., h = period) %>% .[['mean']] %>% as.vector(.),
  ses    = function(data, period) ses(data, h = period)[['mean']] %>% as.vector(.),
  croston = function(data, period) croston(data, h = period)[['mean']] %>% as.vector(.)
  )

# Function that finds and returns results from most accurate forecasting method over the validation window
Tournament <- function(input, models){
  Score <- vector('list', length = length(models))
  for (i in seq_along(models)){
  Score[[i]] <- sum(abs(input$Sales[input$Date > start] - models[[i]](data = input$Sales[input$Date < start],
                                                                      period = validation)))
  }
  Score <- unlist(Score) %>% t(.)
  names(Score) <- names(models)
  Winner <- names(Score)[Score[1,] == min(Score[1,])]
  Forecast <- models[[Winner]](data = input$Sales[input$Date < start + validation],
                               period = forecast) %>% as.data.frame(.)
  Forecast$Model <- Winner
  return(Forecast)
}

# Run the Tournament function in parallel
Forecasts <- split(input, input$Store) %>% mclapply(., Tournament, models, mc.cores = 2) %>% rbindlist(.)

names(Forecasts)[1] <- 'Forecast'
Forecasts$Date <- input[Date > start + validation, Date]
Forecasts$Store <- input[Date > start + validation, Store]
