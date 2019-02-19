# Tournament for Rossmann Data #

library(forecast)
library(data.table)
library(parallel)

input <- fread("~/Desktop/Forecasting/rossmann-store-sales/train.csv")
input <- input[, c(1,3,4)]

input[, Date := as.Date(Date)]
input <- input[order(Store, Date)]
input[, Sales := ts(Sales), by = Store]

forecast <- 30
validation <- 30
start <- max(input$Date) - validation

models <- list(
  arima  = function(data, period) auto.arima(data) %>% forecast(., h = period) %>% .[['mean']] %>% as.vector(.),
  ses    = function(data, period) ses(data, h = period)[['mean']] %>% as.vector(.),
  croston = function(data, period) croston(data, h = period)[['mean']] %>% as.vector(.)
  )

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

Forecasts <- split(input, input$Store) %>% mclapply(., Tournament, models, mc.cores = 2) %>% rbindlist(.)

names(Forecasts)[1] <- 'Forecast'
Forecasts$Date <- input[Date > start + validation, Date]
Forecasts$Store <- input[Date > start + validation, Store]
