# Bayesian Optimization of a Random Forest forecast using Rossmann data #
{ 
options(scipen = 999)
xfun::pkg_attach2(c('rBayesianOptimization','h2o','data.table','lubridate'))
 
#Import and format data
input <- fread("~/train.csv")
input[, Date := ymd(Date)]
setorder(input, Store, Date)

# Create a month's worth of lags
lags <- c(1:30)
lag_list <- paste("lag", lags)
input[, (lag_list) := shift(Sales, lags, NA, 'lag'), by = Store]

# Target encode date features using a list of functions
date_features <- list(
  qtr = function(x) quarter(x),
  month = function(x) month(x),
  week = function(x) week(x),
  weekday = function(x) weekdays(x, abbreviate = T))

for (i in seq_along(date_features)) {
input[, (names(date_features[i])) := mean(Sales), by = .(Store, date_features[[i]](Date))]
}

# Create a holdout sample for time series validation
holdout <- input[Date >= max(Date) - 60,]
holdout[, t := .GRP, by = Store]

# Format the input
features <- c(lag_list, names(date_features))
input <- na.omit(input[Date < max(Date) - 60, c('Sales', features), with = F])
h2o.init()
input <- as.h2o(data.matrix(input))
id <- h2o.ls() 

# Set up a Random Forest bayesian optimization function for the # of rows sampled 
# and the minimum # observations per leaf
rf_bayesopt <- function(mtry, node.size){
 
  model <- h2o.randomForest( x = features,
                             y = "Sales",
                             training_frame = input,
                             max_depth = 30,
                             ntrees = 150,
                             mtries = as.integer((mtry/9)*(length(features)-1)),
                             min_rows = as.integer(2 + node.size))
  
  # Begin forecasting while replacing lagged values with RF predictions
  holdout$RF <- 0
  holdout <- split(holdout, holdout$t)
  holdout[[1]]$RF <- as.vector(predict(model, as.h2o(data.matrix(holdout[[1]]))))
  
  for(i in 2:length(holdout)){
    holdout[[i]][,c(lag_list[-1])] <- holdout[[i-1]][,c(lag_list[-length(lag_list)])]
    holdout[[i]][,c(lag_list[1])] <- holdout[[i-1]]$RF
    holdout[[i]]$RF <- as.vector(h2o.predict(model, as.h2o(data.matrix(holdout[[i]]))))
  }
  
  holdout <- rbindlist(holdout)
  
  # Get the simple sum of absolute errors over the holdout sample
  Score <- sum(abs(holdout$Sales - holdout$RF))
  
  # Remove h2o cluster data
  model <- h2o.ls()
  removal <- as.character(model$key[!model$key %in% id$key])
  h2o.rm(removal)
  gc()
  
  # Return the score for the function to optimize
  list(Score = -1*log(Score), Pred = 1) }

# Run the optimization and return the best parameters
Bayes_Opt_Result <- BayesianOptimization(rf_bayesopt, 
                               bounds = list(mtry = c(3L,9L), node.size = c(0L,8L)),
                               init_grid_dt = NULL, init_points = 10, n_iter = 10,
                               acq = "ucb", verbose = TRUE)$Best_Par

Bayes_Opt_Result <- list(mtry = as.integer((Result[1]/9)*(length(features)-1)),
                  node.size = as.integer(3 + Result[2]*3))

# Remove everything in the global environment except for the result
rm(list = ls()[!ls() %in% 'Bayes_Opt_Result'])
gc()
}
