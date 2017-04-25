options(gsubfn.engine = "R")
#The libraries we will use
library(sqldf)
library(dplyr)

#Import the data -- financial data
#This dataframe contains daily price data for a variety of assets
df = read.csv('Fin_Data.csv')
df <- na.omit(df)

#We will add an index so it becomes easier to compare rows to previous rows in R
df$row <- 1:nrow(df)

#Get a rough sense of the data
head(df)

#Columns: BOVESPA	DJIA	NIKKEI	STOXX	Oil	Gold	Copper	EURUSD	10YR	LIBOR	Soy	VIX

#Get all the relevant stock index data
stockIndices <- "BOVESPA, DJIA, NIKKEI, STOXX "
query <- paste("SELECT VIX, ", stockIndices, " FROM df WHERE BOVESPA > 11000;", sep = " ")
query1 <- sqldf(query)
query1

#Find all DJIA prices and oil prices on days when the VIX > 30
query2 <- sqldf("SELECT Date, DJIA, Oil FROM df WHERE VIX > 30;")
query2

#Find all rows where the soy price increased from the previous day
query3 <- sqldf("SELECT df1.Date, df1.DJIA, df1.Soy FROM df as df1, df as df2 WHERE df1.row = df2.row-1 AND df1.Soy < df2.Soy;")
query3

#Find the percentage of times in the data where the soy price increased from the previous day
query4 <- sqldf("SELECT 1.0*COUNT(*)/(SELECT COUNT(*) FROM df) FROM df as df1, df as df2 WHERE df1.row = df2.row-1 AND df1.Soy < df2.Soy;")
query4
#Thus, we see that soy prices increased on approximately 50% of days in our dataset

#What about the percentage increases for gold?
query5 <- sqldf("SELECT 1.0*COUNT(*)/(SELECT COUNT(*) FROM df) FROM df as df1, df as df2 WHERE df1.row = df2.row-1 AND df1.Gold < df2.Gold;")
query5
#Thus, gold prices increased on approximately 47% of the days in this dataset

#We can also combine datasets
#Let's add in another dataset containing
df_unemploy = read.csv('Unemploy_Data.csv')
df_unemploy <- na.omit(df_unemploy)
head(df_unemploy)
#Let's find all dates, unemployment rates, and DJIA prices when the dates are equal for both datasets
query6 <- sqldf("SELECT df.Date, df_unemploy.UNRATE, df.DJIA FROM df inner join df_unemploy WHERE df_unemploy.DATE = df.Date;")
query6

#Amount of days where all asset prices increased
query7 <- "SELECT DISTINCT df1.Date, df1.DJIA FROM df as df1, df as df2 WHERE df1.row = df2.row-1"
vec <- c("DJIA", "Oil", "Gold", "BOVESPA", "NIKKEI", "Copper", "Soy", "STOXX")
for(col in vec){
  if(col != "STOXX"){
    query7 <- paste(query7, " AND df1.", col, " < df2.", col)
  }
}
sqldf(query7)

query8 <- "SELECT max(BOVESPA) as max_bovespa, max(DJIA) as max_djia, max(Oil) as max_oil, max(Gold) as max_Gold FROM df;"
max_df <- sqldf(query8)
max_df[,"max_djia"] #max DJIA for the given period


query9 <- "SELECT min(BOVESPA) as min_bov, min(DJIA) as min_djia, min(Oil) as min_oil, min(Gold) as min_Gold FROM df;"
min_df <- sqldf(query9)
min_df[, "min_djia"] #minimum oil price for the period

query10 <- "SELECT max(BOVESPA)-min(BOVESPA) as range_bov,  max(DJIA)-min(DJIA) as range_djia FROM df;"
range_df <- sqldf(query10)
range_df[, "range_djia"] #minimum oil price for the period

#Dates where either gold, oil, bovespa, or DJIA at the peak
query11 <- "SELECT Date, DJIA, BOVESPA, Gold, Oil FROM df as df1 WHERE df1.BOVESPA = (SELECT max(BOVESPA) from df) OR df1.DJIA = (SELECT max(DJIA) from df) OR df1.Oil = (SELECT max(Oil) from df) OR df1.Gold = (SELECT max(Gold) from df);"
sqldf(query11)

#Different ways of subsetting the data -- want all days when oil prices increased
#We can use a function to add columns to the database
df = read.csv('Fin_Data.csv')
df <- na.omit(df)
df$row <- 1:nrow(df)t
pct <- function(x) {as.numeric(x)/lag(as.numeric(x))} #our function
keeps <- c("DJIA", "Oil", "Gold", "BOVESPA", "Soy", "NIKKEI", "Copper") #Columns to calculate the percentage increase on
df<-data.frame(df, apply(df[, (colnames(df) %in% keeps)],2, pct) )
df<-df[-1,] #Remove the first row with NA values for percentage increase s
head(df) #We can then take a look at our data
df <- na.omit(df)

library(data.table) #To change col names
old_names <- c("Soy.1", "NIKKEI.1", "Copper.1", "DJIA.1", "BOVESPA.1", "Oil.1")
new_names <- c("Soy_Percent_Increase", "NIKKEI_Percent_Increase", "Copper_Percent_Increase", "DJIA_Percent_Increase", "BOVESPA_Percent_Increase", "Oil_Percent_Increase")
setnames(df, old_names, new_names)
df_percent_increases <- apply(df[, (colnames(df) %in% new_names)], 2, function(x) as.numeric(x))
all_increasing_days <- (df[apply(df_percent_increases , 1 , function(x) {all(x > 1.0)}),])
all_increasing_days #Here we can see all days where all asset prices increased
nrow(all_increasing_days) #We can see how many days had the prices of all assets increase

#We can do the same thing finding all increasing days using sqldf
add_to_query <- paste(new_names, " > 1.0", sep = "")
substring <- ""
for (title in add_to_query){
  if(!grepl("Soy", title)){
    substring <- paste(substring, " AND ", title)
  }else{
    substring <- paste(substring, title)
  }
}
query12 <- paste("SELECT *  FROM df WHERE ", substring)
query12
sqldf(query12) 

#The percentage of days where oil, copper, and gold increased
query13 <- "SELECT 1.0*(SELECT COUNT(*) FROM df WHERE Copper_Percent_Increase > 1.0 AND Oil_Percent_Increase > 1.0)/COUNT(*)  FROM df"
sqldf(query13) 

#all days where the DJIA is 1.25 it's average for the period
query14<-"SELECT Date, DJIA from df WHERE DJIA > (SELECT avg(DJIA)*1.25 FROM df)"
sqldf(query14)

df = read.csv('Fin_Data.csv')
df <- na.omit(df)
df$row <- 1:nrow(df)
#Max DJIA drop in a day
query15 <- "SELECT DISTINCT df1.Date, df2.Date, (df1.DJIA - df2.DJIA) as djia_drop from df as df1, df as df2 WHERE df1.row = df2.row-1 AND NOT EXISTS(SELECT * FROM df as df3, df as df4 WHERE df3.row = df4.row-1 AND (df3.DJIA - df4.DJIA) < (df1.DJIA - df2.DJIA))"
sqldf(query15)

#We can use a function to calculate the days between different rows 
calcDate <- function(x) {
  return (as.numeric(as.Date(x, "%m/%d/%Y")))
}
df$Date <- lapply(df$Date, calcDate)
df$lagDate <- lag(df$Date)
df$DaysBetween <- tryCatch(as.numeric(df$Date) - as.numeric(df$lagDate), error = function(e) 0)



#df[sample(1:nrow(df), 50),]
#df[apply(df[, -1], MARGIN = 1, function(x) all(x > 10)), ]
#newdata <- mydata[ which(mydata$gender=='F' & mydata$age > 65), ]
#df$negRow <- lapply(df$row, function(x) x=-x)

