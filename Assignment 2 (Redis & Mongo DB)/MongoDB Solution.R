#----------------------------------Assignment in Big Data Systems-------------------------------------#
#-------------------------------------------- MongoDB/TASK 2-------------------------------------------#

# List of libraries to check
libraries <- c("mongolite", "lubridate", "httpuv", "jsonlite", "rmutil", "stringr", "dplyr", "rmarkdown")

# Check if each library is installed and install it if necessary
for (lib in libraries) {
  if (!requireNamespace(lib, quietly = TRUE)) {
    install.packages(lib)}
  library(lib, character.only = TRUE)
}

#setwd() # <-- set your working directory here

# Read file with paths of JSON files to a list
my_data <- readLines("BIKES/files_list.txt", skipNul = TRUE)
my_data <- my_data[-1][nzchar(my_data[-1])]

# Create a mongo connection object and create an empty collection
m <- mongo(collection = "bikeAds",  db = "mydb", url = "mongodb://localhost")

ageScore <- 0
mileageScore <- 0
priceScore <- 0
cc_bhpScore <- 0

lower1age <- 0
lower2age <- 6
lower3age <- 11
upper1age <- 5
upper2age <- 10

lower1mlg <- 0
lower2mlg <- 20001
lower3mlg <- 50001
upper1mlg <- 20000
upper2mlg <- 50000

lower1price <- 0
lower2price <- 2001
lower3price <- 5001
upper1price <- 2000
upper2price <- 5000

group1Factor <- 50
group2Factor <- 30
group3Factor <- 20

lower1cc_bhp <- 0
lower2cc_bhp <- 0.051
lower3cc_bhp <- 0.11
upper1cc_bhp <- 0.05
upper2cc_bhp <- 0.1

for (row in 1:length(my_data)) {
  data <- fromJSON(readLines(paste('BIKES/',my_data[row],sep =''), encoding = 'UTF-8'))
  data$ad_data$Negotiable <- any(grepl("Negotiable", unlist(data), ignore.case = TRUE),grepl("συζητήσιμη", unlist(data), ignore.case = TRUE),grepl("συζητησιμη", unlist(data), ignore.case = TRUE))
  data$ad_data$Mileage <- as.numeric(gsub("[^0-9.]|\\.", "", data$ad_data$Mileage))
  data$ad_data$Price <- as.numeric(gsub("[^0-9.]|\\.", "", data$ad_data$Price))
  data$metadata$model <- gsub(" -.*", "", data$metadata$model)
  data$ad_data$`Cubic capacity` <- as.numeric(gsub("[^0-9.]|\\.", "", data$ad_data$`Cubic capacity`))
  data$ad_data$Power <- as.numeric(gsub("[^0-9.]|\\.", "", data$ad_data$Power))
  data$ad_data$Price[which(data$ad_data$Price == 'Askforprice')] <- 0
  data$ad_data$Registration <- as.Date(paste0("01/",gsub(" ", "", data$ad_data$Registration)), format = '%d/%m/%Y')
  data$ad_data$Age <- round(as.numeric(difftime(as.Date(paste0("01/", month(Sys.Date()), "/", year(Sys.Date())), format = "%d/%m/%Y"), as.Date(data$ad_data$Registration), units = "days")) / 365.25,0)

  ageScore <- ifelse(data$ad_data$Age >= lower1age & data$ad_data$Age <= upper1age, data$ad_data$Age * group1Factor,
                     ifelse(data$ad_data$Age >= lower2age & data$ad_data$Age <= upper2age, data$ad_data$Age * group2Factor,
                            ifelse(data$ad_data$Age >= lower3age, data$ad_data$Age * group3Factor, 0)))

  mileageScore <- ifelse(data$ad_data$Mileage >= lower1mlg & data$ad_data$Mileage <= upper1mlg, data$ad_data$Mileage * group1Factor,
                         ifelse(data$ad_data$Mileage >= lower2mlg & data$ad_data$Mileage <= upper2mlg, data$ad_data$Mileage * group2Factor,
                                ifelse(data$ad_data$Mileage >= lower3mlg, data$ad_data$Mileage * group3Factor, 0)))

  priceScore <- ifelse(data$ad_data$Price >= lower1price & data$ad_data$Price <= upper1price, data$ad_data$Price * group1Factor,
                       ifelse(data$ad_data$Price >= lower2price & data$ad_data$Price <= upper2price, data$ad_data$Price * group2Factor,
                              ifelse(data$ad_data$Price >= lower3price, data$ad_data$Price * group3Factor, 0)))

  cc_bhpScore <- if (length(data$ad_data$`Cubic capacity`) > 0) {
                    ifelse((data$ad_data$Power / data$ad_data$`Cubic capacity`) >= lower1cc_bhp & (data$ad_data$Power / data$ad_data$`Cubic capacity`) <= upper1cc_bhp, (data$ad_data$Power / data$ad_data$`Cubic capacity`) * group3Factor,
                           ifelse((data$ad_data$Power / data$ad_data$`Cubic capacity`) >= lower2cc_bhp & (data$ad_data$Power / data$ad_data$`Cubic capacity`) <= upper2cc_bhp, (data$ad_data$Power / data$ad_data$`Cubic capacity`) * group2Factor,
                                  ifelse((data$ad_data$Power / data$ad_data$`Cubic capacity`) >= lower3cc_bhp, (data$ad_data$Power / data$ad_data$`Cubic capacity`) * group1Factor, 0)))}
                else {0}

  data$ad_data$Score <-ageScore + mileageScore + priceScore + cc_bhpScore

  data <- toJSON(data, auto_unbox = TRUE)
  m$insert(data)
}

#2.2 How many bikes are there for sale?

#count all records in database
bikesForSale <- m$count('{}')
print(bikesForSale)

#2.3 What is the average price of a motorcycle (give a number)?
#What is the number of listings that were used in order to calculate this average (give a number as well)?
#Is the number of listings used the same as the answer in 1.2? Why?

#Aggregation to find average price of all ads with price greater than or equal to 100 euros
bikesAvgPrice <- m$aggregate(
  '[
  {"$match": {"ad_data.Price": { "$gte": 100 }}},
  {"$group":{"_id": null, "average":{"$avg":"$ad_data.Price"}}}
  ]'
)

#count the number of listings used
bikesUsedForAverage <-  nrow(m$aggregate('[
  {"$match": {"ad_data.Price": { "$gte": 100 }}}]'))

#print the results
print(bikesAvgPrice$average)
print(bikesUsedForAverage)

#2.4	What is the maximum and minimum price of a motorcycle currently available in the market?

#calculate the max price with aggregate()
maxPrice <- m$aggregate(
  '[
   {"$match": {"ad_data.Price": { "$gt": 0 }}},
   {"$group":{"_id": null, "max":{"$max":"$ad_data.Price"}}}
   ]'
)

#print the result
print(maxPrice$max)

#calculate the max price with aggregate()
minPrice <- m$aggregate(
  '[
   {"$match": {"ad_data.Price": { "$gt": 100 }}},
   {"$group":{"_id": null, "min":{"$min":"$ad_data.Price"}}}
   ]'
)

#print the result
print(minPrice$min)

#since we idenitfied as valid the ads with price >100 euros in q.2.3 we can say the minimum price is 100 euros

#2.5	How many listings have a price that is identified as negotiable?

#calculate the number of listings matching first the listings with price >0 and then with   aggregate() we count their number
negotiableBikes <- m$aggregate(
  '[
  {"$match": {"ad_data.Negotiable": true}},
  {"$group": { "_id": null, "negCount": { "$sum": 1 }}}
                   ]'
)
#print the result
print(negotiableBikes$negCount)

#2.6	For each Brand, what percentage of its listings is listed as negotiable?

#calculate the percentages with an aggregate pipeline where we group by brand and we count the total listings per brand
#and the number of negotiable listings per brand then we perform some numeric operations to have the desired results
negPercentage <- m$aggregate(
  '[
  { "$group": {
    "_id": "$metadata.brand",
    "totalCount": { "$sum": 1 },
    "negotiableCount": {
      "$sum": {
        "$cond": {
          "if":{ "$eq": [ "$ad_data.Negotiable", true ] },
          "then": 1,
          "else": 0
        }
      }
    }
  }},
  { "$addFields": {
    "negotiablePercentage": {
      "$cond": {
        "if": { "$ne": [ "$negotiableCount", 0 ] },
        "then": {
          "$multiply": [
            { "$divide": [ "$negotiableCount", "$totalCount" ] },
            100
            ]
        },
        "else": 0
      }
    }
  }},
  { "$sort": { "negotiablePercentage": -1 } }
  ]'
)

#print the result
print(negPercentage)

#2.7	What is the motorcycle brand with the highest average price?

#calculate the result with aggregate() by matching all listings with price >100 euros then group by brand
#and calculate the avg price and then sort in descending order and limit the result by one
bikesAvgHighestPrice <- m$aggregate(
  '[
  {"$match": {"ad_data.Price": { "$gte": 100 }}},
  {"$group":{"_id": "$metadata.brand", "average":{"$avg":"$ad_data.Price"}}},
  { "$sort": { "average": -1}},
  {"$limit": 1}
  ]'
)

#print the result
print(bikesAvgHighestPrice)

#2.8	What are the TOP 10 models with the highest average age? (Round age by one decimal number)

#in the same manner with the above question
top10highest <- m$aggregate(
  '[
               {"$match": {"ad_data.Mileage": { "$gt": 0 }}},
			         {"$group":{"_id": "$metadata.model","avgMileage": {"$avg":"$ad_data.Mileage"}, "avgAGE":{"$avg":"$ad_data.Age"}}},
               {"$sort": {"avgAGE":-1, "avgMileage":1}},
               {"$limit": 10}
                            ]'
)

top10highest <- select(top10highest, 1, 3)

#print the result
print(top10highest)

#2.9	How many bikes have "ABS" as an extra?

#matching the listings with ABS and then counting the total number of listings
abs <- m$aggregate('[
  {"$match": {"extras": "ABS"}},
  {"$group": {"_id": null, "ABSCount": {"$sum": 1}}}
                   ]')

#print the result
print(abs$ABSCount)

#2.10	What is the average Mileage of bikes that have "ABS" AND "Led lights" as an extra?

#in the same manner with the above question
absLed <- m$aggregate(
  '[
  {"$match": { "$and": [ {"extras": "ABS"}, {"extras":"Led lights"}]}},
  {"$group":{"_id": null, "absLedAvg":{"$avg": "$ad_data.Mileage"}}}
                    ]'
)

#print the result
print(absLed$absLedAvg)

#2.11	What are the TOP 3 colors per bike category?

#calculate the result with aggregate() by grouping by category and color and calculating the number of occurences per color
#then sorting by count in descending order and grouping again this time pushing the results in in new documents and projecting the elements we want
#by slicing the top 3 colors
top3colors <- m$aggregate(
  '[
           {"$group":
              {"_id": {"category": "$ad_data.Category","color": "$ad_data.Color"},"count": {"$sum": 1}}},
           {"$sort": {"_id.category": -1,"count": -1}},
           {"$group":
              {"_id":"$_id.category","topColors":{"$push":"$_id.color"}}},
           {"$project": { "category": "$_id.category", "top3Colors": { "$slice": [ "$topColors", 3 ] } } }
                      ]'
)

print(top3colors)

#2.12	Identify a set of ads that you consider "Best Deals".

#we match our target budget then sort by score and limit the result to 100 listings
bestDeals <- m$aggregate(
  '[
  {"$match": { "$and": [ {"ad_data.Price":  { "$gt": 0 }}, {"ad_data.Price":  { "$lte": 10000 }}]}},
  {"$sort": {"ad_data.Score": -1}},
  {"$limit": 100}
     ]'
)

#view the result
View(bestDeals)