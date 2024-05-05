#Read in full data file
df1 <- read.csv("Iowa_Liquor_Sales.csv")


df1_copy <- df1 

str(df1_copy)

#Change Data type of Date from CHR to Date
df1_copy$Date <- as.Date(df1$Date, format = "%m/%d/%Y")
str(df1_copy)

#Change data type of store.number to CHR
df1_copy$Store.Number <- as.character(df1_copy$Store.Number)
str(df1_copy)

#Change data type of county.number to CHR
df1_copy$County.Number <- as.character(df1_copy$County.Number)
str(df1_copy)

#Change data type of category to CHR
df1_copy$Category <- as.character(df1_copy$Category)
str(df1_copy)

df1_copy$Vendor.Number <- as.character(df1_copy$Vendor.Number)
str(df1_copy)

df1_copy$Item.Number <- as.character(df1_copy$Item.Number)
str(df1_copy)


df1_copy$Bottle.Volume..ml. <- gsub(",", "", df1_copy$Bottle.Volume..ml.)

# Convert to numeric (double)
df1_copy$Bottle.Volume..ml. <- as.numeric(df1_copy$Bottle.Volume..ml.)
str(df1_copy)

#Change bottle cost
df1_copy$State.Bottle.Cost <- gsub(",", "", df1_copy$State.Bottle.Cost)

# Convert to numeric (double)
df1_copy$State.Bottle.Cost <- as.numeric(df1_copy$State.Bottle.Cost)
str(df1_copy)


# Assuming your dataframe is named df1_copy

# Remove commas from the values and convert to numeric (double)
df1_copy$State.Bottle.Retail <- as.numeric(gsub(",", "", df1_copy$State.Bottle.Retail))
df1_copy$Bottles.Sold <- as.numeric(gsub(",", "", df1_copy$Bottles.Sold))
df1_copy$Sale..Dollars. <- as.numeric(gsub(",", "", df1_copy$Sale..Dollars.))
df1_copy$Volume.Sold..Liters. <- as.numeric(gsub(",", "", df1_copy$Volume.Sold..Liters.))
df1_copy$Volume.Sold..Gallons. <- as.numeric(gsub(",", "", df1_copy$Volume.Sold..Gallons.))
df1_copy$Pack <- as.numeric(df1_copy$Pack)
str(df1_copy)





#split data frames into 4 smaller ones
num_records <- nrow(df1_copy)
num_per_frame <- ceiling(num_records / 4)

# Generate a sequence of indices to split the dataframe
indices <- rep(1:4, each = num_per_frame)
indices <- indices[1:num_records]

# Split the dataframe into 4 smaller data frames
daf1 <- df1_copy[indices == 1, ]
daf2 <- df1_copy[indices == 2, ]
daf3 <- df1_copy[indices == 3, ]
daf4 <- df1_copy[indices == 4, ]

#check str of dataframes
str(daf1)
str(daf2)
str(daf3)
str(daf4)

#write to csv files 
write.table(daf1, file = "Iowa_Liquor_Sales1.csv", row.names = F, col.names = F, sep = "|")
write.table(daf2, file = "Iowa_Liquor_Sales2.csv", row.names = F, col.names = F, sep = "|")
write.table(daf3, file = "Iowa_Liquor_Sales3.csv", row.names = F, col.names = F, sep = "|")
write.table(daf4, file = "Iowa_Liquor_Sales4.csv", row.names = F, col.names = F, sep = "|")

