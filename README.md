# Redshift R Driver
[![Build Status](https://travis-ci.org/keboola/redshift-r-client.svg?branch=master)](https://travis-ci.org/keboola/redshift-r-client)

Redshift R Driver provides a simple interface to query the Amazon Redshift database from R. 
Most methods are just wrappers around RJDBC methods. A specical method is used for inserting 
data frames into database which is done with compound INSERT statements which perform much better
than individual INSERT statements.

## Installation
Package is available only on Github, so you need to use `devtools` to install the package
```
library('devtools')
install_github('keboola/provisioning-r-client', ref = 'master')
```

## Usage
```
# connect to database
driver <- RedshiftDriver$new()     
driver$connect("myhost.example.com", "mydb", "user", "passwrod", "myschema")
    
# insert some data
df <- data.frame("foo" = c(1,3,5), "bar" = c("one", "three", "five"))
driver$saveDataFrame(df, "fooBar")

# select data (return a data.frame)
dfResult <- driver$select("SELECT foo, bar, row_num FROM fooBar ORDER BY foo")

# optionally insert row numbers from the data frame
driver$saveDataFrame(df, "fooBar", rowNumbers = TRUE)
dfResult <- driver$select("SELECT foo, bar, row_num FROM fooBar ORDER BY row_num")

# optionally insert data to an existing table
driver$update(
    paste0("CREATE TABLE ", schema, ".fooBar (foo INTEGER, bar CHARACTER VARYING (200));")
)
driver$saveDataFrame(df, "fooBar", incremental = TRUE)

# utility methods
cols <- driver$columnTypes(paste0(schema, ".fooBar"))
cols[["foo"]] # -> "integer"            
driver$tableExists("fooBar") # -> TRUE

# for DML and DML statements use the update method
driver$update("DROP TABLE IF EXISTS fooBar;")  
```
