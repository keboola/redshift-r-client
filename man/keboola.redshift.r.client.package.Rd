\name{keboola.redshift.r.client}
\alias{keboola.redshift.r.client.package}
\docType{package}
\title{
Amazon Redshift database driver
}
\description{
A set of methods useful for connecting to Amazon Redshift database. These functions are mostly wrappers
around JDBC methods. The main advantage are methods for quick (bulk) inserting of data and retrieving 
of data into data frames. These functions are type aware.
}
\details{
\tabular{ll}{
Package: \tab keboola.redshift.r.client\cr
Type: \tab Package\cr
Version: \tab 1.0\cr
Date: \tab 2015-04-28\cr
License: \tab MIT\cr
}
}
\author{
Maintainer: Ondrej Popelka <ondrej.popelka@keboola.com>
}
\keyword{ database }
\examples{
\dontrun{
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
}
}
