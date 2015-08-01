library(testthat)

# default values
RS_HOST <- "fooBar.example.com"
RS_DB <- "fooBar"
RS_SCHEMA <- "fooBar"
RS_USER <- "fooBar"
RS_PASSWORD <- "fooBar"
DATA_DIR <- "../tests/data"

# override with config if any
if (file.exists("config.R")) {
    source("config.R")
}

# override with environment if any
if (nchar(Sys.getenv("RS_HOST")) > 0) {
    RS_HOST <- Sys.getenv("RS_HOST")  
}
if (nchar(Sys.getenv("RS_DB")) > 0) {
    RS_DB <- Sys.getenv("RS_DB")  
}
if (nchar(Sys.getenv("RS_SCHEMA")) > 0) {
    RS_SCHEMA <- Sys.getenv("RS_SCHEMA")  
}
if (nchar(Sys.getenv("RS_USER")) > 0) {
    RS_USER <- Sys.getenv("RS_USER")  
}
if (nchar(Sys.getenv("RS_PASSWORD")) > 0) {
    RS_PASSWORD <- Sys.getenv("RS_PASSWORD")  
}
if (nchar(Sys.getenv("DATA_DIR")) > 0) {
    DATA_DIR <- Sys.getenv("DATA_DIR")  
}


test_check("keboola.redshift.r.client")
