library('keboola.redshift.r.client')

test_that("connect", {
    driver <- RedshiftDriver$new()     
    expect_equal(
        driver$connect(RS_HOST, RS_DB, RS_USER, RS_PASSWORD, RS_SCHEMA), 
        TRUE
    )
    expect_that(
        driver$connect("invalid", RS_DB, RS_USER, RS_PASSWORD, RS_SCHEMA),
        throws_error()
    )
})

test_that("prepare", {
    driver <- RedshiftDriver$new()     
    expect_equal(
        driver$prepareStatement("SELECT * FROM foo WHERE bar = ?", "baz"), 
        "SELECT * FROM foo WHERE bar = 'baz'"
    )
    expect_equal(
        driver$prepareStatement("SELECT * FROM foo WHERE bar = ?", "ba'z"), 
        "SELECT * FROM foo WHERE bar = 'ba''z'"
    )
    expect_equal(
        driver$prepareStatement("SELECT * FROM foo WHERE bar = ?", 42), 
        "SELECT * FROM foo WHERE bar = '42'"
    )
    expect_equal(
        driver$prepareStatement("SELECT * FROM foo WHERE bar = ? AND baz = ?", list(bar=42,baz=21)), 
        "SELECT * FROM foo WHERE bar = '42' AND baz = '21'"
    )
    expect_equal(
        driver$prepareStatement("SELECT * FROM foo WHERE bar = ? AND baz = ?", 42, 21), 
        "SELECT * FROM foo WHERE bar = '42' AND baz = '21'"
    )
})

test_that("update", {
    driver <- RedshiftDriver$new()     
    expect_that(
        driver$update("CREATE TABLE foo (bar INTEGER);"), 
        throws_error()
    )
    driver$connect(RS_HOST, RS_DB, RS_USER, RS_PASSWORD, RS_SCHEMA)
    driver$update("DROP TABLE IF EXISTS foo CASCADE;")
    
    expect_equal(
        driver$update("CREATE TABLE foo (bar INTEGER);"), 
        TRUE
    )
    expect_that(
        driver$update("CREATE TABLE foobar error;"), 
        throws_error()
    )
})

test_that("update", {
    driver <- RedshiftDriver$new()     
    expect_that(
        driver$update("CREATE TABLE foo (bar INTEGER);"), 
        throws_error()
    )
    driver$connect(RS_HOST, RS_DB, RS_USER, RS_PASSWORD, RS_SCHEMA)
    driver$update("DROP TABLE IF EXISTS foo CASCADE;")
    
    expect_equal(
        driver$update("CREATE TABLE foo (bar INTEGER);"), 
        TRUE
    )
    expect_that(
        driver$update("CREATE TABLE foobar error;"), 
        throws_error()
    )
})

test_that("tableExists", {
    driver <- RedshiftDriver$new()     
    driver$connect(RS_HOST, RS_DB, RS_USER, RS_PASSWORD, RS_SCHEMA)
    driver$update("DROP TABLE IF EXISTS foo CASCADE;")
    driver$update("CREATE TABLE foo (bar INTEGER);")
    
    expect_equal(
        driver$tableExists("foo"), 
        TRUE
    )
    expect_equal(
        driver$tableExists("non-existent-table"), 
        FALSE
    )
})

test_that("columnTypes", {
    driver <- RedshiftDriver$new()     
    driver$connect(RS_HOST, RS_DB, RS_USER, RS_PASSWORD, RS_SCHEMA)
    driver$update(paste0("DROP TABLE IF EXISTS ", driver$schema, ".foo CASCADE;"))
    driver$update(paste0("CREATE TABLE ", driver$schema, ".foo (bar INTEGER, baz CHARACTER VARYING (200));"))
    colTypes <- vector()
    colTypes[["bar"]] <- "integer"
    colTypes[["baz"]] <- "character varying"
    
    expect_equal(
        sort(driver$columnTypes('foo')),
        sort(colTypes)
    )
    expect_equal(
        length(driver$columnTypes("non-existent-table")), 
        0
    )
})

test_that("saveDataFrame", {
    driver <- RedshiftDriver$new()     
    driver$connect(RS_HOST, RS_DB, RS_USER, RS_PASSWORD, RS_SCHEMA)
    driver$update("DROP TABLE IF EXISTS fooBar CASCADE;")
    df <- data.frame("foo" = c(1,3,5), "bar" = c("one", "three", "five"))
    driver$saveDataFrame(df, "fooBar", rowNumbers = FALSE, incremental = FALSE)
    dfResult <- driver$select("SELECT foo, bar FROM fooBar ORDER BY foo")
    df[, "bar"] <- as.character(df[, "bar"])
    
    expect_equal(
        dfResult,
        df
    )

    driver$update("DROP TABLE IF EXISTS fooBar CASCADE;")
    df <- data.frame("foo" = c(1,3,5), "bar" = c("one", "three", "five"))
    driver$saveDataFrame(df, "fooBar", rowNumbers = TRUE, incremental = FALSE)
    dfResult <- driver$select("SELECT foo, bar, row_num FROM fooBar ORDER BY foo")
    df[, "bar"] <- as.character(df[, "bar"])
    df[['row_num']] <- c(1, 2, 3)

    expect_equal(
        dfResult,
        df
    )
    
    driver$update("DROP TABLE IF EXISTS fooBar CASCADE;")
    driver$update(paste0("CREATE TABLE ", RS_SCHEMA, ".fooBar (bar INTEGER);"))
    # verify that the old table will get deleted
    df <- data.frame("foo" = c(1,3,5), "bar" = c("one", "three", "five"))
    driver$saveDataFrame(df, "fooBar", rowNumbers = FALSE, incremental = FALSE)
    dfResult <- driver$select("SELECT foo, bar FROM fooBar ORDER BY foo")
    df[, "bar"] <- as.character(df[, "bar"])
    
    expect_equal(
        dfResult,
        df
    )
    
    driver$update("DROP TABLE IF EXISTS fooBar CASCADE;")
    driver$update(paste0("CREATE TABLE ", RS_SCHEMA, ".fooBar (bar INTEGER);"))
    driver$update(paste0("CREATE VIEW ", RS_SCHEMA, ".basBar AS (SELECT * FROM ", RS_SCHEMA, ".fooBar);"))
    # verify that the old table will get deleted even whant it has dependencies
    df <- data.frame("foo" = c(1,3,5), "bar" = c("one", "three", "five"))
    driver$saveDataFrame(df, "fooBar", rowNumbers = FALSE, incremental = FALSE)
    dfResult <- driver$select("SELECT foo, bar FROM fooBar ORDER BY foo")
    df[, "bar"] <- as.character(df[, "bar"])
    
    expect_equal(
        dfResult,
        df
    )
    
    driver$update("DROP TABLE IF EXISTS fooBar CASCADE;")
    driver$update(paste0("CREATE TABLE ", RS_SCHEMA, ".fooBar (bar INTEGER);"))
    # verify that the old table will not get deleted
    expect_that(
        driver$saveDataFrame(df, "fooBar", rowNumbers = FALSE, incremental = TRUE),
        throws_error()
    )
})

test_that("saveDataFrameFile", {
    driver <- RedshiftDriver$new()     
    driver$connect(RS_HOST, RS_DB, RS_USER, RS_PASSWORD, RS_SCHEMA)
    driver$update("DROP TABLE IF EXISTS fooBar CASCADE;")
    df <- read.csv(file.path(DATA_DIR, 'data1.csv'))
    df$timestamp <- as.POSIXlt(df$timestamp, tz = 'UTC')
    driver$saveDataFrame(df, "fooBar", rowNumbers = FALSE, incremental = FALSE)
    dfResult <- driver$select("SELECT \"timestamp\", anoms, expected_value FROM fooBar ORDER BY \"timestamp\";")
    dfResult$timestamp <- as.POSIXlt(df$timestamp, tz = 'UTC')
    expect_equal(nrow(df), nrow(df[which(dfResult$timestamp == df$timestamp),]))
})

test_that("saveDataFrameScientificNA", {
    driver <- RedshiftDriver$new()
    driver$connect(RS_HOST, RS_DB, RS_USER, RS_PASSWORD, RS_SCHEMA)
    driver$update("DROP TABLE IF EXISTS fooBar CASCADE;")
    df <- data.frame(
        id = c(1, 2, 6e+05),
        text = character(3),
        fact = c('fact1', 'fact2', 'fact2'),
        stringsAsFactors = FALSE
    )
    df[1, 'text'] <- 'a'
    df[2, 'text'] <- NA
    df[['fact']] <- factor(df[['fact']])
    driver$saveDataFrame(df, "fooBar", rowNumbers = FALSE, incremental = FALSE, forcedColumnTypes = list(id = "integer", text = "character"))
    dfResult <- driver$select("SELECT id, text FROM fooBar;")
    expect_equal(nrow(df), nrow(dfResult))
    
    driver <- RedshiftDriver$new()
    driver$connect(RS_HOST, RS_DB, RS_USER, RS_PASSWORD, RS_SCHEMA)
    driver$update("DROP TABLE IF EXISTS fooBar CASCADE;")
    df <- data.frame(
        id = c(1, 2, 6e+05, NA),
        fact = c(12, NA, NA, 3),
        stringsAsFactors = FALSE
    )
    driver$saveDataFrame(df, "fooBar", rowNumbers = FALSE, incremental = FALSE, forcedColumnTypes = list(id = "integer", fact = "character"))
    dfResult <- driver$select("SELECT id, fact FROM fooBar;")
    expect_equal(nrow(df), nrow(dfResult))
})

test_that("saveDataFrameLarge", {
    driver <- RedshiftDriver$new()     
    driver$connect(RS_HOST, RS_DB, RS_USER, RS_PASSWORD, RS_SCHEMA)
    driver$update("DROP TABLE IF EXISTS fooBar CASCADE;")
    df <- data.frame(a = rep('a', 10000), b = seq(1, 10000))
    driver$saveDataFrame(df, "fooBar", rowNumbers = FALSE, incremental = FALSE, displayProgress = FALSE)
    dfff <- driver$select("SELECT a,b FROM fooBar ORDER BY b;")
    dfResult <- driver$select("SELECT COUNT(*) AS cnt FROM fooBar;")
    expect_equal(dfResult[1, 'cnt'], nrow(df))
})