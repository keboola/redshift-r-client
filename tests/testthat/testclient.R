library('keboola.redshift.r.client')

test_that("connect", {
    driver <- RedshiftDriver$new()     
    expect_equal(
        driver$connect(host, db, user, password, schema), 
        TRUE
    )
    expect_that(
        driver$connect("invalid", db, user, password, schema),
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
})

test_that("update", {
    driver <- RedshiftDriver$new()     
    expect_that(
        driver$update("CREATE TABLE foo (bar INTEGER);"), 
        throws_error()
    )
    driver$connect(host, db, user, password, schema)
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
    driver$connect(host, db, user, password, schema)
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
    driver$connect(host, db, user, password, schema)
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
    driver$connect(host, db, user, password, schema)
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
    driver$connect(host, db, user, password, schema)
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
    driver$update(paste0("CREATE TABLE ", schema, ".fooBar (bar INTEGER);"))
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
    driver$update(paste0("CREATE TABLE ", schema, ".fooBar (bar INTEGER);"))
    driver$update(paste0("CREATE VIEW ", schema, ".basBar AS (SELECT * FROM ", schema, ".fooBar);"))
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
    driver$update(paste0("CREATE TABLE ", schema, ".fooBar (bar INTEGER);"))
    # verify that the old table will not get deleted
    expect_that(
        driver$saveDataFrame(df, "fooBar", rowNumbers = FALSE, incremental = TRUE),
        throws_error()
    )
})
