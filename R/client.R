#' Class to access Redshift database

#' @import methods RJDBC rJava
#' @export RedshiftDriver
#' @exportClass RedshiftDriver
#' @field conn Database connection (JDBCConnection)
#' @field schema Current database schema
RedshiftDriver <- setRefClass(
    'RedshiftDriver',
    fields = list(
        conn = 'ANY', # JDBCConnection | NULL
        schema = 'character'
    ),
    methods = list(
        initialize = function() {
            conn <<- NULL
            schema <<- ""
        },
        
        #' Connect to Amazon Redshift database
        #' 
        #' @param jdbcUrl JDBC connection string
        #' @param username Database user name
        #' @param password Database password
        #' @param schema Database schema
        #' @exportMethod
        #' @return TRUE
        connect = function(host, db, user, password, schema, port = 5439) {
            libPath <- system.file("lib", "postgresql-9.1-901.jdbc4.jar", package = "keboola.redshift.r.client")
            driver <- JDBC("org.postgresql.Driver", libPath, identifier.quote = '"')
            jdbcUrl <- paste0("jdbc:postgresql://", host, ":", port,  "/", db)
            # if url has GET parameters already, then concat name and password after &
            lead <- ifelse(grepl("\\?", jdbcUrl), "&", "?")
            url <- paste0(jdbcUrl, lead, "user=", user, "&password=", password)
            conn <<- dbConnect(driver, url)
            schema <<- schema
            TRUE
        },
        
        #' Prepare a SQL query with quoted parameters
        #' 
        #' @param sql SQL string, parameter placeholders are marked with ?
        #' @param ... Query parameters, number of parameters must be same as number of 
        #'  question marks
        #' @exportMethod
        #' @return SQL string
        prepareStatement = function(sql, ...) {
            parameters <- list(...)
            quotedParameters <- lapply(
                X = parameters, 
                function (value) {
                    # escape the quotes (if any) in a value
                    value <- gsub("'", "''", value)
                    # quote the value
                    value <- paste0("'", value, "'")
                }
            )
            quotedParameters <- unlist(quotedParameters)
            if (length(quotedParameters) > 0) {
                for (i in 1:length(quotedParameters)) {
                    sql <- sub("\\?", quotedParameters[[i]], sql)
                }
            }
            sql
        },
        
        #' Select data from database
        #' 
        #' @param sql Query string, may contain placeholders ? for parameters
        #' @param ... Query parameters
        #' @exportMethod
        #' @return A dataframe with results
        select = function(sql, ...) {
            sql <- prepareStatement(sql, ...)
            tryCatch(
                {
                    print(paste("attempting query:",sql))
                    ret <- dbGetQuery(conn, sql)
                },
                error = function(e) {
                    stop(paste0("Failed to execute query ", e, " q: (", sql, ") "))
                }
            )
            ret
        },
        
        #' Select via JDBS result set fetching to avoid memory restraints
        #' 
        #' @param statement Prepared Query statement
        #' @param maxmem Upper limit in bytes of read - default 500MB
        #' @param chunksize Rows to return per fetch - default 32k for 1st fetch, then 512k
        #' @exportMethod
        #' @return data.frame
        fetch = function(statement, maxmem = 500000000, chunksize = -1) {
            out <- data.frame()
            print(paste("sending query",maxmem,"maxmem", chunksize, "chunksize"))
            results <- RJDBC::dbSendQuery(conn, statement)
            print("query sent")
            print(paste("resultInfo", dbGetInfo(results)))
            partialResults <- TRUE
            tryCatch(
            {
                while (object.size(out) < maxmem && partialResults) {
                    print("trying to fetch")
                    partialResults <- fetch(results, chunksize)
                    print(paste("fetched res with size", nrow(partialResults)))
                    if (partialResults) {
                        out <- rbind(out, partialResults)    
                    }
                }
            }, error = function(e) {
                stop(paste("Error fetching data", e))
            })
            out
        },
        
        #' Update/Insert data to database
        #' 
        #' @param sql Query string, may contain placeholders ? for parameters
        #' @param ... Query parameters
        #' @exportMethod
        #' @return TRUE
        update = function(sql, ...) {
            sql <- prepareStatement(sql, ...)
            tryCatch(
                {
                    ret <- dbSendUpdate(conn, sql)
                },
                error = function(e) {
                    stop(paste0("Failed to execute query ", e, " q: (", sql, ") "))
                }
            )
            TRUE
        },

        #' Save a dataframe to database using bulk inserts. The table will be created to accomodate to data frame columns.
        #'
        #' @param dfRaw A data frame, column names of data frame must correspond to column names of table
        #' @param table Name of the table.
        #' @param rowNumbers If true then the table will contain a column named 'row_num' with sequential row index
        #' @param incremental If true then the table will not be recreated, only data will be inserted
        #' @param forcedColumnTypes List of column names and their respective types in database.
        #' @exportMethod
        #' @return void
        saveDataFrame = function(dfRaw, table, rowNumbers = FALSE, incremental = FALSE, forcedColumnTypes) {
            # drop the table if already exists and loading is not incremental
            tableFull <- paste0(schema, '.', table)
            if (!incremental) {
                # convert factors to strings
                df <- data.frame(lapply(colnames(dfRaw), function(colname)
                {
                    if (is.factor(dfRaw[[colname]])) {
                        as.character(dfRaw[[colname]])
                    } else {
                        dfRaw[[colname]]
                    }
                }
                ), stringsAsFactors = FALSE)
                colnames(df) <- colnames(dfRaw)
                
                # get column types
                types <- lapply(df, typeof)
                classes <- lapply(df, class)
                
                # convert column types to database types and create list of column defininitions
                if (rowNumbers) {
                    columns <- list("row_num INTEGER")
                } else {
                    columns <- list()		
                }
                for (name in names(types)) {
                    type <- "";
                    if ('POSIXt' %in% classes[[name]]) {
                        # handles both POSIXct and POSIXlt as POSIXt is common ancestor
                        type <- 'TIMESTAMP'
                    } else if (types[[name]] == 'double') {
                        type <- 'DECIMAL (30,20)'
                    } else if (types[[name]] == 'integer') {
                        type <- 'BIGINT'
                    } else if (types[[name]] == 'logical') {
                        type <- 'INTEGER'
                    } else if (types[[name]] == 'character') {
                        type <- 'VARCHAR(2000)'
                    } else if (types[[name]] == 'NULL') {
                        type <- 'INTEGER'
                    } else {
                        stop(paste0("Unknown type: ", types[[name]], ' or class: ', classes[[name]]))
                    }
                    
                    if (!missing(forcedColumnTypes) && (name %in% names(forcedColumnTypes))) {
                        type <- as.character(forcedColumnTypes[[name]])
                    }
                    if (type == "") {
                        stop(paste0("Unhandled column type ", types[[name]]))
                    }
                    columns <- c(columns, paste0('"', name, '" ', type))
                }
                # drop the table if necessary
                if (tableExists(table)) {
                    update(paste0("DROP TABLE ", tableFull, " CASCADE;"))
                }
                # create the table
                sql <- paste0("CREATE TABLE ", tableFull, " (", paste(columns, collapse = ", "), ");")
                update(sql)
            } else {
                df <- dfRaw
            }
            
            # Maximum size of a statement is 16MB http://docs.aws.amazon.com/redshift/latest/dg/c_redshift-sql.html	
            # set the limit a little bit lower, because the counting is not precise
            sqlLimit <- 5000000 
            # create query header
            colNames <- colnames(df)
            colNames <- lapply(
                X = colNames,
                function (value) {
                    value <- paste0('"', value, '"')
                }
            )
            if (rowNumbers) {
                sqlHeader <- paste0("INSERT INTO ", tableFull, " (row_num, ", paste(colNames, collapse = ", "), ") VALUES ")
            } else {
                sqlHeader <- paste0("INSERT INTO ", tableFull, " (", paste(colNames, collapse = ", "), ") VALUES ")			
            }
            
            # list for row values
            sqlVals <- list()
            sqlLength <- 0
            
            if (nrow(df) > 0) {
                # data frame is non-empty 
                for (i in 1:nrow(df)) {
                    # save row so as not to modify the original data frame
                    row <- df[i,]
                    # escape and quote all values
                    row <- lapply(
                        X = row, 
                        function (value) {
                            if (is.null(value) || is.na(value)) {
                                value <- "NULL"
                            } else {
                                # escape the quotes (if any) in a value
                                value <- gsub("'", "''", value)
                                # quote the value
                                value <- paste0("'", value, "'")
                            }
                        }
                    )
                    # produce a single row of values
                    if (rowNumbers) {
                        sqlVal <- paste0("('", i, "', ", paste(row, collapse = ", "), ")")
                    } else {
                        sqlVal <- paste0("(", paste(row, collapse = ", "), ")")
                    }
                    # store the row in list
                    sqlVals <- c(sqlVals, sqlVal)
                    # keep track of length of the list
                    sqlLength <- sqlLength + nchar(sqlVal)
                    if (sqlLength > sqlLimit) {
                        # query length is over limit, execute it
                        sql <- paste0(sqlHeader, paste(sqlVals, collapse = ", "))
                        update(sql)
                        # clear row values
                        sqlLength <- 0
                        sqlVals <- list()
                    }
                }
            }
            # if there are some rows left, insert them 
            if (sqlLength > 0) {
                sql <- paste0(sqlHeader, paste(sqlVals, collapse = ", "))
                update(sql)
            }
            TRUE
        },
        
        #' Verify that a table exists in database
        #'
        #' @param tableName Name of the table (without schema).
        #' @return logical TRUE if the table exists, FALSE otherwise.
        #' @exportMethod
        tableExists = function(tableName) {
            res <- select("SELECT COUNT(*) AS count FROM information_schema.tables WHERE table_schema ILIKE ? AND table_name ILIKE ?;", schema, tableName);
            ret <- res[1, 'count'] > 0
            ret
        },
        
        #' Get list of columns in table and their datatypes
        #' 
        #' @param tableName Name of the table (without schema).
        #' @return Named vector, name is column name, value is datatype
        #' @exportMethod
        columnTypes = function(tableName) {
            ret <- select("SELECT column_name, data_type FROM information_schema.columns WHERE (table_schema ILIKE ?) AND (table_name ILIKE ?);", schema, tableName);
            colnames(ret) <- c('column', 'dataType')    
            retVector <- as.vector(ret[,'dataType'])
            names(retVector) <- ret[,'column']
            retVector
        }        
    )
)
