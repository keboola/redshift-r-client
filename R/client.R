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
        
        connect = function(host, db, user, password, schema, port = 5439) {
            "Connect to Amazon Redshift database.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{jdbcUrl} JDBC connection string.}
            \\item{\\code{username} Database user name.}
            \\item{\\code{password} Database password.}
            \\item{\\code{schema} Database schema.}
            \\item{\\code{port} Database server port.}
            }}
            \\subsection{Return Value}{TRUE}"
            #libPath <- system.file("lib", "RedshiftJDBC41-1.1.10.1010.jar", package = "keboola.redshift.r.client")
            #driver <- JDBC("com.amazon.redshift.jdbc41.Driver", libPath, identifier.quote = '"')
            #jdbcUrl <- paste0("jdbc:redshift://", host, ":", port,  "/", db)
            libPath <- system.file("lib", "postgresql-9.4.1208.jre7.jar", package = "keboola.redshift.r.client")
            driver <- JDBC("org.postgresql.Driver", libPath, identifier.quote = '"')
            jdbcUrl <- paste0("jdbc:postgresql://", host, ":", port,  "/", db)
            
            # if url has GET parameters already, then concat name and password after &
            lead <- ifelse(grepl("\\?", jdbcUrl), "&", "?")
            url <- paste0(jdbcUrl, lead, "user=", user, "&password=", password)
            conn <<- dbConnect(driver, url)
            schema <<- schema
            TRUE
        },
        
        prepareStatement = function(sql, ...) {
            "Prepare a SQL query with quoted parameters.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{sql} SQL string, parameter placeholders are marked with ?.}
            \\item{\\code{...} Query parameters, number of parameters must be same as number of question marks.}
            }}
            \\subsection{Return Value}{SQL string}"
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
        
        select = function(sql, ...) {
            "Select data from database.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{sql} Query string, may contain placeholders ? for parameters.}
            \\item{\\code{...} Query parameters, number of parameters must be same as number of question marks.}
            }}
            \\subsection{Return Value}{A data.frame with results}"
            sql <- prepareStatement(sql, ...)
            tryCatch(
                {
                    ret <- dbGetQuery(conn, sql)
                },
                error = function(e) {
                    stop(paste0("Failed to execute query ", e, " q: (", sql, ") "))
                }
            )
            ret
        },
        
        fetch = function(statement, maxmem = 500000000, chunksize = -1) {
            "Select via JDBC result set fetching to avoid memory restraints.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{statement} Prepared Query statement.}
            \\item{\\code{maxmem} Upper limit in bytes of read - default 500MB.}
            \\item{\\code{chunksize} Rows to return per fetch - default 32k for 1st fetch, then 512k.}
            }}
            \\subsection{Return Value}{A data.frame with results}"
            out <- data.frame()
            results <- RJDBC::dbSendQuery(conn, statement)
            partialResults <- TRUE
            tryCatch(
            {
                while (object.size(out) < maxmem && partialResults) {
                    partialResults <- fetch(results, chunksize)
                    if (partialResults) {
                        out <- rbind(out, partialResults)    
                    }
                }
            }, error = function(e) {
                stop(paste("Error fetching data", e))
            })
            out
        },
        
        update = function(sql, ...) {
            "Update/Insert data to database.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{sql} Query string, may contain placeholders ? for parameters.}
            \\item{\\code{...} Query parameters.}
            }}
            \\subsection{Return Value}{TRUE}"
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

        saveDataFrame = function(dfRaw, table, rowNumbers = FALSE, incremental = FALSE, forcedColumnTypes, displayProgress) {
            "Save a dataframe to database using bulk inserts. The table will be created to accomodate to data frame columns.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{dfRaw} A data.frame, column names of data frame must correspond to column names of table.}
            \\item{\\code{table} Name of the table.}
            \\item{\\code{rowNumbers} If true then the table will contain a column named 'row_num' with sequential row index}
            \\item{\\code{incremental} If true then the table will not be recreated, only data will be inserted.}
            \\item{\\code{forcedColumnTypes} Optional list of column names and their respective types in database.}
            \\item{\\code{displayProgress} Optional logical, if set to true, saving progress will be displayed.}
            }}
            \\subsection{Return Value}{TRUE}"
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
                    type <- types[[name]];
                    if ('POSIXt' %in% classes[[name]]) {
                        # handles both POSIXct and POSIXlt as POSIXt is common ancestor
                        type <- 'POSIXt'
                    }
                    if (!missing(forcedColumnTypes) && (name %in% names(forcedColumnTypes))) {
                        type <- as.character(forcedColumnTypes[[name]])
                    }
                    
                    if (type == 'POSIXt') {
                        type <- 'TIMESTAMP'
                    } else if (type == 'double') {
                        type <- 'DECIMAL (30,20)'
                    } else if (type == 'integer') {
                        type <- 'BIGINT'
                    } else if (type == 'logical') {
                        type <- 'INTEGER'
                    } else if (type == 'character') {
                        type <- 'VARCHAR(2000)'
                    } else if (type == 'NULL') {
                        type <- 'INTEGER'
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
            rowLimit <- 5000
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
            
            if (nrow(df) > 0) {
                # data frame is non-empty
                cntr <- 0
                from <- 1
                to <- rowLimit
                while (TRUE) {
                    ptm <- proc.time()
                    rows <- df[from:min(nrow(df), to), ]
                    rows <- sapply(rows, function(col) {
                        if (is.numeric(col)) {
                            col <- ifelse(is.na(col) | is.null(col), NA, format(col, scientific = FALSE))
                        } else {
                            col <- as.character(col)
                        }
                        # put in literal null if empty value or escape the quotes (if any) in a value and quote it
                        col <- ifelse(is.na(col) | is.null(col), 'NULL', paste0("'", gsub("'", "''", col), "'"))
                        col
                    })
                    # if the initial dataframe contains only a single row, it will get 
                    # coerced into a vector by sapply, bring back the matrix now:
                    if (class(rows) != 'matrix') {
                        cn <- colnames(rows)
                        rows <- matrix(rows, nrow = nrow(df), byrow = FALSE)
                        colnames(rows) <- cn
                    }
                    
                    if (rowNumbers) {
                        assign("rowCounter", 1, envir = .GlobalEnv)
                    }
                    sqlVals <- apply(rows, MARGIN = 1, FUN = function(row) {
                        # produce a single row of values
                        if (rowNumbers) {
                            row <- paste0("('", rowCounter, "', ", paste(row, collapse = ", "), ")")
                            assign("rowCounter", rowCounter + 1, envir = .GlobalEnv)
                        } else {
                            row <- paste0("(", paste(row, collapse = ", "), ")")
                        }
                        row
                    })
                    sql <- paste0(sqlHeader, paste(sqlVals, collapse = ", "))
                    update(sql)
                    tm <- (proc.time() - ptm)[['elapsed']]
                    if (!missing(displayProgress) && displayProgress) {
                        write(paste0("Saved row: ", to, " tm: ", tm, " r/s:", rowLimit / tm), stdout())
                    }
                    # clear row values
                    ptm <- proc.time()
                    from <- to + 1
                    to <- from + rowLimit
                    if (from > nrow(df)) {
                        break;
                    }
                }
            }
            TRUE
        },
        
        tableExists = function(tableName) {
            "Verify that a table exists in database.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{tableName} Name of the table (without schema).}
            }}
            \\subsection{Return Value}{TRUE if the table exists, FALSE otherwise.}"
            res <- select("SELECT COUNT(*) AS count FROM information_schema.tables WHERE table_schema ILIKE ? AND table_name ILIKE ?;", schema, tableName);
            ret <- res[1, 'count'] > 0
            ret
        },
        
        columnTypes = function(tableName) {
            "Get list of columns in table and their datatypes.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{tableName} Name of the table (without schema).}
            }}
            \\subsection{Return Value}{Named vector, name is column name, value is datatype.}"
            ret <- select("SELECT column_name, data_type FROM information_schema.columns WHERE (table_schema ILIKE ?) AND (table_name ILIKE ?);", schema, tableName);
            colnames(ret) <- c('column', 'dataType')    
            retVector <- as.vector(ret[,'dataType'])
            names(retVector) <- ret[,'column']
            retVector
        }        
    )
)
