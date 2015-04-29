library('keboola.redshift.r.client')

test_that("str_length is number of characters", {
     ;
    expect_equal(
        redshift.connect(paste0("jdbc:postgresql://", hostname, ":5439",  "/", db), user, password, schema), 
        TRUE
    )
})
