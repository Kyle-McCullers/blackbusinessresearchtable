# Load the public BBRT snapshot in R and reproduce the headline numbers.
# install.packages(c("arrow", "dplyr"))
library(arrow); library(dplyr)

firms <- read_parquet("data/businesses.parquet")
headline <- firms |> filter(is.na(certification_status) | certification_status != "former")
cat(format(nrow(firms), big.mark = ","), "listings;", format(nrow(headline), big.mark = ","), "in the headline count\n")
headline |> count(address_state, sort = TRUE) |> head(10)
