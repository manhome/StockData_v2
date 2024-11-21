setwd("F:/App/R/StockData_trial")

library(xts)
library(dplyr)
#library(purrr)
library(openxlsx)

PctChg <- function(x) {
#    y <- x / lag(na.locf(x, na.rm=FALSE)) - 1
    x0 <- lag(na.locf(x, na.rm=FALSE))
    y <- (x - x0)/abs(x0)
    y[is.infinite(y)|is.nan(y)] <- NA
    return (y)
}

LagCumChg <- function(x) {
    x[is.na(x)] <- 0
    y <- cumprod(1/(1+x))
    y <- lag(y)
    y[is.na(y)] <- 1
    return (y)
}

LagCumChg_NA <- function(x) {
	y <- LagCumChg(x)
    y[is.na(x)] <- NA
    return (y)
}

chrFile <- "testData.xlsx"
tmp_obj <- readWorkbook(chrFile, sheet=1, detectDates=TRUE)
#tmp_obj1 <- readWorkbook(chrFile, sheet=1, detectDates=FALSE)

tmp_dfPctChg <- tmp_obj %>%
                select(Date, SecID, Field_1, Field_2, Field_3, Field_4) %>%
                group_by(SecID) %>%
                arrange(Date, .by_group=TRUE) %>%
                mutate(across(-any_of(c("SecID", "Date")), ~ PctChg(.)))
