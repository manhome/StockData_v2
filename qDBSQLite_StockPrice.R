qDBSQLite_Stocks_Update <- function(dbconn, dfData)
{
    tmp_chrTable <- "tmpStocks"

    cat(sprintf("\tCreate Table %s\n", tmp_chrTable))
    dbWriteTable(dbconn, tmp_chrTable, dfData, overwrite=TRUE, temporary=TRUE)

    dbBegin(dbconn)

    cat(sprintf("\tDelete from %s\n", tmp_chrTable))
#    chrSQL <- lstSQL$Stocks_Delete_Temp
    chrSQL <- "DELETE FROM Stocks
                WHERE EXISTS(SELECT 1 FROM tmpStocks a2
                WHERE a2.SecID = Stocks.SecID)"
    rc <- tryCatch(
            qDB_SendStatement(dbconn, chrSQL),
            error=function(e) {
                cat(sprintf("\t%s\n", e))
                return (-1)
            })
    if (rc!=0) {
        dbRollback(dbconn)
        return (-1)
    } # if

    cat(sprintf("\tInsert from %s\n", tmp_chrTable))
#    chrSQL <- lstSQL$Stocks_Insert_Temp
    chrSQL <- "INSERT INTO Stocks (SecID, Name, Ccy, Exch_yahoo, Exch_quandl, SecID_yahoo, SecID_quandl)
                SELECT SecID, Name, Ccy, Exch_yahoo, Exch_quandl, SecID_yahoo, SecID_quandl
                FROM tmpStocks a1
                WHERE NOT EXISTS(SELECT 1 FROM Stocks a2 WHERE a2.SecID = a1.SecID)"
    rc <- tryCatch(
            qDB_SendStatement(dbconn, chrSQL),
            error=function(e) {
                cat(sprintf("\t%s\n", e))
                return (-1)
            })
    if (rc!=0) {
        dbRollback(dbconn)
        return (-1)
    } # if

    dbCommit(dbconn)
    return (0)
}

qDBSQLite_StockPriceChg_SecID <- function(dbconn, dfData)
{
    tmp_obj <- NULL

    tmp_chrTable <- "tmpStockPrices"

    dbWriteTable(dbconn, tmp_chrTable, dfData, overwrite=TRUE, temporary=TRUE)

    cat(sprintf("\tCreate Index %s\n", tmp_chrTable))
#    chrSQL <- lstSQL$tmpStockPrices_Create_Index
    chrSQL <- "CREATE INDEX idx_tmpStockPrices ON tmpStockPrices (SecID, PriceDate, Close, AdjClose, Volume)"
    rc <- tryCatch(
            qDB_SendStatement(dbconn, chrSQL),
            error=function(e) {
                cat(sprintf("\t%s\n", e))
                return (-1)
            })
    if (rc!=0) {
        return (NULL)
    } # if

#    chrSQL <- lstSQL$tmpStockCloseChg_SecID
    chrSQL <- "SELECT SecID, MIN(PriceDate) FROM StockPrices s0
                WHERE EXISTS(SELECT 1 FROM tmpStockPrices t1
                    WHERE t1.SecID = s0.SecID
                    AND t1.PriceDate = s0.PriceDate
                    AND abs(s0.Close - t1.Close) >= 0.005)
                    GROUP BY SecID ORDER BY SecID"
    tmp_df <- tryCatch(
                qDB_SendStatement(dbconn, chrSQL),
                error=function(e) {
                    cat(sprintf("\t%s\n", e))
                    return (NULL)
                })

    if (is.data.frame(tmp_df)&&(!is.null(tmp_df))&&(nrow(tmp_df)>0)) {
        cat(sprintf("\tClose of '%s' changed since %s\n", tmp_df[[1]], tmp_df[[2]]))
        tmp_obj <- rbind(tmp_obj, tmp_df)
    } # if

#    chrSQL <- lstSQL$tmpStockAdjCloseChg_SecID
    chrSQL <- "SELECT SecID, MIN(PriceDate) FROM StockPrices s0
                WHERE EXISTS(SELECT 1 FROM tmpStockPrices t1
                    WHERE t1.SecID = s0.SecID
                    AND t1.PriceDate = s0.PriceDate
                    AND abs(s0.AdjClose - t1.AdjClose) >= 0.005)
                    GROUP BY SecID ORDER BY SecID"
    tmp_df <- tryCatch(
                qDB_SendStatement(dbconn, chrSQL),
                error=function(e) {
                    cat(sprintf("\t%s\n", e))
                    return (NULL)
                })

    if (is.data.frame(tmp_df)&&(!is.null(tmp_df))&&(nrow(tmp_df)>0)) {
        cat(sprintf("\tAdjClose of '%s' changed since %s\n", tmp_df[[1]], tmp_df[[2]]))
        tmp_obj <- rbind(tmp_obj, tmp_df)
    } # if

    if ((!is.null(tmp_obj))&&(nrow(tmp_obj)>0)) {
        tmp_obj <- sort(unique(tmp_obj[[1]]))
    } # if

    return (tmp_obj)
}

qDBSQLite_tmpYahooChg_Select <- function(dbconn, dfData)
{
    tmp_obj <- NULL

    # Get SecID having Close, AdjClose Price changes
    tmp_chrSecID <- qDBSQLite_StockPriceChg_SecID(dbconn, dfData)
    if (is.null(tmp_chrSecID)) {
        return (NULL)
    } #if

    tmp_chrTable <- "tmpSecID"
    tmp_dfData <- data.frame(SecID=tmp_chrSecID, stringsAsFactors=FALSE)

    dbWriteTable(dbconn, tmp_chrTable, tmp_dfData, overwrite=TRUE, temporary=TRUE)

    cat(sprintf("\tCreate Index %s\n", tmp_chrTable))
#    chrSQL <- lstSQL$tmpSecID_Create_Index
    chrSQL <- "CREATE INDEX idx_tmpSecID ON tmpSecID (SecID)"
    rc <- tryCatch(
            qDB_SendStatement(dbconn, chrSQL),
            error=function(e) {
                cat(sprintf("\t%s\n", e))
                return (-1)
            })
    if (rc!=0) {
        return (-1)
    } # if

#    chrSQL <- lstSQL$tmpYahooChg_Select_SecID
    chrSQL <- "SELECT SecID, PriceDate, Open, High, Low, Close, Volume, AdjClose
                FROM YahooChg a1
                WHERE EXISTS(SELECT 1 FROM tmpSecID t1 WHERE t1.SecID = a1.SecID)"
    tmp_obj <- tryCatch(
                qDB_SendStatement(dbconn, chrSQL),
                error=function(e) {
                    cat(sprintf("\t%s\n", e))
                    return (NULL)
                })

    return (tmp_obj)
}

qDBSQLite_YahooChg_Update <- function(dbconn, dfData)
{
    tmp_chrTable <- "tmpYahooChg"
    tmp_dfData <- dfData
    for (colName in c("OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "ADJUSTED")) {
        k1 <- which(is.nan(tmp_dfData[[colName]]))
        k2 <- which(is.infinite(tmp_dfData[[colName]]))
        k <- union(k1, k2)
        j <- which(names(tmp_dfData)==colName)
        if ((length(k) > 0)&&(length(j) > 0)) {
            tmp_dfData[k,j] <- NA
        } # if
    } # for (colName

    PctChg <- function(x) {
        # assign NA to 0 as well
        x[which(x==0)] <- NA
        x0 <- lag(na.locf(x, na.rm=FALSE))
        y <- (x - x0) / abs(x0)
        y[is.infinite(y)|is.nan(y)] <- NA
        return (y)
    }

    tmp_dfOHLC <- tmp_dfData %>%
                    select(SecID, PriceDate, OPEN, HIGH, LOW, CLOSE, ADJUSTED) %>%
                    group_by(SecID) %>%
                    arrange(PriceDate, .by_group=TRUE) %>%
#                    mutate_at(vars(-SecID, -PriceDate), ~ PctChg(.))
                    mutate(across(-any_of(c("SecID", "PriceDate")), ~ PctChg(.)))

    tmp_dfV <- tmp_dfData %>%
                filter(!is.na(VOLUME) & (VOLUME > 0)) %>%
                select(SecID, PriceDate, VOLUME) %>%
                group_by(SecID) %>%
                arrange(PriceDate, .by_group=TRUE) %>%
#                mutate_at(vars(-SecID, -PriceDate), ~ PctChg(.))
                mutate(across(-any_of(c("SecID", "PriceDate")), ~ PctChg(.)))

    tmp_dfOHLCV <- tmp_dfOHLC %>%
                    left_join(tmp_dfV, by=c("SecID", "PriceDate"))

    cat(sprintf("\tCreate Temporary Table %s\n", tmp_chrTable))
    dbWriteTable(dbconn, tmp_chrTable, tmp_dfOHLCV, overwrite=TRUE, temporary=TRUE)

    cat(sprintf("\tCreate Index %s\n", tmp_chrTable))
#    chrSQL <- lstSQL$tmpYahooChg_Create_Index
    chrSQL <- "CREATE INDEX idx_tmpYahooChg ON tmpYahooChg (SecID, PriceDate)"
    rc <- tryCatch(
            qDB_SendStatement(dbconn, chrSQL),
            error=function(e) {
                cat(sprintf("\t%s\n", e))
                return (-1)
            })
    if (rc!=0) {
        return (-1)
    } # if

    dbBegin(dbconn)

    cat(sprintf("\tDelete YahooChg from %s\n", tmp_chrTable))
#    chrSQL <- lstSQL$YahooChg_Delete_Temp
    chrSQL <- "DELETE FROM YahooChg
                WHERE EXISTS(SELECT 1 FROM tmpYahooChg a2
                    WHERE a2.SecID = YahooChg.SecID
                    AND YahooChg.PriceDate > (SELECT MIN(a3.PriceDate) FROM tmpYahooChg a3
                        WHERE a3.SecID = a2.SecID))"
    rc <- tryCatch(
            qDB_SendStatement(dbconn, chrSQL),
            error=function(e) {
                cat(sprintf("\t%s\n", e))
                return (-1)
            })
    if (rc!=0) {
        dbRollback(dbconn)
        return (-1)
    } # if

    cat(sprintf("\tInsert YahooChg from %s\n", tmp_chrTable))
#    chrSQL <- lstSQL$YahooChg_Insert_Temp
    chrSQL <- "INSERT INTO YahooChg (SecID, PriceDate, Open, High, Low, Close, Volume, AdjClose)
                SELECT DISTINCT SecID, PriceDate, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJUSTED
                FROM tmpYahooChg a1
                WHERE a1.CLOSE IS NOT NULL
                AND NOT EXISTS(SELECT 1 FROM YahooChg a2
                    WHERE a2.SecID = a1.SecID AND a2.PriceDate = a1.PriceDate)"
    rc <- tryCatch(
            qDB_SendStatement(dbconn, chrSQL),
            error=function(e) {
                cat(sprintf("\t%s\n", e))
                return (-1)
            })
    if (rc!=0) {
        dbRollback(dbconn)
        return (-1)
    } # if

    dbCommit(dbconn)
    return (0)
}

qDBSQLite_YahooLastPrice_Update <- function(dbconn, dfData)
{
    tmp_chrTable <- "tmpYahooLastPrice"
    tmp_dfData <- dfData
    for (colName in c("OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "ADJUSTED")) {
        k1 <- which(is.nan(tmp_dfData[[colName]]))
        k2 <- which(is.infinite(tmp_dfData[[colName]]))
        k <- unique(k1, k2)
        j <- which(names(tmp_dfData)==colName)
        if ((length(k) > 0)&&(length(j) > 0)) {
            tmp_dfData[k,j] <- NA
        } # if
    } # for (colName

    # Search for the Latest Price Date for each security having non-zero volume
    tmp_dfData <- tmp_dfData %>%
                    filter(!is.na(VOLUME) & (VOLUME > 0)) %>%
                    group_by(SecID) %>%
                    summarize(PriceDate=max(PriceDate, na.rm=TRUE)) %>%
                    inner_join(dfData, by=c("SecID", "PriceDate"))

    dbBegin(dbconn)

    cat(sprintf("\tCreate Temporary Table %s\n", tmp_chrTable))
    dbWriteTable(dbconn, tmp_chrTable, tmp_dfData, overwrite=TRUE, temporary=TRUE)

    cat(sprintf("\tDelete from %s\n", tmp_chrTable))
    # Remove existing LastPrice records if there exist more recent records in temporary table
#    chrSQL <- lstSQL$YahooLastPrice_Delete_Temp
    chrSQL <- "DELETE FROM YahooLastPrice
                WHERE EXISTS(SELECT 1 FROM tmpYahooLastPrice a2
                    WHERE a2.SecID = YahooLastPrice.SecID
                    AND a2.PriceDate >= YahooLastPrice.PriceDate)"
    rc <- tryCatch(
            qDB_SendStatement(dbconn, chrSQL),
            error=function(e) {
                cat(sprintf("\t%s\n", e))
                return (-1)
            })
    if (rc!=0) {
        dbRollback(dbconn)
        return (-1)
    } # if

    cat(sprintf("\tInsert from %s\n", tmp_chrTable))
#    chrSQL <- lstSQL$YahooLastPrice_Insert_Temp
    chrSQL <- "INSERT INTO YahooLastPrice (SecID, PriceDate, Open, High, Low, Close, Volume, AdjClose)
                SELECT SecID, PriceDate, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJUSTED
                FROM tmpYahooLastPrice a1
                WHERE NOT EXISTS(SELECT 1 FROM YahooLastPrice a2
                    WHERE a2.SecID = a1.SecID AND a2.PriceDate >= a1.PriceDate)"
    rc <- tryCatch(
            qDB_SendStatement(dbconn, chrSQL),
            error=function(e) {
                cat(sprintf("\t%s\n", e))
                return (-1)
            })
    if (rc!=0) {
        dbRollback(dbconn)
        return (-1)
    } # if

    dbCommit(dbconn)
    return (0)
}

qDBSQLite_StockPrices_Update <- function(dbconn)
{
    tmp_chrVars <- c("Open", "High", "Low", "Close", "Volume", "AdjClose")

#    chrSQL <- lstSQL$tmpYahooLastPrice_Select
    chrSQL <- "SELECT SecID, PriceDate, Open, High, Low, Close, Volume, AdjClose
                FROM YahooLastPrice
                WHERE Volume > 0"
    tmp_dfLastPrice <- qDB_SendStatement(dbconn, chrSQL)

    # existing PctChg records meet the following criteria:
    # (1) have LastPrice records
    # (2) the Date on and before max(PriceDate) of the table [StockPrices]
#    chrSQL <- lstSQL$tmpYahooChg_Select
    chrSQL <- "SELECT a1.SecID, a1.PriceDate, a1.Open, a1.High, a1.Low, a1.Close, a1.Volume, a1.AdjClose
                FROM YahooChg a1
                WHERE EXISTS(SELECT 1 FROM YahooLastPrice a2
                    WHERE a2.SecID = a1.SecID
                    AND a1.PriceDate >= IFNULL(
                        (SELECT MAX(a3.PriceDate) FROM StockPrices a3
                        WHERE a3.SecID = a2.SecID AND a3.Volume > 0
                        AND (SELECT MAX(a4.PriceDate) FROM StockPrices a4
                        WHERE a4.SecID = a2.SecID AND a4.Volume > 0) > a3.PriceDate), '1900-01-01'))"
    tmp_dfYahooChg <- qDB_SendStatement(dbconn, chrSQL)

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

    #> Note: Using an external vector in selections is ambiguous.
    #> i Use `all_of(vars)` instead of `vars` to silence this message.
    #> i See <https://tidyselect.r-lib.org/reference/faq-external-vector.html>.
    # In reverse-chronological order, P(t-1) = P(t)/(1 + CumChg)
    tmp_df <- tmp_dfLastPrice %>%
                inner_join(tmp_dfYahooChg, by="SecID", suffix=c(".v", "")) %>%
                filter(PriceDate.v >= PriceDate) %>%
                group_by(SecID) %>%
                arrange(desc(PriceDate), .by_group=TRUE) %>%
                mutate(across(c(Open, High, Low, Close, AdjClose), ~ LagCumChg(.))) %>%
                mutate(across(Volume, ~ LagCumChg_NA(.))) %>%
                mutate(
                    Open=(Open.v * Open),
                    High=(High.v * High),
                    Low=(Low.v * Low),
                    Close=(Close.v * Close),
                    Volume=(Volume.v * Volume),
                    AdjClose=(AdjClose.v * AdjClose)) %>%
                select(c("SecID", "PriceDate", all_of(tmp_chrVars)))

    # replace NA with 0 for Volume
    tmp_df <- tmp_df %>%
                mutate(Volume=coalesce(Volume, 0))

    tmp_dfStockPrices <- tmp_df

    cat(sprintf("\tOverwrite the whole price history due to Close / AdjClose changes.\n"))

    tmp_dfStockPrices.1 <- NULL

    #> Note: Using an external vector in selections is ambiguous.
    #> i Use `all_of(vars)` instead of `vars` to silence this message.
    #> i See <https://tidyselect.r-lib.org/reference/faq-external-vector.html>.
    # In reverse-chronological order
    tmp_dfYahooChg <- qDBSQLite_tmpYahooChg_Select(dbconn, tmp_dfStockPrices)

    if (!is.null(tmp_dfYahooChg)) {
	    tmp_df <- tmp_dfLastPrice %>%
	                inner_join(tmp_dfYahooChg, by="SecID", suffix=c(".v", "")) %>%
	                filter(PriceDate.v >= PriceDate) %>%
	                group_by(SecID) %>%
	                arrange(desc(PriceDate), .by_group=TRUE) %>%
                    mutate(across(c(Open, High, Low, Close, AdjClose), ~ LagCumChg(.))) %>%
                    mutate(across(Volume, ~ LagCumChg_NA(.))) %>%
	                mutate(
	                    Open=(Open.v * Open),
	                    High=(High.v * High),
	                    Low=(Low.v * Low),
	                    Close=(Close.v * Close),
	                    Volume=(Volume.v * Volume),
	                    AdjClose=(AdjClose.v * AdjClose)) %>%
	                select(c("SecID", "PriceDate", all_of(tmp_chrVars)))

        # replace NA with 0 for Volume
        tmp_df <- tmp_df %>%
                    mutate(Volume=coalesce(Volume, 0))

        tmp_dfStockPrices.1 <- tmp_df
    } # if

    if (!is.null(tmp_dfStockPrices.1)) {
        tmp_dfStockPrices <- tmp_dfStockPrices %>%
                                filter(!(SecID %in% tmp_dfStockPrices.1$SecID))
    } # if

    tmp_dfStockPrices <- rbind(tmp_dfStockPrices, tmp_dfStockPrices.1)

    dbBegin(dbconn)

    tmp_chrTable <- "tmpStockPrices"
    cat(sprintf("\tCreate Temporary Table %s\n", tmp_chrTable))
    dbWriteTable(dbconn, tmp_chrTable, tmp_dfStockPrices, overwrite=TRUE, temporary=TRUE)

    cat(sprintf("\tCreate Index %s\n", tmp_chrTable))
#    chrSQL <- lstSQL$tmpStockPrices_Create_Index
    chrSQL <- "CREATE INDEX idx_tmpStockPrices ON tmpStockPrices (SecID, PriceDate, Close, AdjClose, Volume)"
    rc <- tryCatch(
            qDB_SendStatement(dbconn, chrSQL),
            error=function(e) {
                cat(sprintf("\t%s\n", e))
                return (-1)
            })
    if (rc!=0) {
        dbRollback(dbconn)
        return (-1)
    } # if

    cat(sprintf("\tDelete StockPrices from %s\n", tmp_chrTable))
#    chrSQL <- lstSQL$StockPrices_Delete_Temp
    chrSQL <- "DELETE FROM StockPrices
                WHERE EXISTS(SELECT 1 FROM tmpStockPrices t1
                    WHERE t1.SecID = StockPrices.SecID AND StockPrices.PriceDate >= t1.PriceDate)"
    rc <- tryCatch(
            qDB_SendStatement(dbconn, chrSQL),
            error=function(e) {
                cat(sprintf("\t%s\n", e))
                return (-1)
            })
    if (rc!=0) {
        dbRollback(dbconn)
        return (-1)
    } # if

    cat(sprintf("\tInsert from %s\n", tmp_chrTable))
#    chrSQL <- lstSQL$StockPrices_Insert_Temp
    chrSQL <- "INSERT INTO StockPrices (SecID, PriceDate, Open, High, Low, Close, Volume, AdjClose)
                SELECT SecID, PriceDate, Open, High, Low, Close, IFNULL(Volume,0), AdjClose
                FROM tmpStockPrices a1
                WHERE NOT EXISTS(SELECT 1 FROM StockPrices a2
                    WHERE a2.SecID = a1.SecID AND a2.PriceDate = a1.PriceDate)"
    rc <- tryCatch(
            qDB_SendStatement(dbconn, chrSQL),
            error=function(e) {
                cat(sprintf("\t%s\n", e))
                return (-1)
            })
    if (rc!=0) {
        dbRollback(dbconn)
        return (-1)
    } # if

    dbCommit(dbconn)
    return (0)
}

qDBSQLite_Quandl_Update <- function(dbconn, dfData)
{
    tmp_chrTable <- "tmpQuandl"

    cat(sprintf("\tCreate Table %s\n", tmp_chrTable))
    dbWriteTable(dbconn, tmp_chrTable, dfData, overwrite=TRUE, temporary=TRUE)

    cat(sprintf("\tCreate Index %s\n", tmp_chrTable))
    rc <- tryCatch(
            qDB_SendStatement(dbconn, lstSQL$tmpQuandl_Create_Index),
            error=function(e) {
                cat(sprintf("\t%s\n", e))
                return (-1)
            })
    if (rc!=0) {
        return (-1)
    } # if

    dbBegin(dbconn)

    cat(sprintf("\tDelete from %s\n", tmp_chrTable))
    rc <- tryCatch(
            qDB_SendStatement(dbconn, lstSQL$Quandl_Delete_Temp),
            error=function(e) {
                cat(sprintf("\t%s\n", e))
                return (-1)
            })
    if (rc!=0) {
        dbRollback(dbconn)
        return (-1)
    } # if

    cat(sprintf("\tInsert from %s\n", tmp_chrTable))
    rc <- tryCatch(
            qDB_SendStatement(dbconn, lstSQL$Quandl_Insert_Temp),
            error=function(e) {
                cat(sprintf("\t%s\n", e))
                return (-1)
            })
    if (rc!=0) {
        dbRollback(dbconn)
        return (-1)
    } # if

    dbCommit(dbconn)
    return (0)
}

qDBSQLite_StockPrices_Turnover_Update <- function(dbconn)
{
    tmp_chrTable <- "StockPrices"

    dbBegin(dbconn)

    cat(sprintf("\tUpdate Turnover in %s\n", tmp_chrTable))
#    chrSQL <- lstSQL$StockPrices_Turnover_Update
    chrSQL <- "UPDATE StockPrices
                SET Turnover = (SELECT q1.Turnover FROM Quandl q1
                    WHERE q1.SecID = StockPrices.SecID AND q1.PriceDate = StockPrices.PriceDate)
                WHERE Turnover IS NULL"
    rc <- tryCatch(
            qDB_SendStatement(dbconn, chrSQL),
            error=function(e) {
                cat(sprintf("\t%s\n", e))
                return (-1)
            })
    if (rc!=0) {
        dbRollback(dbconn)
        return (-1)
    } # if

    dbCommit(dbconn)
    return (0)
}
