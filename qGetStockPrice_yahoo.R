library(XML)
library(xts)
library(lubridate)
library(stringr)
library(dplyr)
library(DBI)
library(RSQLite)
library(quantmod)
library(openxlsx)

chrOS <- Sys.getenv("OS")
if (length(grep(pattern="windows", chrOS, ignore.case=TRUE)) > 0) {
   chrSrcDrive <- "F:"
} else {
   chrSrcDrive <- "/media/hdd"
}

source(file.path(chrSrcDrive, "App/Lib/R/qUtility.R"), local=TRUE, echo=FALSE)
source(file.path(chrSrcDrive, "App/Lib/R/qDB.R"), local=TRUE, echo=FALSE)
source(file.path(chrSrcDrive, "App/R/StockData_v2/qDBSQLite_StockPrice.R"), local=TRUE, echo=FALSE)

Init <- function()
{
    tmp_objNames <- c("dfYahoo")
    for (i in 1:length(tmp_objNames)) {
        tmp_objName <- tmp_objNames[i]
        if (!exists(tmp_objName, where=pos.rdata_save, inherits=FALSE)) {
            assign(tmp_objName, NULL, pos=pos.rdata_save)
        } # if
    } # for (i
}

qMain <- function(chrFileParam, dtStart, dtEnd)
{
    dtTimeBegin_F <- Sys.time()
    cat(sprintf("Get parameter file '%s' began at %s.\n", basename(chrFileParam), dtTimeBegin_F))

    cat(sprintf("Read parameter file '%s'.\n", chrFileParam))

    if (!file.exists(chrFileParam)) {
        cat(sprintf("Parameter file '%s' does not exist.\n", chrFileParam))
        return (-1)
    } # if

    # Read parameter file
    qReadXMLParamFile(chrFileParam)

    chrrdata_save <<- dfParam.rdata_save[[1]]
    chrFilePattern <<- dfParam.filepattern[[1]]
    chrDB_stocks <<- dfParam.db_stocks[[1]]
    chrDB_stockprices <<- dfParam.db_stockprices[[1]]
    chrDB_yahoo <<- dfParam.db_yahoo[[1]]
#    chrFileSQL <<- dfParam.file_sql[[1]]

#    # Read stored SQL
#    qReadXMLParamFile(chrFileSQL)

#    lstSQL <<- sapply(dfParam.sql,
#                FUN=function(x) {
#                    x <- gsub(pattern="[[:blank:]]|[^[:print:]]", " ", x)
#                    x <- gsub(pattern="[[:blank:]]{2,}", " ", x)
#                    x <- str_trim(x)
#                    return (x)
#                }, simplify=FALSE)

    if (!file.exists(chrrdata_save)) {
      save(file=chrrdata_save)
    } # if
    pos.rdata_save <<- qAttach.rdata(chrrdata_save)

    Init()

    dbconn <<- dbConnect(SQLite(), "")

    on.exit(
        tryCatch(
            dbDisconnect(dbconn),
            error=function(e) {
                cat(sprintf("\t%s\n", e))
        })
    )

#    chrSQL <- lstSQL$attach_db_stocks
#    chrSQL <- "ATTACH DATABASE 'F:\\App\\db\\test\\hk_stocks.db' AS stocks"
    chrSQL <- "ATTACH DATABASE '%s' AS stocks"
    chrSQL <- sprintf(chrSQL, chrDB_stocks)
    rc <- tryCatch(
            qDB_SendStatement(dbconn, chrSQL),
            error=function(e) {
                cat(sprintf("\t%s\n", e))
            })

#    chrSQL <- lstSQL$attach_db_stockprices
#    chrSQL <- "ATTACH DATABASE 'F:\\App\\db\\test\\hk_stockprices.db' AS stockprices"
    chrSQL <- "ATTACH DATABASE '%s' AS stockprices"
    chrSQL <- sprintf(chrSQL, chrDB_stockprices)
    rc <- tryCatch(
            qDB_SendStatement(dbconn, chrSQL),
            error=function(e) {
                cat(sprintf("\t%s\n", e))
            })

#    chrSQL <- lstSQL$attach_db_yahoo
#    chrSQL <- "ATTACH DATABASE 'F:\\App\\db\\test\\hk_yahoo.db' AS yahoo"
    chrSQL <- "ATTACH DATABASE '%s' AS yahoo"
    chrSQL <- sprintf(chrSQL, chrDB_yahoo)
    rc <- tryCatch(
            qDB_SendStatement(dbconn, chrSQL),
            error=function(e) {
                cat(sprintf("\t%s\n", e))
            })

#    chSQL <- lstSQL$attach_db_quandl
#    chrSQL <- "ATTACH DATABASE 'F:\\App\\db\\test\\hk_quandl.db' AS quandl"
#    rc <- tryCatch(
#            qDB_SendStatement(dbconn, chrSQL),
#            error=function(e) {
#                cat(sprintf("\t%s\n", e))
#            })

#    chrSQL <- lstSQL$YahooChg_Select_MaxDate
    chrSQL <- "SELECT a1.SecID_yahoo, a1.SecID, a1.Exch_yahoo, 
                    IFNULL((SELECT MAX(PriceDate) FROM YahooChg a2 
                        WHERE a2.SecID = a1.SecID_yahoo 
                        AND a2.Volume != 0
                        AND a2.PriceDate < '%s'), '1900-01-01') AS PriceDate 
                FROM Stocks a1"
    chrSQL <- sprintf(chrSQL, format(dtStart, '%Y-%m-%d'))

    tmp_dfMaxDate <- tryCatch(
                        qDB_SendStatement(dbconn, chrSQL),
                        error=function(e) {
                            cat(sprintf("\t%s\n", e))
                        })

    tmp_chrKeys <- c("SecID", "PriceDate")
    tmp_dfYahoo <- NULL

#    n <- nrow(tmp_dfMaxDate)
    n <- 20
    for (i in 1:n) {
        if (i%%100 < 1) cat(sprintf("\t%.2f pct completed.\n", 100*i/n))

        tmp_chrExch_yahoo <- tmp_dfMaxDate$Exch_yahoo[i]
        tmp_chrSecID <- tmp_dfMaxDate$SecID[i]
        tmp_chrSecID_yahoo <- tmp_dfMaxDate$SecID_yahoo[i]
        tmp_chrStartDate <- tmp_dfMaxDate$PriceDate[i]
        tmp_chrEndDate <- format(dtEnd)

        cat(sprintf("\tProcess %s\n", tmp_chrSecID_yahoo))
        tmp_obj <- tryCatch(
                    getSymbols.yahoo(tmp_chrSecID_yahoo,
                        from=tmp_chrStartDate,
                        to=tmp_chrEndDate,
                        auto.assign=FALSE),
                    error=function(e) {
                        cat(sprintf("\t%s\n", e))
                        return (NULL)
                    })

#        if (class(tmp_obj)=="try-error") next
        if (!is.na(match("try-error", class(tmp_obj)))) next
        if (is.null(tmp_obj)) next
        if (nrow(tmp_obj)==0) next

        # change colnames to upper case
        names(tmp_obj) <- toupper(names(tmp_obj))

        # trim symbol from raw column names
        names(tmp_obj) <- sub(pattern=paste0(tmp_chrSecID_yahoo, "."), "", names(tmp_obj), fixed=TRUE)
        tmp_df <- as.data.frame(tmp_obj, stringsAsFactors=FALSE)
        tmp_df$SecID <- tmp_chrSecID_yahoo
        tmp_df$PriceDate <- format(index(tmp_obj))
        tmp_df <- tmp_df[union(tmp_chrKeys, setdiff(names(tmp_df), tmp_chrKeys))]
        row.names(tmp_df) <- NULL

        tmp_chrMinDate <- format(min(as.Date(tmp_df$PriceDate), na.rm=TRUE), "%Y%m%d")
        tmp_chrMaxDate <- format(max(as.Date(tmp_df$PriceDate), na.rm=TRUE), "%Y%m%d")
        tmp_chrFile <- sprintf(chrFilePattern, tmp_chrExch_yahoo,
                            tmp_chrSecID, tmp_chrMinDate, tmp_chrMaxDate)
        write.csv(tmp_df, tmp_chrFile, row.names=FALSE, na="")

        tmp_dfYahoo <- rbind(tmp_dfYahoo, tmp_df)
        row.names(tmp_dfYahoo) <- NULL
    } # for (i

    # simple check for duplicate-key records
    tmp_dfCheck <- tmp_dfYahoo %>%
                    group_by(SecID, PriceDate) %>%
                    summarize(Count=n()) %>%
                    filter(Count > 1)
    if (nrow(tmp_dfCheck) > 0) {
        cat(sprintf("\tDuplicate-Key SecID:\n"))
        with(tmp_dfCheck, cat(sprintf("\t{%s, %s}\n", SecID, PriceDate)))
        cat(sprintf("\tPlease check! Those SecID will be skipped!\n"))
        tmp_dfYahoo <- tmp_dfYahoo %>%
                        filter(!(SecID %in% tmp_dfCheck$SecID))
    } # if

    row.names(tmp_dfYahoo) <- NULL
    dfYahoo <<- tmp_dfYahoo
    qSave.pos(pos.rdata_save)

    tryCatch(
        qDBSQLite_YahooChg_Update(dbconn, dfData=tmp_dfYahoo),
        error=function(e) {
            cat(sprintf("\t%s\n", e))
        })

    tryCatch(
        qDBSQLite_YahooLastPrice_Update(dbconn, dfData=tmp_dfYahoo),
        error=function(e) {
            cat(sprintf("\t%s\n", e))
        })

    tryCatch(
        qDBSQLite_StockPrices_Update(dbconn),
        error=function(e) {
            cat(sprintf("\t%s\n", e))
        })

#    tryCatch(
#        qDBSQLite_StockPrices_Turnover_Update(dbconn),
#        error=function(e) {
#            cat(sprintf("\t%s\n", e))
#        })

    tryCatch(
        dbDisconnect(dbconn),
        error=function(e) {
            cat(sprintf("\t%s\n", e))
        })

    dtTimeEnd_F <- Sys.time()
    cat(sprintf("Get parameter file '%s' finished at %s.\n", basename(chrFileParam), dtTimeBegin_F))
    print(difftime(dtTimeEnd_F, dtTimeBegin_F))

    return (0)
}

#######
# Main
#######
# trailingOnly=TRUE means that only arguments after --args are returned
# if trailingOnly=FALSE then you got:
# [1] "--no-restore" "--no-save" "--args" "2010-01-28" "example" "100"
# Debug (begin)
Debug <- function() {
args <- commandArgs(trailingOnly=TRUE)
cat(sprintf("Input Parameter = %s\n", args))

if (length(args) == 0) {
    chrCmd <- "Invalid command. e.g. <prompt>command.bat param.xml [YYYYMMDD] [YYYYMMDD]"
    print(chrCmd)
    quit(save="no", status=-1)
} # if (length(args

dtStart <- as.Date("1900-01-01")
dtEnd <- Sys.Date() - 1

chrFileParam <- args[1]

if (length(args) > 1) {
    chrStartDate <- args[2]
    dtStart <- as.Date(chrStartDate, "%Y%m%d")
} # if (length(args

if (length(args) > 2) {
    chrEndDate <- args[3]
    dtEnd <- as.Date(chrEndDate, "%Y%m%d")
} # if (length(args

rc <- qMain(chrFileParam, dtStart, dtEnd)
if (rc != 0) {
    quit(save="no", status=rc)
} # if (rc
}
# Debug (end)

########
# Debug
########
testDebug <- function()
{
    setwd("F:/App/R/StockData_v2")
    chrFileParam <- "_sys_param_HK_GetStockPrice_yahoo.xml"
    dtStart <- as.Date("2020-01-01")
    dtEnd <- as.Date("2021-03-01")
#    dtEnd <- Sys.Date() - 1
    rc <- qMain(chrFileParam, dtStart, dtEnd)
    # to be continued ...
}

