library(openxlsx)
library(XML)
library(xts)
library(lubridate)
library(stringr)
library(dplyr)
library(DBI)
library(RSQLite)

chrOS <- Sys.getenv("OS");
if (length(grep(pattern="windows", chrOS, ignore.case=TRUE)) > 0) {
   chrSrcDrive <- "F:"
} else {
   chrSrcDrive <- "/media/hdd"
}

source(file.path(chrSrcDrive, "App/Lib/R/qUtility.R"), local=TRUE, echo=FALSE)
source(file.path(chrSrcDrive, "App/Lib/R/qDB.R"), local=TRUE, echo=FALSE)
source(file.path(chrSrcDrive, "App/R/StockData_v2/qDBSQLite_StockPrice.R"), local=TRUE, echo=FALSE)

qMain <- function(chrFileParam)
{
    dtTimeBegin_F <- Sys.time();
    cat(sprintf("Get parameter file '%s' began at %s.\n", basename(chrFileParam), dtTimeBegin_F));

    # Read parameter file
    qReadXMLParamFile(chrFileParam);

    chrFileStock <<- dfParam.file_stock[[1]];
#    chrFileSQL <<- dfParam.file_sql[[1]];

    # Read stored SQL
#    qReadXMLParamFile(chrFileSQL);

#    lstSQL <<- sapply(dfParam.sql,
#                FUN=function(x) {
#                    x <- gsub(pattern="[[:blank:]]|[^[:print:]]", " ", x)
#                    x <- gsub(pattern="[[:blank:]]{2,}", " ", x)
#                    x <- str_trim(x)
#                    return (x)
#                }, simplify=FALSE);

    tmp_chrSheets <- c("stock", "etf");
#    tmp_obj <- readWorksheetFromFile(chrFileStock, colTypes="character", sheet=tmp_chrSheets);
    # openxlsx::readWorkbook only accepts 1 sheet each time
    # Set simplify=FALSE to return a list of data.frame
    tmp_obj <- sapply(tmp_chrSheets, function(x) readWorkbook(chrFileStock, sheet=x),
                simplify=FALSE, USE.NAMES=FALSE)
    tmp_obj <- do.call("rbind", tmp_obj);

    dbconn <<- dbConnect(SQLite(), "");

    on.exit(
        tryCatch(
            dbDisconnect(dbconn),
            error=function(e) {
                cat(sprintf("\t%s\n", e));
            })
    );

#    chrSQL <- lstSQL$attach_db_stocks
    chrSQL <- "ATTACH DATABASE 'F:\\App\\db\\test\\hk_stocks.db' AS stocks"
    rc <- tryCatch(
            qDB_SendStatement(dbconn, chrSQL),
            error=function(e) {
                cat(sprintf("\t%s\n", e));
            })

    tryCatch(
        qDBSQLite.Stocks_Update(dbconn, dfData=tmp_obj),
        error=function(e) {
            cat(sprintf("\t%s\n", e));
        })

    tryCatch(
        dbDisconnect(dbconn),
        error=function(e) {
            cat(sprintf("\t%s\n", e));
        })

    dtTimeEnd_F <- Sys.time();
    cat(sprintf("Get parameter file '%s' finished at %s.\n", basename(chrFileParam), dtTimeBegin_F));
    print(difftime(dtTimeEnd_F, dtTimeBegin_F));

    return (0);
}

#######
# Main
#######
# trailingOnly=TRUE means that only arguments after --args are returned
# if trailingOnly=FALSE then you got:
# [1] "--no-restore" "--no-save" "--args" "2010-01-28" "example" "100"
#Debug <- function() {
args <- commandArgs(trailingOnly=TRUE);
cat(sprintf("Input Parameter = %s\n", args));

if (length(args) == 0) {
    chrCmd <- "Invalid command. e.g. <prompt>command.bat param.xml"
    print(chrCmd);
    quit(save="no", status=-1);
} # if (length(args

chrFileParam <- args[1];

rc <- qMain(chrFileParam);
if (rc != 0) {
    quit(save="no", status=rc);
} # if (rc
#} # Debug

########
# Debug
########
testDebug <- function()
{
    setwd("F:/App/R/StockData_v2")
    chrFileParam <- "_sys_param_HK_UpdateStocks.xml"
    rc <- qMain(chrFileParam)
}
