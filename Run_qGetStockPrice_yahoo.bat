@echo off
REM SET R_Path="C:\Program Files\R\R-3.0.1\bin\x64"
SET arg1=%1
SET arg2=%2
SET arg3=%3
FOR /f %%i in ('powershell -command "& { (Get-Date).ToString("""yyyyMMdd""")}"') DO (SET TODAY=%%i)
SET errorlog=errorlog_qGetStockPrice_yahoo_%arg1%_%TODAY%.txt
R.exe CMD BATCH --no-save --no-restore "--args %arg1% %arg2% %arg3%" qGetStockPrice_yahoo.R %errorlog%
IF %ERRORLEVEL% NEQ 0 GOTO :ERROR

GOTO :END

:ERROR
ECHO "There was an error."
REM EXIT 1

:END
ECHO "End."
REM EXIT 0
