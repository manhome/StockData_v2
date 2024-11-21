@echo off
SET arg1=%1
SET arg2=%2
SET arg3=%3
FOR /f %%i in ('powershell -command "& { (Get-Date).ToString("""yyyyMMdd""")}"') DO (SET TODAY=%%i)
SET errorlog=errorlog_qUpdateStocks_%arg1%_%TODAY%.txt
R.exe CMD BATCH --no-save --no-restore "--args %arg1% %arg2% %arg3%" qUpdateStocks.R %errorlog%
IF %ERRORLEVEL% NEQ 0 GOTO :ERROR

GOTO :END

:ERROR
ECHO "There was an error."
REM EXIT 1

:END
ECHO "End."
REM EXIT 0
