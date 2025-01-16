@echo off
REM This Source Code Form is subject to the terms of the Mozilla Public
REM License, v. 2.0. If a copy of the MPL was not distributed with this
REM file, You can obtain one at https://mozilla.org/MPL/2.0/.


SET "TYPEDB_HOME=%~dp0"
IF %TYPEDB_HOME:~-1%==\ SET TYPEDB_HOME=%TYPEDB_HOME:~0,-1%

if "%1" == "" goto missingargument

if "%1" == "console" goto startconsole
if "%1" == "server"  goto startserver

echo   Invalid argument: %1. Possible commands are:
goto print_usage

:missingargument

 echo   Missing argument. Possible commands are:
goto print_usage

:startconsole

where java >NUL 2>NUL
if %ERRORLEVEL% GEQ 1 (
  echo Java is not installed on this machine. TypeDB needs Java 11+ in order to run.
  pause
  exit 1
)

for /f "tokens=1,* delims= " %%a in ("%*") do set ARGS=%%b

set "G_CP=%TYPEDB_HOME%\console\conf\;%TYPEDB_HOME%\console\lib\*"
java %CONSOLE_JAVAOPTS% -cp "%G_CP%" -Dtypedb.dir="%TYPEDB_HOME%" com.typedb.console.TypeDBConsole %ARGS%
goto exit

:startserver
%TYPEDB_HOME%\server\typedb_server_bin.exe
goto exit

:exit
exit /b 0

:print_usage
echo   Server:          typedb server [--help]
echo   Console:         typedb console [--help]
goto exiterror

:exiterror
exit /b 1
