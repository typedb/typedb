@echo off
REM This Source Code Form is subject to the terms of the Mozilla Public
REM License, v. 2.0. If a copy of the MPL was not distributed with this
REM file, You can obtain one at https://mozilla.org/MPL/2.0/.


SET "TYPEDB_HOME=%~dp0"
IF %TYPEDB_HOME:~-1%==\ SET TYPEDB_HOME=%TYPEDB_HOME:~0,-1%

if "%1" == "" goto missingargument

if "%1" == "console" goto startconsole
if "%1" == "server"  goto startserver
if "%1" == "psql"    goto startpsql

echo   Invalid argument: %1. Possible commands are:
goto print_usage

:missingargument

 echo   Missing argument. Possible commands are:
goto print_usage

:startconsole

for /f "tokens=1,* delims= " %%a in ("%*") do set ARGS=%%b
"%TYPEDB_HOME%\console\typedb_console_bin.exe" %ARGS%
goto exit

:startserver
for /f "tokens=1,* delims= " %%a in ("%*") do set ARGS=%%b
"%TYPEDB_HOME%\server\typedb_server_bin.exe" %ARGS%
goto exit

:startpsql
REM Launch psql connected to the TypeDB pgwire endpoint.
SET PSQL_HOST=localhost
SET PSQL_PORT=5432
SET PSQL_USER=admin
SET PSQL_DB=typedb
shift
:psqlargs
if "%1"=="" goto runpsql
if "%1"=="--host"     ( set PSQL_HOST=%2& shift& shift& goto psqlargs )
if "%1"=="--port"     ( set PSQL_PORT=%2& shift& shift& goto psqlargs )
if "%1"=="--user"     ( set PSQL_USER=%2& shift& shift& goto psqlargs )
if "%1"=="--database" ( set PSQL_DB=%2& shift& shift& goto psqlargs )
if "%1"=="--help" (
  echo Usage: typedb psql [--host HOST] [--port PORT] [--user USER] [--database DB]
  echo.
  echo Launches psql connected to the TypeDB pgwire endpoint.
  echo.
  echo Defaults:
  echo   --host      localhost
  echo   --port      5432
  echo   --user      admin
  echo   --database  typedb
  echo.
  echo Password handling:
  echo   psql reads PGPASSWORD or prompts interactively.
  goto exit
)
echo Unknown option: %1
goto exiterror
:runpsql
psql -h %PSQL_HOST% -p %PSQL_PORT% -U %PSQL_USER% -d %PSQL_DB%
goto exit

:exit
exit /b 0

:print_usage
echo   Server:          typedb server [--help]
echo   Console:         typedb console [--help]
echo   Postgres shell:  typedb psql [--host HOST] [--port PORT] [--user USER] [--database DB]
goto exiterror

:exiterror
exit /b 1
