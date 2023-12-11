@echo off
REM Copyright (C) 2022 Vaticle
REM
REM This program is free software: you can redistribute it and/or modify
REM it under the terms of the GNU Affero General Public License as
REM published by the Free Software Foundation, either version 3 of the
REM License, or (at your option) any later version.
REM
REM This program is distributed in the hope that it will be useful,
REM but WITHOUT ANY WARRANTY; without even the implied warranty of
REM MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
REM GNU Affero General Public License for more details.
REM
REM You should have received a copy of the GNU Affero General Public License
REM along with this program.  If not, see <https://www.gnu.org/licenses/>.
REM

SET "TYPEDB_HOME=%~dp0"
IF %TYPEDB_HOME:~-1%==\ SET TYPEDB_HOME=%TYPEDB_HOME:~0,-1%

where java >NUL 2>NUL
if %ERRORLEVEL% GEQ 1 (
  echo Java is not installed on this machine. TypeDB needs Java 11+ in order to run.
  pause
  exit 1
)

if "%1" == "" goto missingargument

if "%1" == "server"  goto startserver

echo   Invalid argument: %1. Possible commands are:
goto print_usage

:missingargument

echo   Missing argument. Possible commands are:
goto print_usage

:startserver

set "G_CP=%TYPEDB_HOME%\server\conf\;%TYPEDB_HOME%\server\lib\*"
echo "%G_CP%"
echo "%TYPEDB_HOME%"

if "%2"=="--help" (
    java %SERVER_JAVAOPTS% -cp "%G_CP%" -Dtypedb.dir="%TYPEDB_HOME%" com.vaticle.typedb.core.server.TypeDBServer %2 %3 %4 %5 %6 %7 %8 %9
) else (
    start cmd /c java %SERVER_JAVAOPTS% -cp "%G_CP%" -Dtypedb.dir="%TYPEDB_HOME%" com.vaticle.typedb.core.server.TypeDBServer %2 %3 %4 %5 %6 %7 %8 %9 ^|^| pause
)
goto exit

:exit
exit /b 0

:print_usage
echo   Server:          typedb server [--help]
goto exiterror

:exiterror
exit /b 1
