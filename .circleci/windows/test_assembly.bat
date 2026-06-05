@echo off
REM This Source Code Form is subject to the terms of the Mozilla Public
REM License, v. 2.0. If a copy of the MPL was not distributed with this
REM file, You can obtain one at https://mozilla.org/MPL/2.0/.

REM Cargo, not bazel: rules_rust 0.56's crate_universe leaves ring's MSVC headers
REM dangling on Windows, breaking ring's curve25519.c. Cargo builds ring natively.
REM Linux/macOS still run these tests under bazel.

CALL refreshenv

cargo build --profile=release
if %errorlevel% neq 0 exit /b %errorlevel%
cargo build --profile=release -p typedb_admin_bin
if %errorlevel% neq 0 exit /b %errorlevel%
copy /Y target\release\typedb_server_bin.exe .\ >nul
copy /Y target\release\typedb_admin_bin.exe .\ >nul

bazel --output_base=C:\b --windows_enable_symlinks build --config=ci --enable_runfiles //:assemble-all-windows-x86_64-zip
if %errorlevel% neq 0 exit /b %errorlevel%

copy /Y bazel-bin\typedb-all-windows-x86_64.zip .\ >nul
if exist typedb-extracted rmdir /s /q typedb-extracted
tar -xf typedb-all-windows-x86_64.zip
if %errorlevel% neq 0 exit /b %errorlevel%
move typedb-all-windows-x86_64-0.0.0 typedb-extracted >nul
if %errorlevel% neq 0 exit /b %errorlevel%

start "typedb-server" /B typedb-extracted\server\typedb_server_bin.exe --development-mode.enabled=true --server.http.enabled=false
timeout /t 15 /nobreak >nul

typedb-extracted\console\typedb_console_bin.exe --username=admin --password=password --address=localhost:1729 --tls-disabled --script=tests\assembly\script.tql
set CONSOLE_EC=%errorlevel%

taskkill /F /IM typedb_server_bin.exe >nul 2>&1

if %CONSOLE_EC% neq 0 (
    echo Console script failed with exit code %CONSOLE_EC%
    exit /b %CONSOLE_EC%
)
exit /b 0
