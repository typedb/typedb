@echo off
REM This Source Code Form is subject to the terms of the Mozilla Public
REM License, v. 2.0. If a copy of the MPL was not distributed with this
REM file, You can obtain one at https://mozilla.org/MPL/2.0/.

REM Cargo, not bazel: rules_rust 0.56's crate_universe leaves ring's MSVC headers
REM dangling on Windows, breaking ring's curve25519.c. Cargo builds ring natively.
REM Linux/macOS still run these tests under bazel.

CALL refreshenv

cargo test -p server --test test_admin_service
if %errorlevel% neq 0 exit /b %errorlevel%

cargo test -p typedb-admin --lib
if %errorlevel% neq 0 exit /b %errorlevel%

REM Windows arm of //tests/assembly:test_admin_assembly.
cargo build -p typedb_server_bin -p typedb_admin_bin
if %errorlevel% neq 0 exit /b %errorlevel%

cargo test -p typedb_admin_bin --test admin_assembly
if %errorlevel% neq 0 exit /b %errorlevel%
