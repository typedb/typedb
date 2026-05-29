@echo off
REM This Source Code Form is subject to the terms of the Mozilla Public
REM License, v. 2.0. If a copy of the MPL was not distributed with this
REM file, You can obtain one at https://mozilla.org/MPL/2.0/.

REM Bazel build does not work on Windows because rules_rust 0.56's
REM crate_universe leaves dangling symlinks for ring's third_party/ MSVC headers,
REM and ring's curve25519.c #includes one of them. Use cargo instead.

CALL refreshenv

cargo test -p server --test test_admin_service
if %errorlevel% neq 0 exit /b %errorlevel%

cargo test -p typedb-admin --lib
if %errorlevel% neq 0 exit /b %errorlevel%

REM Build the actual server + admin binaries, then verify the admin binary can
REM connect to the server binary through the Named Pipe transport. The in-process
REM test_admin_service above covers the library; this covers the binaries' own
REM surface (clap parsing, tokio runtime, exit-code propagation).
REM On Linux/macOS the same coverage is provided by //tests/assembly:test_admin_assembly.
cargo build -p typedb_server_bin -p typedb_admin_bin
if %errorlevel% neq 0 exit /b %errorlevel%

cargo test -p typedb_admin_bin --test binary_smoke
if %errorlevel% neq 0 exit /b %errorlevel%
