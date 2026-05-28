@echo off
REM This Source Code Form is subject to the terms of the Mozilla Public
REM License, v. 2.0. If a copy of the MPL was not distributed with this
REM file, You can obtain one at https://mozilla.org/MPL/2.0/.

REM Refresh PATH so chocolatey-installed software (bazel, MSVC build tools, ...) is visible.
CALL refreshenv

REM Run the cross-OS admin endpoint tests. These exercise both the happy-path connect
REM (correct endpoint = Named Pipe with the right name) and the rejection path
REM (wrong pipe name must not connect). The test crate is platform-aware: on Windows
REM it uses tokio::net::windows::named_pipe; on Unix it uses tokio::net::UnixListener.
bazel --windows_enable_symlinks test --config=ci --test_output=errors ^
    //admin/client:test_crate_client ^
    //server:test_admin_service
if %errorlevel% neq 0 exit /b %errorlevel%
