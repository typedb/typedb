@echo off
REM This Source Code Form is subject to the terms of the Mozilla Public
REM License, v. 2.0. If a copy of the MPL was not distributed with this
REM file, You can obtain one at https://mozilla.org/MPL/2.0/.

REM install dependencies needed for build
git apply .circleci\windows\git.patch
if %errorlevel% neq 0 (
    echo "Failed to apply patch. Regenerate it with 'git diff'. Exiting...";
    exit /b %errorlevel%
)

choco install .circleci\windows\dependencies.config --yes --no-progress

REM permanently set variables for Bazel build
SETX BAZEL_SH "C:\Program Files\Git\usr\bin\bash.exe"
SETX CARGO_NET_GIT_FETCH_WITH_CLI true
