@echo off
REM This Source Code Form is subject to the terms of the Mozilla Public
REM License, v. 2.0. If a copy of the MPL was not distributed with this
REM file, You can obtain one at https://mozilla.org/MPL/2.0/.

REM build file
cargo build --profile=release
copy target\release\typedb_server_bin.exe  .\
git apply .circleci\windows\git.patch

SET DEPLOY_ARTIFACT_USERNAME=%REPO_TYPEDB_USERNAME%
SET DEPLOY_ARTIFACT_PASSWORD=%REPO_TYPEDB_PASSWORD%
set /p VER=<VERSION
bazel --windows_enable_symlinks run --define version=%VER%  --enable_runfiles //:deploy-typedb-server -- release

