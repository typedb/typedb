@echo off
REM
REM GRAKN.AI - THE KNOWLEDGE GRAPH
REM Copyright (C) 2019 Grakn Labs Ltd
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

REM needs to be called such that software installed
REM by Chocolatey in prepare.bat is accessible
CALL refreshenv

REM build grakn-core-all-windows archive
bazel build //:assemble-windows-zip || GOTO :error

REM unpack and start Grakn server
unzip bazel-genfiles\grakn-core-all-windows.zip -d bazel-genfiles\dist\ || GOTO :error
PUSHD bazel-genfiles\dist\grakn-core-all-windows\
CALL grakn.bat server start || GOTO :error
POPD

REM run application test
bazel test //test/common:grakn-application-test --test_output=streamed --spawn_strategy=standalone --cache_test_results=no || GOTO :error

REM stop Grakn server
PUSHD bazel-genfiles\dist\grakn-core-all-windows\
CALL grakn.bat server stop
POPD

:error
IF %errorlevel% NEQ 0 EXIT /b %errorlevel%
