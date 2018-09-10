@REM
@REM GRAKN.AI - THE KNOWLEDGE GRAPH
@REM Copyright (C) 2018 Grakn Labs Ltd
@REM
@REM This program is free software: you can redistribute it and/or modify
@REM it under the terms of the GNU Affero General Public License as
@REM published by the Free Software Foundation, either version 3 of the
@REM License, or (at your option) any later version.
@REM
@REM This program is distributed in the hope that it will be useful,
@REM but WITHOUT ANY WARRANTY; without even the implied warranty of
@REM MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
@REM GNU Affero General Public License for more details.
@REM
@REM You should have received a copy of the GNU Affero General Public License
@REM along with this program.  If not, see <https://www.gnu.org/licenses/>.
@REM

@echo off
if "%OS%" == "Windows_NT" setlocal

pushd %~dp0..
set CASSANDRA_HOME=%CD%
popd

if NOT DEFINED CASSANDRA_MAIN set CASSANDRA_MAIN=org.apache.cassandra.service.CassandraDaemon
if NOT DEFINED JAVA_HOME goto :err

REM -----------------------------------------------------------------------------
REM JVM Opts
set JAVA_OPTS=-ea^
 -javaagent:"%CASSANDRA_HOME%\lib\jamm-0.3.0.jar"^
 -Xms2G^
 -Xmx2G^
 -XX:+HeapDumpOnOutOfMemoryError^
 -XX:+UseParNewGC^
 -XX:+UseConcMarkSweepGC^
 -XX:+CMSParallelRemarkEnabled^
 -XX:SurvivorRatio=8^
 -XX:MaxTenuringThreshold=1^
 -XX:CMSInitiatingOccupancyFraction=75^
 -XX:+UseCMSInitiatingOccupancyOnly^
 -Dlogback.configurationFile=logback.xml^
 -Djava.library.path="%CASSANDRA_HOME%\lib\sigar-bin"^
 -Dcassandra.jmx.local.port=7199
REM **** JMX REMOTE ACCESS SETTINGS SEE: https://wiki.apache.org/cassandra/JmxSecurity ***
REM -Dcom.sun.management.jmxremote.port=7199^
REM -Dcom.sun.management.jmxremote.ssl=false^
REM -Dcom.sun.management.jmxremote.authenticate=true^
REM -Dcom.sun.management.jmxremote.password.file=C:\jmxremote.password

set "CLASSPATH=%CASSANDRA_HOME%\lib\*"

:parseArgumentsLoop
IF NOT "%1"=="" (
    IF "%1"=="-l" (
        SET logDirectory=%2
        SHIFT
    )
    IF "%1"=="-p" (
        SET pidFile=%2
        SHIFT
    )
    SHIFT
    GOTO :parseArgumentsLoop
)

REM Include the build\classes\main directory so it works in development
set CASSANDRA_CLASSPATH="%CLASSPATH%";"%CASSANDRA_HOME%\build\classes\main";"%CASSANDRA_HOME%\build\classes\thrift"
set CASSANDRA_PARAMS=-Dcassandra -Dcassandra-foreground=yes
set CASSANDRA_PARAMS=%CASSANDRA_PARAMS% -Dcassandra.logdir=%logDirectory%
set CASSANDRA_PARAMS=%CASSANDRA_PARAMS% -Dcassandra-pidfile=%pidFile%
set CASSANDRA_PARAMS=%CASSANDRA_PARAMS% -Dcassandra.config="file:\\\%CASSANDRA_HOME%\cassandra\cassandra.yaml"
set CASSANDRA_PARAMS=%CASSANDRA_PARAMS% -Dlogback.configurationFile="file:\\\%CASSANDRA_HOME%\cassandra\logback.xml"

echo Starting Cassandra Server
start "cassandra" /b "%JAVA_HOME%\bin\java.exe" %JAVA_OPTS% %CASSANDRA_PARAMS% -cp %CASSANDRA_CLASSPATH% "%CASSANDRA_MAIN%"
exit