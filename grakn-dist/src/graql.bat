@echo off
REM 
REM GRAKN.AI - THE KNOWLEDGE GRAPH
REM Copyright (C) 2018 Grakn Labs Ltd
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

SET "GRAKN_HOME=%cd%"


SET GRAKN_CONFIG="conf\grakn.properties"
set "G_CP=%GRAKN_HOME%\conf\;%GRAKN_HOME%\services\grakn\client;%GRAKN_HOME%\services\lib\*"

java -cp "%G_CP%" -Dgrakn.dir="%GRAKN_HOME%" -Dgrakn.conf="%GRAKN_HOME%/%GRAKN_CONFIG%" -Dengine.javaopts="%ENGINE_JAVAOPTS%" ai.grakn.engine.bootup.Graql %*
