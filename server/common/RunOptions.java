/*
 * Copyright (C) 2021 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.server.common;

import grakn.core.common.exception.GraknException;
import grakn.core.server.Version;
import picocli.CommandLine;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public abstract class RunOptions {

    public boolean isServer() {
        return false;
    }

    public Server asServer() {
        throw GraknException.of(ILLEGAL_CAST, RunOptions.class, Server.class);
    }

    public boolean isDataImport() {
        return false;
    }

    public DataImport asDataImport() {
        throw GraknException.of(ILLEGAL_CAST, RunOptions.class, DataImport.class);
    }

    public boolean isDataExport() {
        return false;
    }

    public DataExport asDataExport() {
        throw GraknException.of(ILLEGAL_CAST, RunOptions.class, DataExport.class);
    }

    public boolean isPrintSchema() {
        return false;
    }

    @CommandLine.Command(name = "grakn server", mixinStandardHelpOptions = true, version = {Version.VERSION})
    public static class Server extends RunOptions {

        @CommandLine.Option(descriptionKey = "server.data",
                names = {"--data"},
                description = "Directory in which database server data will be stored")
        protected String data;

        @CommandLine.Option(descriptionKey = "server.logs",
                names = {"--logs"},
                description = "Directory in which database server logs will be stored")
        protected String logs;

        @CommandLine.Option(descriptionKey = "server.port",
                names = {"--port"},
                defaultValue = ServerDefaults.DEFAULT_DATABASE_PORT + "",
                description = "Port number of database server in which GRPC clients will connect to")
        private int port;

        @CommandLine.Option(descriptionKey = "grabl.trace",
                names = {"--grabl-trace"},
                negatable = true,
                defaultValue = "false",
                description = "Enable Grabl performance tracing")
        private boolean grablTrace;

        @CommandLine.Option(descriptionKey = "grabl.uri",
                names = {"--grabl-uri"},
                description = "Grabl tracing server URI")
        private URI grablURI;

        @CommandLine.Option(descriptionKey = "grabl.username",
                names = {"--grabl-username"},
                description = "Grabl username")
        private String grablUsername;

        @CommandLine.Option(descriptionKey = "grabl.token",
                names = {"--grabl-token"},
                description = "Grabl account access token")
        private String grablToken;

        @CommandLine.Option(descriptionKey = "debug",
                names = {"--debug"},
                description = "Debug mode")
        private boolean debug;

        public Path dataDir() {
            if (data == null) return ServerDefaults.DATA_DIR;
            return Paths.get(data).isAbsolute()
                    ? Paths.get(data)
                    : ServerDefaults.GRAKN_DIR.resolve(data);
        }

        public Path logsDir() {
            if (logs == null) return ServerDefaults.LOGS_DIR;
            return Paths.get(logs).isAbsolute()
                    ? Paths.get(logs)
                    : ServerDefaults.GRAKN_DIR.resolve(logs);
        }

        public int port() {
            return port;
        }

        public boolean debug() {
            return debug;
        }

        public boolean grablTrace() {
            return grablTrace;
        }

        public URI grablURI() {
            return grablURI;
        }

        public String grablUsername() {
            return grablUsername;
        }

        public String grablToken() {
            return grablToken;
        }

        @Override
        public boolean isServer() {
            return true;
        }

        @Override
        public Server asServer() {
            return this;
        }
    }

    @CommandLine.Command(name = "import")
    public static class DataImport extends RunOptions {

        private final Server serverCommand;

        @CommandLine.Parameters(index = "0", description = "Database to import data into")
        private String database;

        @CommandLine.Parameters(index = "1", description = "File containing the data to import")
        private String filename;

        @CommandLine.Parameters(index = "2..*", arity = "0..*", description = "Schema concept remap labels")
        private Map<String, String> remapLabels = new LinkedHashMap<>();

        public DataImport(Server serverCommand) {
            this.serverCommand = serverCommand;
        }

        public String database() {
            return database;
        }

        public String filename() {
            return filename;
        }

        public Map<String, String> remapLabels() {
            return remapLabels;
        }

        public int port() {
            return serverCommand.port();
        }

        @Override
        public boolean isDataImport() {
            return true;
        }

        @Override
        public DataImport asDataImport() {
            return this;
        }
    }

    @CommandLine.Command(name = "export")
    public static class DataExport extends RunOptions {

        private final Server serverCommand;

        @CommandLine.Parameters(index = "0", description = "Database to export data from")
        private String database;

        @CommandLine.Parameters(index = "1", description = "File for the data to export to")
        private String filename;

        public DataExport(Server serverCommand) {
            this.serverCommand = serverCommand;
        }

        public String database() {
            return database;
        }

        public String filename() {
            return filename;
        }

        public int port() {
            return serverCommand.port();
        }

        @Override
        public boolean isDataExport() {
            return true;
        }

        @Override
        public DataExport asDataExport() {
            return this;
        }
    }
}

