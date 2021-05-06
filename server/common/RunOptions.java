/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.server.common;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.server.Version;
import picocli.CommandLine;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public abstract class RunOptions {

    public boolean isServer() {
        return false;
    }

    public Server asServer() {
        throw TypeDBException.of(ILLEGAL_CAST, RunOptions.class, Server.class);
    }

    public boolean isDataImport() {
        return false;
    }

    public DataImport asDataImport() {
        throw TypeDBException.of(ILLEGAL_CAST, RunOptions.class, DataImport.class);
    }

    public boolean isDataExport() {
        return false;
    }

    public DataExport asDataExport() {
        throw TypeDBException.of(ILLEGAL_CAST, RunOptions.class, DataExport.class);
    }

    public boolean isPrintSchema() {
        return false;
    }

    @CommandLine.Command(name = "typedb server", mixinStandardHelpOptions = true, version = {Version.VERSION})
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

        @CommandLine.Option(descriptionKey = "vaticle.factory.trace",
                names = {"--vaticle-factory-trace"},
                negatable = true,
                defaultValue = "false",
                description = "Enable Vaticle Factory performance tracing")
        private boolean factoryTrace;

        @CommandLine.Option(descriptionKey = "vaticle.factory.uri",
                names = {"--vaticle-factory-uri"},
                description = "Vaticle Factory tracing server URI")
        private URI factoryURI;

        @CommandLine.Option(descriptionKey = "vaticle.factory.username",
                names = {"--vaticle-factory-username"},
                description = "Vaticle Factory username")
        private String factoryUsername;

        @CommandLine.Option(descriptionKey = "vaticle.factory.token",
                names = {"--vaticle-factory-token"},
                description = "Vaticle Factory account access token")
        private String factoryToken;

        @CommandLine.Option(descriptionKey = "debug",
                names = {"--debug"},
                description = "Debug mode")
        private boolean debug;

        public Path dataDir() {
            if (data == null) return ServerDefaults.DATA_DIR;
            return Paths.get(data).isAbsolute()
                    ? Paths.get(data)
                    : ServerDefaults.TYPEDB_DIR.resolve(data);
        }

        public Path logsDir() {
            if (logs == null) return ServerDefaults.LOGS_DIR;
            return Paths.get(logs).isAbsolute()
                    ? Paths.get(logs)
                    : ServerDefaults.TYPEDB_DIR.resolve(logs);
        }

        public int port() {
            return port;
        }

        public boolean debug() {
            return debug;
        }

        public boolean factoryTrace() {
            return factoryTrace;
        }

        public URI factoryURI() {
            return factoryURI;
        }

        public String factoryUsername() {
            return factoryUsername;
        }

        public String factoryToken() {
            return factoryToken;
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

