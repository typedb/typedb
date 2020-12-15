/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.server.util;

import grakn.core.common.exception.GraknException;
import grakn.core.server.Version;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public interface ServerCommand {
    default boolean isStart() {
        return false;
    }

    default Start asStart() {
        throw GraknException.of(ILLEGAL_CAST);
    }

    default boolean isImportData() {
        return false;
    }

    default ImportData asImportData() {
        throw GraknException.of(ILLEGAL_CAST);
    }

    @Command(name = "grakn server", mixinStandardHelpOptions = true, version = {Version.VERSION})
    class Start implements ServerCommand {

        @Option(descriptionKey = "server.data",
                names = {"--data"},
                description = "Directory in which database server data will be stored")
        private String data;

        @Option(descriptionKey = "server.port",
                names = {"--port"},
                defaultValue = ServerDefaults.DEFAULT_DATABASE_PORT + "",
                description = "Port number of database server in which GRPC clients will connect to")
        private int port;

        @Option(descriptionKey = "grabl.trace",
                names = {"--grabl-trace"},
                negatable = true,
                defaultValue = "false",
                description = "Enable Grabl performance tracing")
        private boolean grablTrace;

        @Option(descriptionKey = "grabl.uri",
                names = {"--grabl-uri"},
                description = "Grabl tracing server URI")
        private URI grablURI;

        @Option(descriptionKey = "grabl.username",
                names = {"--grabl-username"},
                description = "Grabl username")
        private String grablUsername;

        @Option(descriptionKey = "grabl.token",
                names = {"--grabl-token"},
                description = "Grabl account access token")
        private String grablToken;

        @Option(descriptionKey = "debug",
                names = {"--debug"},
                description = "Debug mode")
        private boolean debug;

        public Path dataDir() {
            if (data == null) return ServerDefaults.DATA_DIR;
            return Paths.get(data).isAbsolute()
                    ? Paths.get(data)
                    : ServerDefaults.GRAKN_DIR.resolve(data);
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
        public boolean isStart() {
            return true;
        }

        @Override
        public ServerCommand.Start asStart() {
            return this;
        }
    }

    @Command(name = "import")
    class ImportData implements ServerCommand {

        private final Start startCommand;

        @Parameters(  index = "0", description = "Database to import data into")
        private String database;

        @Parameters(index = "1", description = "File containing the data to import")
        private String filename;

        @Parameters(index = "2..*", arity = "0..*", description = "Schema concept remap labels")
        private Map<String, String> remapLabels = new LinkedHashMap<>();

        public ImportData(Start startCommand) {
            this.startCommand = startCommand;
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
            return startCommand.port();
        }

        @Override
        public boolean isImportData() {
            return true;
        }

        @Override
        public ImportData asImportData() {
            return this;
        }
    }
}

