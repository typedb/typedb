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

import grakn.core.server.Version;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

@Command(name = "grakn", mixinStandardHelpOptions = true, version = {Version.VERSION})
public class ServerOptions {

    public static final String DEFAULT_PROPERTIES_FILE = "server/conf/grakn.properties";
    public static final String GRAKN_LOGO_FILE = "server/resources/grakn-core-ascii.txt";
    public static final String DEFAULT_DATABASE_DIRECTORY = "server/db";
    public static final int DEFAULT_DATABASE_PORT = 48555;

    @CommandLine.Option(descriptionKey = "database.directory",
            names = {"--database-directory"},
            defaultValue = DEFAULT_DATABASE_DIRECTORY,
            description = "Directory to write database files")
    private String databaseDirectory;

    @CommandLine.Option(descriptionKey = "database.port",
            names = {"--database-port"},
            defaultValue = DEFAULT_DATABASE_PORT + "",
            description = "GRPC port for Grakn clients to connect to the server")
    private int databasePort;

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

    public ServerOptions() {}

    public Path databaseDirectory() {
        return Paths.get(databaseDirectory);
    }

    public int databasePort() {
        return databasePort;
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
}
