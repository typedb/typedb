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
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

@Command(name = "grakn", mixinStandardHelpOptions = true, version = {Version.VERSION})
public class ServerOptions {

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

    public ServerOptions() {}

    public Path dataDir() {
        if (data == null) return ServerDefaults.DATA_DIR;
        return Paths.get(data).isAbsolute()
                ? Paths.get(data)
                : ServerDefaults.GRAKN_DIR.resolve(data);
    }

    public int port() {
        return port;
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
