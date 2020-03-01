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

package grakn.core.rule;

import grakn.core.common.config.Config;
import grakn.core.common.config.ConfigKey;
import grakn.core.server.GraknStorage;
import org.junit.rules.ExternalResource;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.DB_CACHE;

public class GraknTestStorage extends ExternalResource {
    private static final Path DEFAULT_CASSANDRA_CONFIG_PATH = Paths.get("test-integration/resources/cassandra-embedded.yaml");

    private final Path originalCassandraConfigPath;
    private File updatedCassandraConfigPath;
    private int storagePort;
    private int nativeTransportPort;

    public GraknTestStorage() {
        this(DEFAULT_CASSANDRA_CONFIG_PATH);
    }

    public GraknTestStorage(Path cassandraConfigPath) {
        // this has to be set to something to avoid throwing security manager errors
        System.setProperty("java.security.manager", "nottodaypotato");
        this.originalCassandraConfigPath = cassandraConfigPath;
    }

    @Override
    protected void before() {
        try {
            System.out.println("Starting Grakn Storage...");
            generateCassandraRandomPorts();
            updatedCassandraConfigPath = buildCassandraConfigWithRandomPorts();
            // cassandra will find its config file from the System Properties
            System.setProperty("cassandra.config", "file:" + updatedCassandraConfigPath.getAbsolutePath());
            System.setProperty("cassandra-foreground", "true");
            GraknStorage.main(new String[]{});
            System.out.println("Grakn Storage started");
        } catch (IOException e) {
            throw new RuntimeException("Cannot start components", e);
        }
    }

    @Override
    protected void after() {
        try {
            updatedCassandraConfigPath.delete();
        } catch (Exception e) {
            throw new RuntimeException("Could not shut down ", e);
        }
    }

    // Shared configs that need to be exposed to GraknServer as well

    int storagePort() {
        return storagePort;
    }

    int nativeTransportPort() {
        return nativeTransportPort;
    }


    /**
     * When launching cassandra only, we still want to be able to connect a JanusGraphFactory to cassandra
     * This expects a Janus-compatible configuration, which we create here on demand, essentially as a mock
     */
    public Config createCompatibleServerConfig() {
        Properties mockServerProperties = new Properties();
        mockServerProperties.setProperty(ConfigKey.STORAGE_HOSTNAME.name(), "127.0.0.1");
        mockServerProperties.setProperty(ConfigKey.STORAGE_PORT.name(), String.valueOf(nativeTransportPort));
        mockServerProperties.setProperty("cache." + DB_CACHE.getName(), String.valueOf(false));
        mockServerProperties.setProperty(ConfigKey.HADOOP_STORAGE_PORT.name(), String.valueOf(nativeTransportPort));
        mockServerProperties.setProperty(ConfigKey.TYPE_SHARD_THRESHOLD.name(), String.valueOf(250000));
        return Config.of(mockServerProperties);
    }

    // Cassandra Helpers

    private void generateCassandraRandomPorts() throws IOException {
        storagePort = findUnusedLocalPort();
        nativeTransportPort = findUnusedLocalPort();
    }

    private File buildCassandraConfigWithRandomPorts() throws IOException {
        byte[] bytes = Files.readAllBytes(originalCassandraConfigPath);
        String configString = new String(bytes, StandardCharsets.UTF_8);

        configString = configString + "\nstorage_port: " + storagePort;
        configString = configString + "\nnative_transport_port: " + nativeTransportPort;
        InputStream configStream = new ByteArrayInputStream(configString.getBytes(StandardCharsets.UTF_8));

        String directory = "target/embeddedCassandra";
        org.apache.cassandra.io.util.FileUtils.createDirectory(directory);
        Path copyName = Paths.get(directory, "cassandra-embedded.yaml");
        // Create file in directory we just created and copy the stream content into it.
        Files.copy(configStream, copyName);
        return copyName.toFile();
    }

    private synchronized static int findUnusedLocalPort() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }
}
