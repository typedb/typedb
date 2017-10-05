/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.client;

import ai.grakn.GraknSystemProperty;
import ai.grakn.engine.TaskId;
import ai.grakn.util.REST;
import mjson.Json;
import org.apache.http.HttpResponse;
import org.apache.http.client.ResponseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static ai.grakn.util.REST.Request.ID_PARAMETER;
import static java.util.stream.Collectors.joining;

/**
 * Providing useful methods for the user of the GraknEngine client
 *
 * @author alexandraorth
 */
public class Client {

    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    private enum EngineStatus {
        Running(0),
        NotRunning(1),
        Error(2);

        private final int exitCode;

        EngineStatus(int exitCode) {
            this.exitCode = exitCode;
        }
    }

    final ResponseHandler<Json> asJsonHandler = response -> {
        try(BufferedReader reader = new BufferedReader(
                new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))){
            return Json.read(reader.lines().collect(joining("\n")));
        }
    };

    final ResponseHandler<String> asStringHandler = response -> {
        try(BufferedReader reader = new BufferedReader(
                new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))){
            return reader.lines().collect(joining("\n"));
        }
    };

    public static void main(String[] args) {
        EngineStatus engineStatus;
        try {
            engineStatus = checkServerRunning();
        } catch (Exception e) {
            LOG.error("An unexpected error occurred", e);
            engineStatus = EngineStatus.Error;
        }
        System.exit(engineStatus.exitCode);
    }

    private static EngineStatus checkServerRunning() throws IOException {
        String confPath = GraknSystemProperty.CONFIGURATION_FILE.value();

        if (confPath == null) {
            String msg = "System property `" + GraknSystemProperty.CONFIGURATION_FILE.key() + "` has not been set";
            LOG.error(msg);
            return EngineStatus.Error;
        }

        Properties properties = new Properties();
        try (FileInputStream stream = new FileInputStream(confPath)) {
            properties.load(stream);
        }

        String host = properties.getProperty("server.host");
        int port = Integer.parseInt(properties.getProperty("server.port"));
        if (serverIsRunning(host, port)) {
            LOG.info("Server " + host + ":" + port + " is running");
            return EngineStatus.Running;
        } else {
            LOG.info("Server " + host + ":" + port + " is not running");
            return EngineStatus.NotRunning;
        }
    }

    public static boolean serverIsRunning(String hostAndPort) {
        String[] split = hostAndPort.split(":", 2);
        return serverIsRunning(split[0], Integer.parseInt(split[1]));
    }

    /**
     * Check if Grakn Engine has been started
     *
     * @return true if Grakn Engine running, false otherwise
     */
    public static boolean serverIsRunning(String host, int port) {
        try {
            URL url = new URL("http", host, port, REST.WebPath.System.CONFIGURATION);

            HttpURLConnection connection = (HttpURLConnection) mapQuadZeroRouteToLocalhost(url).openConnection();

            connection.setRequestMethod("GET");

            try {
                connection.connect();
            } catch (IOException e) {
                // If this fails, then the server is not reachable
                return false;
            }

            InputStream inputStream = connection.getInputStream();
            if (inputStream.available() == 0) {
                LOG.error("input stream is not available");
                return false;
            }
            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static URL mapQuadZeroRouteToLocalhost(URL originalUrl) throws MalformedURLException {
        final String QUAD_ZERO_ROUTE = "http://0.0.0.0";

        URL mappedUrl;
        if ((originalUrl.getProtocol() + originalUrl.getHost()).equals(QUAD_ZERO_ROUTE)) {
            mappedUrl = new URL(
                    originalUrl.getProtocol(), "localhost", originalUrl.getPort(), REST.WebPath.System.CONFIGURATION);
        } else {
            mappedUrl = originalUrl;
        }

        return mappedUrl;
    }

    protected String convert(String uri, TaskId id){
        return uri.replace(ID_PARAMETER, id.getValue());
    }

    protected String exceptionFrom(HttpResponse response) throws IOException {
        return asJsonHandler.handleResponse(response).at("exception").asString();
    }
}
