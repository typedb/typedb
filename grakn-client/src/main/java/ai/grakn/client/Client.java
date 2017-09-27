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
import java.io.FileNotFoundException;
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

    public static void main(String[] args) throws IOException {
        int result;
        try {
            result = checkServerRunning();
        } catch (Exception e) {
            LOG.error("An exception has occurred", e);
            throw e;
        }
        System.exit(result);
    }

    private static int checkServerRunning() throws IOException {
        String confPath = GraknSystemProperty.CONFIGURATION_FILE.value();

        if (confPath == null) {
            String msg = "System property `" + GraknSystemProperty.CONFIGURATION_FILE.key() + "` has not been set";
            LOG.error(msg);
            return 2;
        }

        Properties properties = new Properties();
        try (FileInputStream stream = new FileInputStream(confPath)) {
            properties.load(stream);
        } catch (FileNotFoundException e) {
            LOG.error("Could not find config file at `" + confPath + "`");
            return 2;
        }

        String serverUri = properties.getProperty("server.host") + ":" + properties.getProperty("server.port");
        if (serverIsRunning(serverUri)) {
            LOG.info("Server " + serverUri + " is running");
            return 0;
        } else {
            LOG.info("Server " + serverUri + " is not running");
            return 1;
        }
    }

    /**
     * Check if Grakn Engine has been started
     *
     * @return true if Grakn Engine running, false otherwise
     */
    public static boolean serverIsRunning(String uri) {
        try {
            URL url = new URL("http://" + uri + REST.WebPath.System.CONFIGURATION);

            HttpURLConnection connection = (HttpURLConnection) mapQuadZeroRouteToLocalhost(url).openConnection();
            connection.setRequestMethod("GET");
            connection.connect();

            InputStream inputStream = connection.getInputStream();
            if (inputStream.available() == 0) {
                LOG.error("input stream is not available");
                return false;
            }
        } catch (IOException e) {
            LOG.error("An exception has occurred", e);
            return false;
        }
        return true;
    }

    private static URL mapQuadZeroRouteToLocalhost(URL originalUrl) throws MalformedURLException {
        final String QUAD_ZERO_ROUTE = "http://0.0.0.0";

        URL mappedUrl;
        if ((originalUrl.getProtocol() + originalUrl.getHost()).equals(QUAD_ZERO_ROUTE)) {
            mappedUrl = new URL(originalUrl.getProtocol() + "://" +
                "localhost" + ":" + originalUrl.getPort() +
                REST.WebPath.System.CONFIGURATION);
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
