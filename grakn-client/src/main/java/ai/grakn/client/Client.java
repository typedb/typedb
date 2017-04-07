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

import ai.grakn.engine.TaskId;
import ai.grakn.util.REST;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import mjson.Json;
import org.apache.http.HttpResponse;
import org.apache.http.client.ResponseHandler;

import static ai.grakn.util.REST.Request.ID_PARAMETER;
import static java.util.stream.Collectors.joining;

/**
 * Providing useful methods for the user of the GraknEngine client
 *
 * @author alexandraorth
 */
public class Client {

    final ResponseHandler<Json> asJsonHandler = response -> {
        try(BufferedReader reader = new BufferedReader(
                new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))){
            return Json.read(reader.lines().collect(joining("\n")));
        }
    };


    /**
     * Check if Grakn Engine has been started
     *
     * @return true if Grakn Engine running, false otherwise
     */
    public static boolean serverIsRunning(String uri) {
        try {
            HttpURLConnection connection = (HttpURLConnection)
                    new URL("http://" + uri + REST.WebPath.System.CONFIGURATION).openConnection();
            connection.setRequestMethod("GET");
            connection.connect();

            InputStream inputStream = connection.getInputStream();
            if (inputStream.available() == 0) {
                return false;
            }
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    protected String convert(String uri, TaskId id){
        return uri.replace(ID_PARAMETER, id.getValue());
    }

    protected String exceptionFrom(HttpResponse response) throws IOException {
        return asJsonHandler.handleResponse(response).at("exception").asString();
    }
}
