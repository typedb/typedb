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

import ai.grakn.util.REST;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import static ai.grakn.util.REST.Request.ID_PARAMETER;

/**
 * Providing useful methods for the user of the GraknEngine client
 *
 * @author alexandraorth
 */
public class Client {

    /**
     * Check if Grakn Engine has been started
     *
     * @return true if Grakn Engine running, false otherwise
     */
    public static boolean serverIsRunning(String uri) {
        try {
            HttpURLConnection connection = (HttpURLConnection)
                    new URL("http://" + uri + REST.WebPath.GRAPH_FACTORY_URI).openConnection();
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

    protected String convert(String uri){
        return uri.replace(ID_PARAMETER, "{id}");
    }
}
