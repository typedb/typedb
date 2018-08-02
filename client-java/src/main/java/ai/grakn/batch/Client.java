/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package ai.grakn.batch;

import ai.grakn.util.CommonUtil;
import ai.grakn.util.REST;
import ai.grakn.util.SimpleURI;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;

/**
 * Providing useful methods for the user of the GraknEngine client
 *
 * @author alexandraorth
 */
public final class Client {

    /**
     * Check if Grakn Engine has been started
     *
     * @return true if Grakn Engine running, false otherwise
     */
    public static boolean serverIsRunning(SimpleURI uri) {
        URL url;
        try {
            url = UriBuilder.fromUri(uri.toURI()).path(REST.WebPath.KB).build().toURL();
        } catch (MalformedURLException e) {
            throw CommonUtil.unreachableStatement(
                    "This will never throw because we're appending a known path to a valid URI", e
            );
        }

        HttpURLConnection connection;
        try {
            connection = (HttpURLConnection) mapQuadZeroRouteToLocalhost(url).openConnection();
        } catch (IOException e) {
            // If this fails, then the server is not reachable
            return false;
        }

        try {
            connection.setRequestMethod("GET");
        } catch (ProtocolException e) {
            throw CommonUtil.unreachableStatement(
                    "This will never throw because 'GET' is correct and the connection is not open yet", e
            );
        }

        int available;

        try {
            connection.connect();
            InputStream inputStream = connection.getInputStream();
            available = inputStream.available();
        } catch (IOException e) {
            // If this fails, then the server is not reachable
            return false;
        }

        return available != 0;
    }

    private static URL mapQuadZeroRouteToLocalhost(URL originalUrl) {
        final String QUAD_ZERO_ROUTE = "http://0.0.0.0";

        URL mappedUrl;
        if ((originalUrl.getProtocol() + originalUrl.getHost()).equals(QUAD_ZERO_ROUTE)) {
            try {
                mappedUrl = new URL(originalUrl.getProtocol(), "localhost", originalUrl.getPort(), REST.WebPath.KB);
            } catch (MalformedURLException e) {
                throw CommonUtil.unreachableStatement(
                        "This will never throw because the protocol is valid (because it came from another URL)", e
                );
            }
        } else {
            mappedUrl = originalUrl;
        }

        return mappedUrl;
    }

}
