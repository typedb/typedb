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

package ai.grakn.util;

import ai.grakn.Grakn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;


/**
 * <p>
 *     Establishes communication between the graph and engine
 * </p>
 *
 * <p>
 *     Class dedicated to talking with Grakn Engine. Currently used to retrieve factory config and submit commit logs.
 *
 *     The communication with engine is bypassed whenever the engineURL provided is a in-memory location.
 * </p>
 *
 * @author fppt
 */
public class EngineCommunicator {
    private static final Logger LOG = LoggerFactory.getLogger(EngineCommunicator.class);
    private static final String DEFAULT_PROTOCOL = "http://";
    private static final int MAX_RETRY = 5;

    /**
     *
     * @param engineUrl The location of engine.
     * @param restType The type of request to make to engine.
     * @param body The body to attach to the request
     * @return The result of the request
     */
    public static String contactEngine(String engineUrl, String restType, String body){
        if(engineUrl.equals(Grakn.IN_MEMORY)) {
            return "Engine not contacted due to in memory graph being used";
        }

        for(int i = 0; i < MAX_RETRY; i++) {
            try {
                URL url = new URL(DEFAULT_PROTOCOL + engineUrl);
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestProperty("Content-Type", "application/json");
                connection.setRequestMethod(restType);

                if (body != null) {
                    connection.setDoOutput(true);
                    try (DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
                        wr.write(body.getBytes(StandardCharsets.UTF_8));
                    }
                }

                if (connection.getResponseCode() != 200) {
                    throw new IllegalArgumentException(ErrorMessage.INVALID_ENGINE_RESPONSE.getMessage(engineUrl, connection.getResponseCode()));
                }

                //Reading from Connection
                StringBuilder sb = new StringBuilder();
                try (BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        sb.append("\n").append(line);
                    }
                }
                return sb.toString();
            } catch (IOException e) {
                LOG.error(ErrorMessage.COULD_NOT_REACH_ENGINE.getMessage(engineUrl), e);
            }
        }
        throw new RuntimeException(ErrorMessage.COULD_NOT_REACH_ENGINE.getMessage(engineUrl));
    }

    /**
     *
     * @param engineUrl The location of engine.
     * @param restType The type of resquest to make to engine.
     * @return The result of the request
     */
    public static String contactEngine(String engineUrl, String restType){
        return contactEngine(engineUrl, restType, null);
    }
}
