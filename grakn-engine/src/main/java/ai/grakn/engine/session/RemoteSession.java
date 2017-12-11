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

package ai.grakn.engine.session;

import ai.grakn.Grakn;
import ai.grakn.GraknSession;
import ai.grakn.util.REST;
import mjson.Json;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static ai.grakn.util.REST.RemoteShell.ACTION;
import static ai.grakn.util.REST.RemoteShell.ACTION_INIT;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

/**
 * Web socket for running a Graql shell
 *
 * @author Felix Chapman
 */
public class RemoteSession extends WebSocketAdapter {
    private final Map<Session, GraqlSession> sessions = new HashMap<>();
    private final Logger LOG = LoggerFactory.getLogger(RemoteSession.class);

    public static RemoteSession create() {
        return new RemoteSession();
    }

    @Override
    public void onWebSocketClose(int statusCode, String reason) {
        String message = "Websocket closed, code: " + statusCode + ", reason: " + reason;
        // 1000 = Normal close, 1001 = Going away
        if (statusCode == 1000 || statusCode == 1001) {
            LOG.debug(message);
        } else {
            LOG.error(message);
        }
        sessions.remove(getSession()).close();
    }

    @Override
    public void onWebSocketText(String message) {
        try {
            LOG.debug("Received message: " + message);

            Json json = Json.read(message);

            if (json.is(ACTION, ACTION_INIT)) {
                startSession(json);
            } else {
                sessions.get(getSession()).handleMessage(json);
            }
        } catch (Throwable e) {
            LOG.error("Exception", getFullStackTrace(e));
            throw e;
        }
    }

    /**
     * Start a new Graql shell session
     */
    private void startSession(Json json) {
        String keyspace = json.at(REST.RemoteShell.KEYSPACE).asString();
        String outputFormat = json.at(REST.RemoteShell.OUTPUT_FORMAT).asString();
        boolean infer = json.at(REST.RemoteShell.INFER).asBoolean();
        GraknSession factory = Grakn.session(Grakn.DEFAULT_URI, keyspace);
        GraqlSession graqlSession = new GraqlSession(
                getSession(), factory, outputFormat, infer
        );
        sessions.put(getSession(), graqlSession);
    }
}

