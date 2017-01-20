package ai.grakn.graql;

import mjson.Json;
import org.eclipse.jetty.websocket.api.WebSocketException;

import static ai.grakn.util.REST.RemoteShell.ACTION;
import static ai.grakn.util.REST.RemoteShell.ACTION_PING;

/**
 * Provides a method for pinging a JSON websocket session repeatedly
 */
class WebsocketPing {

    private static final int PING_INTERVAL = 60_000;

    static void ping(JsonSession session) {
        try {
            // This runs on a daemon thread, so it will be terminated when the JVM stops
            //noinspection InfiniteLoopStatement
            while (true) {
                session.sendJson(Json.object(ACTION, ACTION_PING));

                try {
                    Thread.sleep(PING_INTERVAL);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (WebSocketException e) {
            // Report an error if the session is still open
            if (session.isOpen()) {
                throw new RuntimeException(e);
            }
        }
    }
}
