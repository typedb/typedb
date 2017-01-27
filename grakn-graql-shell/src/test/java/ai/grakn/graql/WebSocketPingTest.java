package ai.grakn.graql;

import org.eclipse.jetty.websocket.api.WebSocketException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WebSocketPingTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void whenWebSocketFailsThenPingShouldThrowException() {
        JsonSession session = mock(JsonSession.class);

        when(session.isOpen()).thenReturn(true);
        doThrow(new WebSocketException()).when(session).sendJson(any());

        exception.expect(WebSocketException.class);

        WebSocketPing.ping(session);
    }

    @Test
    public void whenWebSocketClosesThenPingShouldNotThrowException() {
        JsonSession session = mock(JsonSession.class);

        when(session.isOpen()).thenReturn(false);
        doThrow(new WebSocketException()).when(session).sendJson(any());

        WebSocketPing.ping(session);
    }
}