package ai.grakn.graql;

import org.eclipse.jetty.websocket.api.WebSocketException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.EOFException;
import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WebSocketPingTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void whenWebSocketFailsThenPingShouldThrowException() throws IOException {
        JsonSession session = mock(JsonSession.class);

        when(session.isOpen()).thenReturn(true);
        doThrow(new WebSocketException()).when(session).sendJson(any());

        exception.expect(RuntimeException.class);

        WebSocketPing.ping(session);
    }

    @Test
    public void whenWebSocketClosesThenPingShouldNotThrowException() throws IOException {
        JsonSession session = mock(JsonSession.class);

        when(session.isOpen()).thenReturn(false);
        doThrow(new WebSocketException()).when(session).sendJson(any());

        WebSocketPing.ping(session);
    }

    @Test
    public void whenWebSocketThrowsEOFExceptionAndIsClosedThenPingShouldNotThrowException() throws IOException {
        JsonSession session = mock(JsonSession.class);

        when(session.isOpen()).thenReturn(false);
        doThrow(EOFException.class).when(session).sendJson(any());

        WebSocketPing.ping(session);
    }
}