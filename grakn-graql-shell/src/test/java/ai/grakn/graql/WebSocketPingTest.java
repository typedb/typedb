/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.graql;

import java.io.EOFException;
import java.io.IOException;
import org.eclipse.jetty.websocket.api.WebSocketException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import static org.mockito.Matchers.any;
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