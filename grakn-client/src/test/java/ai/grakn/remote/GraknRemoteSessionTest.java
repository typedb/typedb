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

package ai.grakn.remote;

import ai.grakn.Grakn;
import ai.grakn.GraknSession;
import ai.grakn.Keyspace;
import ai.grakn.util.SimpleURI;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Felix Chapman
 */
public class GraknRemoteSessionTest {

    private final SimpleURI URI = new SimpleURI("1.1.1.1", 1111);
    private final Keyspace KEYSPACE = Keyspace.of("lalala");

    @Test
    public void whenOpeningASession_ReturnARemoteGraknSessionImpl() {
        try (GraknSession session = RemoteGrakn.session(URI, KEYSPACE)) {
            assertTrue(GraknRemoteSession.class.isAssignableFrom(session.getClass()));
        }
    }

    @Test
    public void whenOpeningASessionWithAGivenUriAndKeyspace_TheUriAndKeyspaceAreSet() {
        try (GraknSession session = RemoteGrakn.session(URI, KEYSPACE)) {
            assertEquals(URI.toString(), session.uri());
            assertEquals(KEYSPACE, session.keyspace());
        }
    }
}