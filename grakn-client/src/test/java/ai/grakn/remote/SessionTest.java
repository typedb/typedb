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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.remote;

import ai.grakn.GraknSession;
import ai.grakn.Keyspace;
import ai.grakn.util.SimpleURI;
import io.grpc.ManagedChannel;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Felix Chapman
 */
public class SessionTest {

    @Rule
    public final GrpcServerMock server = GrpcServerMock.create();

    private static final SimpleURI URI = new SimpleURI("localhost", 999);
    private final Keyspace KEYSPACE = Keyspace.of("lalala");

    @Test
    public void whenOpeningASession_ReturnARemoteGraknSession() {
        try (GraknSession session = Grakn.getSession(URI, KEYSPACE)) {
            assertTrue(Grakn.Session.class.isAssignableFrom(session.getClass()));
        }
    }

    @Test
    public void whenOpeningASessionWithAGivenUriAndKeyspace_TheUriAndKeyspaceAreSet() {
        try (GraknSession session = Grakn.getSession(URI, KEYSPACE)) {
            assertEquals(URI.toString(), session.uri());
            assertEquals(KEYSPACE, session.keyspace());
        }
    }

    @Test @Ignore
    public void whenClosingASession_ShutdownTheChannel() {
        ManagedChannel channel = mock(ManagedChannel.class);

        //GraknSession ignored = Session.create(KEYSPACE, URI, channel);
        //ignored.close();

        verify(channel).shutdown();
    }

    @Test @Ignore
    public void whenOpeningATransactionFromASession_ReturnATransactionWithParametersSet() {
//        try (GraknSession session = Session.create(KEYSPACE, URI, server.channel())) {
//            try (GraknTx tx = session.open(GraknTxType.READ)) {
//                assertEquals(session, tx.session());
//                assertEquals(KEYSPACE, tx.keyspace());
//                assertEquals(GraknTxType.READ, tx.txType());
//            }
//        }
    }
}