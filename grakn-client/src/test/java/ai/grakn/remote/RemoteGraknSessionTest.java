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

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.util.SimpleURI;
import io.grpc.ManagedChannel;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Felix Chapman
 */
public class RemoteGraknSessionTest {

    @Rule
    public final GrpcServerMock server = new GrpcServerMock();

    private SimpleURI uri;
    private final Keyspace KEYSPACE = Keyspace.of("lalala");

    @Before
    public void setUp() {
        uri = server.uri();
    }

    @Test
    public void whenOpeningASession_ReturnARemoteGraknSession() {
        try (GraknSession session = RemoteGrakn.session(uri, KEYSPACE)) {
            assertTrue(RemoteGraknSession.class.isAssignableFrom(session.getClass()));
        }
    }

    @Test
    public void whenOpeningASessionWithAGivenUriAndKeyspace_TheUriAndKeyspaceAreSet() {
        try (GraknSession session = RemoteGrakn.session(uri, KEYSPACE)) {
            assertEquals(uri.toString(), session.uri());
            assertEquals(KEYSPACE, session.keyspace());
        }
    }

    @Test
    public void whenClosingASession_ShutdownTheChannel() {
        ManagedChannel channel = mock(ManagedChannel.class);

        GraknSession ignored = RemoteGraknSession.create(KEYSPACE, uri, channel);
        ignored.close();

        verify(channel).shutdown();
    }

    @Test
    public void whenOpeningATransactionFromASession_ReturnATransactionWithParametersSet() {
        try (GraknSession session = RemoteGraknSession.create(KEYSPACE, uri, server.channel())) {
            try (GraknTx tx = session.open(GraknTxType.READ)) {
                assertEquals(session, tx.session());
                assertEquals(KEYSPACE, tx.keyspace());
                assertEquals(GraknTxType.READ, tx.txType());
            }
        }
    }
}