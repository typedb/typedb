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

import ai.grakn.GraknTx;
import ai.grakn.Keyspace;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.rpc.generated.GraknGrpc.GraknImplBase;
import ai.grakn.rpc.generated.GraknGrpc.GraknStub;
import ai.grakn.rpc.generated.GraknOuterClass.TxRequest;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.annotation.Nullable;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Felix Chapman
 */
public class GraknRemoteTxTest {

    @Rule
    public final GrpcServerRule serverRule = new GrpcServerRule();

    private GraknStub client;
    private @Nullable StreamObserver<TxResponse> serverResponses = null;
    private final GraknImplBase server = mock(GraknImplBase.class);
    private final GraknRemoteSession session = mock(GraknRemoteSession.class);

    private static final Keyspace KEYSPACE = Keyspace.of("blahblah");

    @Before
    public void setUp() {
        when(server.tx(any())).thenAnswer(args -> {
            serverResponses = args.getArgument(0);
            return new StreamObserver<TxRequest>() {
                @Override
                public void onNext(TxRequest value) {

                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onCompleted() {

                }
            };
        });

        serverRule.getServiceRegistry().addService(server);
        client = GraknGrpc.newStub(serverRule.getChannel());

        when(session.stub()).thenReturn(client);
        when(session.keyspace()).thenReturn(KEYSPACE);
    }

    @After
    public void tearDown() {
        if (serverResponses != null) {
            serverResponses.onCompleted();
        }
    }

    @Test
    public void whenCreatingAGraknRemoteTx_MakeATxCallToGrpc() {
        try (GraknTx tx = GraknRemoteTx.create(session)) {
            verify(server).tx(any());
        }
    }

    @Test
    public void whenClosingAGraknRemoteTx_SendCompletedMessageToGrpc() {
        try (GraknTx tx = GraknRemoteTx.create(session)) {
            verify(server).tx(any());
        }
    }

    @Test
    public void whenCreatingAGraknRemoteTxWithKeyspace_SetsKeyspaceOnTx() {
        try (GraknTx tx = GraknRemoteTx.create(session)) {
            assertEquals(KEYSPACE, tx.keyspace());
        }
    }
}
