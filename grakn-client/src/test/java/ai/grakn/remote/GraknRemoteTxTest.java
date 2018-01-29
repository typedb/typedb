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
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.exception.GraknException;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.rpc.generated.GraknGrpc.GraknImplBase;
import ai.grakn.rpc.generated.GraknOuterClass;
import ai.grakn.rpc.generated.GraknOuterClass.Done;
import ai.grakn.rpc.generated.GraknOuterClass.Open;
import ai.grakn.rpc.generated.GraknOuterClass.TxRequest;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;
import ai.grakn.rpc.generated.GraknOuterClass.TxType;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;

import static ai.grakn.graql.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Felix Chapman
 */
public class GraknRemoteTxTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Rule
    public final GrpcServerRule serverRule = new GrpcServerRule().directExecutor();

    private @Nullable StreamObserver<TxResponse> serverResponses = null;
    private StreamObserver<TxRequest> serverRequests = mock(StreamObserver.class);
    private final GraknImplBase server = mock(GraknImplBase.class);
    private final GraknRemoteSession session = mock(GraknRemoteSession.class);

    private static final Keyspace KEYSPACE = Keyspace.of("blahblah");

    @Before
    public void setUp() {
        when(server.tx(any())).thenAnswer(args -> {
            assert serverResponses == null;
            serverResponses = args.getArgument(0);
            return serverRequests;
        });

        doAnswer(args -> {
            assert serverResponses != null;
            serverResponses.onNext(doneResponse());
            return null;
        }).when(serverRequests).onNext(any());

        doAnswer(args -> {
            assert serverResponses != null;
            serverResponses.onCompleted();
            return null;
        }).when(serverRequests).onCompleted();

        serverRule.getServiceRegistry().addService(server);

        when(session.stub()).thenReturn(GraknGrpc.newStub(serverRule.getChannel()));
        when(session.keyspace()).thenReturn(KEYSPACE);
    }

    @After
    public void tearDown() {
        if (serverResponses != null) {
            try {
                serverResponses.onCompleted();
            } catch (IllegalStateException e) {
                // this occurs if something has already ended the call
            }
        }
    }

    @Test
    public void whenCreatingAGraknRemoteTx_MakeATxCallToGrpc() {
        try (GraknTx tx = GraknRemoteTx.create(session, GraknTxType.WRITE)) {
            verify(server).tx(any());
        }
    }

    @Test
    public void whenCreatingAGraknRemoteTx_SendAnOpenMessageToGrpc() {
        try (GraknTx tx = GraknRemoteTx.create(session, GraknTxType.WRITE)) {
            verify(serverRequests).onNext(TxRequest.newBuilder().setOpen(Open.newBuilder().setKeyspace(GraknOuterClass.Keyspace.newBuilder().setValue(KEYSPACE.getValue())).setTxType(TxType.Write)).build());
        }
    }

    @Test
    public void whenClosingAGraknRemoteTx_SendCompletedMessageToGrpc() {
        try (GraknTx tx = GraknRemoteTx.create(session, GraknTxType.WRITE)) {
            verify(serverRequests, never()).onCompleted(); // Make sure transaction is still open here
        }

        verify(serverRequests).onCompleted();
    }

    @Test
    public void whenCreatingAGraknRemoteTxWithSession_SetKeyspaceOnTx() {
        try (GraknTx tx = GraknRemoteTx.create(session, GraknTxType.WRITE)) {
            assertEquals(session, tx.session());
        }
    }

    @Test
    public void whenExecutingAQuery_SendAnExecQueryMessageToGrpc() {
        try (GraknTx tx = GraknRemoteTx.create(session, GraknTxType.WRITE)) {
            tx.graql().match(var("x").isa("person")).get().execute();
        }

//        verify(serverRequests).onNext(TxRequest.newBuilder().)
    }

    @Test
    public void whenCreatingAGraknRemoteTxWithKeyspace_SetsKeyspaceOnTx() {
        try (GraknTx tx = GraknRemoteTx.create(session, GraknTxType.WRITE)) {
            assertEquals(KEYSPACE, tx.keyspace());
        }
    }

    @Test
    public void whenOpeningATxFails_Throw() {
        doAnswer(args -> {
            serverResponses.onError(new RuntimeException("UHOH"));
            return null;
        }).when(serverRequests).onNext(openRequest(KEYSPACE.getValue(), TxType.Write));

        exception.expect(GraknException.class);

        try (GraknTx tx = GraknRemoteTx.create(session, GraknTxType.WRITE)) {
        }
    }

    // TODO: we have copied this too many times
    private static TxRequest openRequest(String keyspaceString, TxType txType) {
        GraknOuterClass.Keyspace keyspace = GraknOuterClass.Keyspace.newBuilder().setValue(keyspaceString).build();
        Open.Builder open = Open.newBuilder().setKeyspace(keyspace).setTxType(txType);
        return TxRequest.newBuilder().setOpen(open).build();
    }

    private static TxResponse doneResponse() {
        return TxResponse.newBuilder().setDone(Done.getDefaultInstance()).build();
    }
}
