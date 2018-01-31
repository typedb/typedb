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
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.grpc.GrpcUtil;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.rpc.generated.GraknGrpc.GraknImplBase;
import ai.grakn.rpc.generated.GraknOuterClass.TxRequest;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;

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

    private static final String ERROR_DESC = "OH NOES";
    private static final StatusRuntimeException ERROR = new StatusRuntimeException(Status.UNKNOWN.withDescription(ERROR_DESC));
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Rule
    public final GrpcServerRule serverRule = new GrpcServerRule().directExecutor();

    private @Nullable StreamObserver<TxResponse> serverResponses = null;

    @SuppressWarnings("unchecked") // safe because mock
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
            serverResponses.onNext(GrpcUtil.doneResponse());
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
        try (GraknTx ignored = GraknRemoteTx.create(session, GraknTxType.WRITE)) {
            verify(server).tx(any());
        }
    }

    @Test
    public void whenCreatingAGraknRemoteTx_SendAnOpenMessageToGrpc() {
        try (GraknTx ignored = GraknRemoteTx.create(session, GraknTxType.WRITE)) {
            verify(serverRequests).onNext(GrpcUtil.openRequest(Keyspace.of(KEYSPACE.getValue()), GraknTxType.WRITE));
        }
    }

    @Test
    public void whenCreatingABatchGraknRemoteTx_SendAnOpenMessageWithBatchSpecifiedToGrpc() {
        try (GraknTx ignored = GraknRemoteTx.create(session, GraknTxType.BATCH)) {
            verify(serverRequests).onNext(GrpcUtil.openRequest(Keyspace.of(KEYSPACE.getValue()), GraknTxType.BATCH));
        }
    }

    @Test
    public void whenClosingAGraknRemoteTx_SendCompletedMessageToGrpc() {
        try (GraknTx ignored = GraknRemoteTx.create(session, GraknTxType.WRITE)) {
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
        Query<?> query = mock(Query.class);
        String queryString = "match $x isa person; get $x;";
        when(query.toString()).thenReturn(queryString);

        try (GraknTx tx = GraknRemoteTx.create(session, GraknTxType.WRITE)) {
            verify(serverRequests).onNext(any()); // The open request

            tx.graql().execute(query);
        }

        verify(serverRequests).onNext(GrpcUtil.execQueryRequest(queryString));
    }

    @Test
    public void whenExecutingAQueryWithInferenceSet_SendAnExecQueryWithInferenceSetMessageToGrpc() {
        Query<?> query = mock(Query.class);
        String queryString = "match $x isa person; get $x;";
        when(query.toString()).thenReturn(queryString);

        try (GraknTx tx = GraknRemoteTx.create(session, GraknTxType.WRITE)) {
            verify(serverRequests).onNext(any()); // The open request

            QueryBuilder graql = tx.graql();

            graql.infer(true);
            graql.execute(query);
            verify(serverRequests).onNext(GrpcUtil.execQueryRequest(queryString, true));

            graql.infer(false);
            graql.execute(query);
            verify(serverRequests).onNext(GrpcUtil.execQueryRequest(queryString, false));
        }
    }

    @Test
    public void whenCommitting_SendACommitMessageToGrpc() {
        try (GraknTx tx = GraknRemoteTx.create(session, GraknTxType.WRITE)) {
            verify(serverRequests).onNext(any()); // The open request

            tx.commit();
        }

        verify(serverRequests).onNext(GrpcUtil.commitRequest());
    }

    @Test
    public void whenCreatingAGraknRemoteTxWithKeyspace_SetsKeyspaceOnTx() {
        try (GraknTx tx = GraknRemoteTx.create(session, GraknTxType.WRITE)) {
            assertEquals(KEYSPACE, tx.keyspace());
        }
    }

    @Test
    public void whenOpeningATxFails_Throw() {
        throwOn(GrpcUtil.openRequest(Keyspace.of(KEYSPACE.getValue()), GraknTxType.WRITE));

        exception.expect(GraknException.class);
        exception.expectMessage(ERROR_DESC);

        GraknTx tx = GraknRemoteTx.create(session, GraknTxType.WRITE);
        tx.close();
    }

    @Test
    public void whenCommittingATxFails_Throw() {
        throwOn(GrpcUtil.commitRequest());

        try (GraknTx tx = GraknRemoteTx.create(session, GraknTxType.WRITE)) {

            exception.expect(GraknException.class);
            exception.expectMessage(ERROR_DESC);

            tx.commit();
        }
    }

    private void throwOn(TxRequest request) {
        doAnswer(args -> {
            assert serverResponses != null;
            serverResponses.onError(ERROR);
            return null;
        }).when(serverRequests).onNext(request);
    }
}
