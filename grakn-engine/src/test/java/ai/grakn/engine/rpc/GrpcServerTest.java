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

package ai.grakn.engine.rpc;

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.exception.GraknException;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.rpc.GraknGrpc;
import ai.grakn.rpc.GraknGrpc.GraknStub;
import ai.grakn.rpc.GraknOuterClass;
import ai.grakn.rpc.GraknOuterClass.Commit;
import ai.grakn.rpc.GraknOuterClass.Done;
import ai.grakn.rpc.GraknOuterClass.ExecQuery;
import ai.grakn.rpc.GraknOuterClass.Infer;
import ai.grakn.rpc.GraknOuterClass.Next;
import ai.grakn.rpc.GraknOuterClass.Open;
import ai.grakn.rpc.GraknOuterClass.QueryResult;
import ai.grakn.rpc.GraknOuterClass.Stop;
import ai.grakn.rpc.GraknOuterClass.TxRequest;
import ai.grakn.rpc.GraknOuterClass.TxResponse;
import ai.grakn.rpc.GraknOuterClass.TxType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static ai.grakn.engine.rpc.GrpcUtil.hasMessage;
import static ai.grakn.engine.rpc.GrpcUtil.hasStatus;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Felix Chapman
 */
public class GrpcServerTest {

    private static final int PORT = 5555;
    private static final String MYKS = "myks";
    private static final String QUERY = "match $x isa person; get;";

    private final EngineGraknTxFactory txFactory = mock(EngineGraknTxFactory.class);
    private final GraknTx tx = mock(GraknTx.class);
    private final GetQuery query = mock(GetQuery.class);

    private GrpcServer server;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    // TODO: usePlainText is not secure
    private final ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", PORT).usePlaintext(true).build();
    private final GraknStub stub = GraknGrpc.newStub(channel);

    @Before
    public void setUp() throws IOException {
        server = GrpcServer.create(PORT, txFactory);

        QueryBuilder qb = mock(QueryBuilder.class);

        when(txFactory.tx(Keyspace.of(MYKS), GraknTxType.WRITE)).thenReturn(tx);
        when(tx.graql()).thenReturn(qb);
        when(qb.parse(QUERY)).thenReturn(query);
    }

    @After
    public void tearDown() throws InterruptedException {
        server.close();
    }

    @Test
    public void whenOpeningAReadTransactionRemotely_AReadTransactionIsOpened() {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Read));
        }

        verify(txFactory).tx(Keyspace.of(MYKS), GraknTxType.READ);
    }

    @Test
    public void whenOpeningAWriteTransactionRemotely_AWriteTransactionIsOpened() {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
        }

        verify(txFactory).tx(Keyspace.of(MYKS), GraknTxType.WRITE);
    }

    @Test
    public void whenOpeningABatchTransactionRemotely_ABatchTransactionIsOpened() {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Batch));
        }

        verify(txFactory).tx(Keyspace.of(MYKS), GraknTxType.BATCH);
    }

    @Test
    public void whenOpeningATransactionRemotely_ReceiveADoneMessage() {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Read));
            TxResponse response = tx.receive().elem();

            assertEquals(doneResponse(), response);
        }
    }

    @Test
    public void whenCommittingATransactionRemotely_TheTransactionIsCommitted() {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.send(commitRequest());
        }

        verify(tx).commit();
    }

    @Test
    public void whenCommittingATransactionRemotely_ReceiveADoneMessage() {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.send(commitRequest());
            TxResponse response = tx.receive().elem();

            assertEquals(doneResponse(), response);
        }
    }

    @Test
    public void whenOpeningTwoTransactions_TransactionsAreOpenedInDifferentThreads() {
        List<Thread> threads = new ArrayList<>();

        when(txFactory.tx(Keyspace.of(MYKS), GraknTxType.WRITE)).thenAnswer(invocation -> {
            threads.add(Thread.currentThread());
            return tx;
        });

        try (
                BidirectionalObserver<TxRequest, TxResponse> tx1 = startTx();
                BidirectionalObserver<TxRequest, TxResponse> tx2 = startTx()
        ) {
            tx1.send(openRequest(MYKS, TxType.Write));
            tx2.send(openRequest(MYKS, TxType.Write));
        }

        assertNotEquals(threads.get(0), threads.get(1));
    }

    @Test
    public void whenOpeningATransactionRemotelyWithAnInvalidKeyspace_Throw() throws Throwable {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest("not!@akeyspace", TxType.Write));

            exception.expect(hasMessage(GraknTxOperationException.invalidKeyspace("not!@akeyspace").getMessage()));

            throw tx.receive().throwable();
        }
    }

    @Test
    public void whenClosingATransactionRemotely_TheTransactionIsClosedWithinTheThreadItWasCreated() {
        final Thread[] threadOpenedWith = new Thread[1];
        final Thread[] threadClosedWith = new Thread[1];

        when(txFactory.tx(Keyspace.of(MYKS), GraknTxType.WRITE)).thenAnswer(invocation -> {
            threadOpenedWith[0] = Thread.currentThread();
            return tx;
        });

        doAnswer(invocation -> {
            threadClosedWith[0] = Thread.currentThread();
            return null;
        }).when(tx).close();

        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
        }

        verify(tx).close();
        assertEquals(threadOpenedWith[0], threadClosedWith[0]);
    }

    @Test
    public void whenExecutingAQueryRemotely_TheQueryIsParsedAndExecuted() {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.send(execQueryRequest(QUERY));
        }

        ai.grakn.graql.Query<?> query = tx.graql().parse(QUERY);
        verify(query).results(any());
    }

    @Test
    public void whenExecutingAQueryRemotely_AResultIsReturned() {
        Concept conceptX = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(conceptX.getId()).thenReturn(ConceptId.of("V123"));
        when(conceptX.isThing()).thenReturn(true);
        when(conceptX.asThing().type().getLabel()).thenReturn(Label.of("L123"));

        Concept conceptY = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(conceptY.getId()).thenReturn(ConceptId.of("V456"));
        when(conceptY.isThing()).thenReturn(true);
        when(conceptY.asThing().type().getLabel()).thenReturn(Label.of("L456"));

        ImmutableList<Answer> answers = ImmutableList.of(
                new QueryAnswer(ImmutableMap.of(Graql.var("x"), conceptX)),
                new QueryAnswer(ImmutableMap.of(Graql.var("y"), conceptY))
        );

        // TODO: reduce wtf
        when(query.results(any())).thenAnswer(params -> query.stream().map(params.<GrpcConverter>getArgument(0)::convert));
        when(query.stream()).thenAnswer(params -> answers.stream());

        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.receive();

            tx.send(execQueryRequest(QUERY));
            TxResponse response1 = tx.receive().elem();

            GraknOuterClass.Concept rpcX = GraknOuterClass.Concept.newBuilder().setId("V123").build();
            GraknOuterClass.Answer.Builder answerX = GraknOuterClass.Answer.newBuilder().putAnswer("x", rpcX);
            QueryResult.Builder resultX = QueryResult.newBuilder().setAnswer(answerX);
            assertEquals(TxResponse.newBuilder().setQueryResult(resultX).build(), response1);

            tx.send(nextRequest());
            TxResponse response2 = tx.receive().elem();

            GraknOuterClass.Concept rpcY = GraknOuterClass.Concept.newBuilder().setId("V456").build();
            GraknOuterClass.Answer.Builder answerY = GraknOuterClass.Answer.newBuilder().putAnswer("y", rpcY);
            QueryResult.Builder resultY = QueryResult.newBuilder().setAnswer(answerY);
            assertEquals(TxResponse.newBuilder().setQueryResult(resultY).build(), response2);

            tx.send(nextRequest());
            TxResponse response3 = tx.receive().elem();

            TxResponse expected = doneResponse();
            assertEquals(expected, response3);

            tx.send(stopRequest());
        }
    }

    @Test(timeout = 1000) // This tests uses an endless stream, so a failure may cause it to never terminate
    public void whenExecutingAQueryRemotelyAndAskingForOneResult_OnlyOneResultIsReturned() throws InterruptedException {
        Concept conceptX = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(conceptX.getId()).thenReturn(ConceptId.of("V123"));
        when(conceptX.isThing()).thenReturn(true);
        when(conceptX.asThing().type().getLabel()).thenReturn(Label.of("L123"));

        Concept conceptY = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(conceptY.getId()).thenReturn(ConceptId.of("V456"));
        when(conceptY.isThing()).thenReturn(true);
        when(conceptY.asThing().type().getLabel()).thenReturn(Label.of("L456"));

        ImmutableList<Answer> answers = ImmutableList.of(
                new QueryAnswer(ImmutableMap.of(Graql.var("x"), conceptX)),
                new QueryAnswer(ImmutableMap.of(Graql.var("y"), conceptY))
        );

        // TODO: reduce wtf
        when(query.results(any())).thenAnswer(params -> query.stream().map(params.<GrpcConverter>getArgument(0)::convert));

        // Produce an endless stream of results - this means if the behaviour is not lazy this will never terminate
        when(query.stream()).thenAnswer(params -> Stream.generate(answers::stream).flatMap(Function.identity()));

        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.receive();

            tx.send(execQueryRequest(QUERY));
            tx.receive();

            tx.send(nextRequest());
            tx.receive().elem();

            tx.send(stopRequest());

            TxResponse response = tx.receive().elem();

            assertEquals(doneResponse(), response);
        }
    }

    @Test
    public void whenExecutingQueryWithoutInferenceSet_InferenceIsNotSet() {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.send(execQueryRequest(QUERY));
            tx.send(nextRequest());
            tx.send(stopRequest());
        }

        verify(tx.graql(), times(0)).infer(anyBoolean());
    }

    @Test
    public void whenExecutingQueryWithInferenceOff_InferenceIsTurnedOff() {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.send(execQueryRequest(QUERY, false));
            tx.send(nextRequest());
            tx.send(stopRequest());
        }

        verify(tx.graql()).infer(false);
    }

    @Test
    public void whenExecutingQueryWithInferenceOn_InferenceIsTurnedOn() {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.send(execQueryRequest(QUERY, true));
            tx.send(nextRequest());
            tx.send(stopRequest());
        }

        verify(tx.graql()).infer(true);
    }

    @Test
    public void whenCommittingBeforeOpeningTx_Throw() throws Throwable {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(commitRequest());

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().throwable();
        }
    }

    @Test
    public void whenExecutingAQueryBeforeOpeningTx_Throw() throws Throwable {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(execQueryRequest(QUERY));

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().throwable();
        }
    }

    @Test
    public void whenOpeningTxTwice_Throw() throws Throwable {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.receive();

            tx.send(openRequest(MYKS, TxType.Write));

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().throwable();
        }
    }

    @Test
    public void whenOpeningTxFails_Throw() throws Throwable {
        when(txFactory.tx(Keyspace.of(MYKS), GraknTxType.WRITE)).thenThrow(GraknExceptionFake.EXCEPTION);

        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));

            exception.expect(hasMessage(GraknExceptionFake.MESSAGE));

            throw tx.receive().throwable();
        }
    }

    @Test
    public void whenCommittingFails_Throw() throws Throwable {
        doThrow(GraknExceptionFake.EXCEPTION).when(tx).commit();

        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.receive();

            tx.send(commitRequest());

            exception.expect(hasMessage(GraknExceptionFake.MESSAGE));

            throw tx.receive().throwable();
        }
    }

    @Test
    public void whenParsingQueryFails_Throw() throws Throwable {
        when(tx.graql().parse(QUERY)).thenThrow(GraknExceptionFake.EXCEPTION);

        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.receive();

            tx.send(execQueryRequest(QUERY));

            exception.expect(hasMessage(GraknExceptionFake.MESSAGE));

            throw tx.receive().throwable();
        }
    }

    @Test
    public void whenExecutingQueryFails_Throw() throws Throwable {
        when(query.results(any())).thenThrow(GraknExceptionFake.EXCEPTION);

        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.receive();

            tx.send(execQueryRequest(QUERY));

            exception.expect(hasMessage(GraknExceptionFake.MESSAGE));

            throw tx.receive().throwable();
        }
    }

    @Test
    public void whenSendingNextBeforeQuery_Throw() throws Throwable {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.receive();

            tx.send(nextRequest());

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().throwable();
        }
    }

    @Test
    public void whenSendingStopBeforeQuery_Throw() throws Throwable {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.receive();

            tx.send(stopRequest());

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().throwable();
        }
    }

    @Test
    public void whenSendingNextAfterStop_Throw() throws Throwable {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.receive();

            tx.send(execQueryRequest(QUERY));
            tx.receive();

            tx.send(stopRequest());
            tx.receive();

            tx.send(nextRequest());

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().throwable();
        }
    }

    @Test
    public void whenSendingAnotherQueryDuringQueryExecution_Throw() throws Throwable {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(MYKS, TxType.Write));
            tx.receive();

            tx.send(execQueryRequest(QUERY));
            tx.receive();

            tx.send(execQueryRequest(QUERY));

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().throwable();
        }
    }

    private BidirectionalObserver<TxRequest, TxResponse> startTx() {
        return BidirectionalObserver.create(stub::tx);
    }

    private static TxRequest openRequest(String keyspaceString, TxType txType) {
        GraknOuterClass.Keyspace keyspace = GraknOuterClass.Keyspace.newBuilder().setValue(keyspaceString).build();
        Open.Builder open = Open.newBuilder().setKeyspace(keyspace).setTxType(txType);
        return TxRequest.newBuilder().setOpen(open).build();
    }

    private static TxRequest commitRequest() {
        return TxRequest.newBuilder().setCommit(Commit.getDefaultInstance()).build();
    }

    private static TxRequest execQueryRequest(String queryString) {
        return execQueryRequest(queryString, Infer.getDefaultInstance());
    }

    private static TxRequest execQueryRequest(String queryString, boolean infer) {
        Infer inferMessage = Infer.newBuilder().setValue(infer).setIsSet(true).build();
        return execQueryRequest(queryString, inferMessage);
    }

    private static TxRequest execQueryRequest(String queryString, Infer infer) {
        GraknOuterClass.Query query = GraknOuterClass.Query.newBuilder().setValue(queryString).build();
        ExecQuery.Builder execQueryRequest = ExecQuery.newBuilder().setQuery(query);
        execQueryRequest.setInfer(infer);
        return TxRequest.newBuilder().setExecQuery(execQueryRequest).build();
    }

    private static TxRequest nextRequest() {
        return TxRequest.newBuilder().setNext(Next.getDefaultInstance()).build();
    }

    private static TxRequest stopRequest() {
        return TxRequest.newBuilder().setStop(Stop.getDefaultInstance()).build();
    }

    private static TxResponse doneResponse() {
        return TxResponse.newBuilder().setDone(Done.getDefaultInstance()).build();
    }

    static class GraknExceptionFake extends GraknException {

        private static final long serialVersionUID = 4308283394793131638L;

        static final String MESSAGE = "OH DEAR";
        static final GraknExceptionFake EXCEPTION = new GraknExceptionFake();

        GraknExceptionFake() {
            super(MESSAGE);
        }
    }
}

