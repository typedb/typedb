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

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.GraknException;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.grpc.ConceptProperty;
import ai.grakn.grpc.GrpcConceptConverter;
import ai.grakn.grpc.GrpcOpenRequestExecutor;
import ai.grakn.grpc.GrpcUtil;
import ai.grakn.grpc.GrpcUtil.ErrorType;
import ai.grakn.grpc.TxGrpcCommunicator;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.rpc.generated.GraknGrpc.GraknStub;
import ai.grakn.rpc.generated.GraknOuterClass;
import ai.grakn.rpc.generated.GraknOuterClass.BaseType;
import ai.grakn.rpc.generated.GraknOuterClass.IteratorId;
import ai.grakn.rpc.generated.GraknOuterClass.Open;
import ai.grakn.rpc.generated.GraknOuterClass.QueryResult;
import ai.grakn.rpc.generated.GraknOuterClass.TxRequest;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;
import ai.grakn.rpc.generated.GraknOuterClass.TxType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
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

import static ai.grakn.grpc.GrpcTestUtil.hasMetadata;
import static ai.grakn.grpc.GrpcTestUtil.hasStatus;
import static ai.grakn.grpc.GrpcUtil.commitRequest;
import static ai.grakn.grpc.GrpcUtil.doneResponse;
import static ai.grakn.grpc.GrpcUtil.execQueryRequest;
import static ai.grakn.grpc.GrpcUtil.nextRequest;
import static ai.grakn.grpc.GrpcUtil.openRequest;
import static ai.grakn.grpc.GrpcUtil.stopRequest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
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

    private static final String EXCEPTION_MESSAGE = "OH DEAR";
    private static final GraknException EXCEPTION = GraqlQueryException.create(EXCEPTION_MESSAGE);

    private static final int PORT = 5555;
    private static final Keyspace MYKS = Keyspace.of("myks");
    private static final String QUERY = "match $x isa person; get;";
    private static final GraknOuterClass.ConceptId V123 =
            GraknOuterClass.ConceptId.newBuilder().setValue("V123").build();
    private static final GraknOuterClass.ConceptId V456 =
            GraknOuterClass.ConceptId.newBuilder().setValue("V456").build();

    private final EngineGraknTxFactory txFactory = mock(EngineGraknTxFactory.class);
    private final EmbeddedGraknTx tx = mock(EmbeddedGraknTx.class);
    private final GetQuery query = mock(GetQuery.class);
    private final GrpcConceptConverter conceptConverter = mock(GrpcConceptConverter.class);

    private GrpcServer grpcServer;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    // TODO: usePlainText is not secure
    private final ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", PORT).usePlaintext(true).build();
    private final GraknStub stub = GraknGrpc.newStub(channel);

    @Before
    public void setUp() throws IOException {
        GrpcOpenRequestExecutor requestExecutor = new GrpcOpenRequestExecutorImpl(txFactory);
        Server server = ServerBuilder.forPort(PORT).addService(new GrpcGraknService(requestExecutor)).build();
        grpcServer = GrpcServer.create(server);
        grpcServer.start();

        QueryBuilder qb = mock(QueryBuilder.class);

        when(txFactory.tx(eq(MYKS), any())).thenReturn(tx);
        when(tx.graql()).thenReturn(qb);
        when(qb.parse(QUERY)).thenReturn(query);

        when(query.results(any())).thenAnswer(params -> Stream.of(QueryResult.getDefaultInstance()));
    }

    @After
    public void tearDown() throws InterruptedException {
        grpcServer.close();
    }

    @Test
    public void whenOpeningAReadTransactionRemotely_AReadTransactionIsOpened() {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.READ));
        }

        verify(txFactory).tx(MYKS, GraknTxType.READ);
    }

    @Test
    public void whenOpeningAWriteTransactionRemotely_AWriteTransactionIsOpened() {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.WRITE));
        }

        verify(txFactory).tx(MYKS, GraknTxType.WRITE);
    }

    @Test
    public void whenOpeningABatchTransactionRemotely_ABatchTransactionIsOpened() {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.BATCH));
        }

        verify(txFactory).tx(MYKS, GraknTxType.BATCH);
    }

    @Test
    public void whenOpeningATransactionRemotely_ReceiveADoneMessage() throws InterruptedException {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.READ));
            TxResponse response = tx.receive().ok();

            assertEquals(doneResponse(), response);
        }
    }

    @Test
    public void whenCommittingATransactionRemotely_TheTransactionIsCommitted() {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.WRITE));
            tx.send(commitRequest());
        }

        verify(tx).commit();
    }

    @Test
    public void whenCommittingATransactionRemotely_ReceiveADoneMessage() throws InterruptedException {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(commitRequest());
            TxResponse response = tx.receive().ok();

            assertEquals(doneResponse(), response);
        }
    }

    @Test
    public void whenOpeningTwoTransactions_TransactionsAreOpenedInDifferentThreads() {
        List<Thread> threads = new ArrayList<>();

        when(txFactory.tx(MYKS, GraknTxType.WRITE)).thenAnswer(invocation -> {
            threads.add(Thread.currentThread());
            return tx;
        });

        try (
                TxGrpcCommunicator tx1 = TxGrpcCommunicator.create(stub);
                TxGrpcCommunicator tx2 = TxGrpcCommunicator.create(stub)
        ) {
            tx1.send(openRequest(MYKS, GraknTxType.WRITE));
            tx2.send(openRequest(MYKS, GraknTxType.WRITE));
        }

        assertNotEquals(threads.get(0), threads.get(1));
    }

    @Test
    public void whenOpeningATransactionRemotelyWithAnInvalidKeyspace_Throw() throws Throwable {
        GraknOuterClass.Keyspace keyspace = GraknOuterClass.Keyspace.newBuilder().setValue("not!@akeyspace").build();
        Open open = Open.newBuilder().setKeyspace(keyspace).setTxType(TxType.Write).build();

        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(TxRequest.newBuilder().setOpen(open).build());

            exception.expect(hasStatus(Status.UNKNOWN.withDescription(GraknTxOperationException.invalidKeyspace("not!@akeyspace").getMessage())));
            exception.expect(hasMetadata(ErrorType.KEY, ErrorType.GRAKN_TX_OPERATION_EXCEPTION));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenClosingATransactionRemotely_TheTransactionIsClosedWithinTheThreadItWasCreated() {
        final Thread[] threadOpenedWith = new Thread[1];
        final Thread[] threadClosedWith = new Thread[1];

        when(txFactory.tx(MYKS, GraknTxType.WRITE)).thenAnswer(invocation -> {
            threadOpenedWith[0] = Thread.currentThread();
            return tx;
        });

        doAnswer(invocation -> {
            threadClosedWith[0] = Thread.currentThread();
            return null;
        }).when(tx).close();

        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.WRITE));
        }

        verify(tx).close();
        assertEquals(threadOpenedWith[0], threadClosedWith[0]);
    }

    @Test
    public void whenExecutingAQueryRemotely_TheQueryIsParsedAndExecuted() {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.WRITE));
            tx.send(execQueryRequest(QUERY));
        }

        ai.grakn.graql.Query<?> query = tx.graql().parse(QUERY);
        verify(query).results(any());
    }

    @Test
    public void whenExecutingAQueryRemotely_AResultIsReturned() throws InterruptedException {
        Concept conceptX = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(conceptX.getId()).thenReturn(ConceptId.of("V123"));
        when(conceptX.isRelationship()).thenReturn(true);
        when(conceptX.asRelationship().type().getLabel()).thenReturn(Label.of("L123"));

        Concept conceptY = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(conceptY.getId()).thenReturn(ConceptId.of("V456"));
        when(conceptY.isAttribute()).thenReturn(true);
        when(conceptY.asAttribute().type().getLabel()).thenReturn(Label.of("L456"));

        ImmutableList<Answer> answers = ImmutableList.of(
                new QueryAnswer(ImmutableMap.of(Graql.var("x"), conceptX)),
                new QueryAnswer(ImmutableMap.of(Graql.var("y"), conceptY))
        );

        // TODO: reduce wtf
        when(query.results(any())).thenAnswer(params -> query.stream().map(params.<GrpcConverter>getArgument(0)::convert));
        when(query.stream()).thenAnswer(params -> answers.stream());

        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(execQueryRequest(QUERY));
            IteratorId iterator = tx.receive().ok().getIteratorId();

            tx.send(nextRequest(iterator));
            TxResponse response1 = tx.receive().ok();

            GraknOuterClass.Concept rpcX =
                    GraknOuterClass.Concept.newBuilder().setId(V123).setBaseType(BaseType.Relationship).build();
            GraknOuterClass.Answer.Builder answerX = GraknOuterClass.Answer.newBuilder().putAnswer("x", rpcX);
            QueryResult.Builder resultX = QueryResult.newBuilder().setAnswer(answerX);
            assertEquals(TxResponse.newBuilder().setQueryResult(resultX).build(), response1);

            tx.send(nextRequest(iterator));
            TxResponse response2 = tx.receive().ok();

            GraknOuterClass.Concept rpcY =
                    GraknOuterClass.Concept.newBuilder().setId(V456).setBaseType(BaseType.Attribute).build();
            GraknOuterClass.Answer.Builder answerY = GraknOuterClass.Answer.newBuilder().putAnswer("y", rpcY);
            QueryResult.Builder resultY = QueryResult.newBuilder().setAnswer(answerY);
            assertEquals(TxResponse.newBuilder().setQueryResult(resultY).build(), response2);

            tx.send(nextRequest(iterator));
            TxResponse response3 = tx.receive().ok();

            TxResponse expected = doneResponse();
            assertEquals(expected, response3);

            tx.send(stopRequest(iterator));
        }
    }

    @Test(timeout = 1000) // This tests uses an endless stream, so a failure may cause it to never terminate
    public void whenExecutingAQueryRemotelyAndAskingForOneResult_OnlyOneResultIsReturned() throws InterruptedException {
        Concept conceptX = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(conceptX.getId()).thenReturn(ConceptId.of("V123"));
        when(conceptX.isEntity()).thenReturn(true);
        when(conceptX.asEntity().type().getLabel()).thenReturn(Label.of("L123"));

        Concept conceptY = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(conceptY.getId()).thenReturn(ConceptId.of("V456"));
        when(conceptY.isEntity()).thenReturn(true);
        when(conceptY.asEntity().type().getLabel()).thenReturn(Label.of("L456"));

        ImmutableList<Answer> answers = ImmutableList.of(
                new QueryAnswer(ImmutableMap.of(Graql.var("x"), conceptX)),
                new QueryAnswer(ImmutableMap.of(Graql.var("y"), conceptY))
        );

        // TODO: reduce wtf
        when(query.results(any())).thenAnswer(params -> query.stream().map(params.<GrpcConverter>getArgument(0)::convert));

        // Produce an endless stream of results - this means if the behaviour is not lazy this will never terminate
        when(query.stream()).thenAnswer(params -> Stream.generate(answers::stream).flatMap(Function.identity()));

        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(execQueryRequest(QUERY));
            IteratorId iterator = tx.receive().ok().getIteratorId();

            tx.send(nextRequest(iterator));
            tx.receive().ok();

            tx.send(nextRequest(iterator));
            tx.receive().ok();

            tx.send(stopRequest(iterator));

            TxResponse response = tx.receive().ok();

            assertEquals(doneResponse(), response);
        }
    }

    @Test
    public void whenExecutingQueryWithoutInferenceSet_InferenceIsNotSet() throws InterruptedException {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.WRITE));
            tx.send(execQueryRequest(QUERY));
            IteratorId iterator = tx.receive().ok().getIteratorId();

            tx.send(nextRequest(iterator));
            tx.send(stopRequest(iterator));
        }

        verify(tx.graql(), times(0)).infer(anyBoolean());
    }

    @Test
    public void whenExecutingQueryWithInferenceOff_InferenceIsTurnedOff() throws InterruptedException {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.WRITE));
            tx.send(execQueryRequest(QUERY, false));
            IteratorId iterator = tx.receive().ok().getIteratorId();

            tx.send(nextRequest(iterator));
            tx.send(stopRequest(iterator));
        }

        verify(tx.graql()).infer(false);
    }

    @Test
    public void whenExecutingQueryWithInferenceOn_InferenceIsTurnedOn() throws InterruptedException {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.WRITE));
            tx.send(execQueryRequest(QUERY, true));
            IteratorId iterator = tx.receive().ok().getIteratorId();

            tx.send(nextRequest(iterator));
            tx.send(stopRequest(iterator));
        }

        verify(tx.graql()).infer(true);
    }

    @Test
    public void whenGettingALabel_TheLabelIsReturned() throws InterruptedException {
        ConceptId id = ConceptId.of("V123456");
        Label label = Label.of("Dunstan");

        Concept concept = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(tx.getConcept(id)).thenReturn(concept);
        when(concept.isSchemaConcept()).thenReturn(true);
        when(concept.asSchemaConcept().getLabel()).thenReturn(label);

        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.READ));
            tx.receive().ok();

            tx.send(GrpcUtil.getConceptPropertyRequest(id, ConceptProperty.LABEL));

            assertEquals(label, ConceptProperty.LABEL.get(conceptConverter, tx.receive().ok()));
        }
    }

    @Test
    public void whenGettingIsImplicitProperty_IsImplicitIsReturned() throws InterruptedException {
        ConceptId id = ConceptId.of("V123456");

        Concept concept = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(tx.getConcept(id)).thenReturn(concept);
        when(concept.isSchemaConcept()).thenReturn(true);
        when(concept.asSchemaConcept().isImplicit()).thenReturn(true);

        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.READ));
            tx.receive().ok();

            tx.send(GrpcUtil.getConceptPropertyRequest(id, ConceptProperty.IS_IMPLICIT));

            assertTrue(ConceptProperty.IS_IMPLICIT.get(conceptConverter, tx.receive().ok()));
        }
    }

    @Test
    public void whenGettingIsInferredProperty_IsInferredIsReturned() throws InterruptedException {
        ConceptId id = ConceptId.of("V123456");

        Concept concept = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(tx.getConcept(id)).thenReturn(concept);
        when(concept.isThing()).thenReturn(true);
        when(concept.asThing().isInferred()).thenReturn(false);

        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.READ));
            tx.receive().ok();

            tx.send(GrpcUtil.getConceptPropertyRequest(id, ConceptProperty.IS_INFERRED));

            assertFalse(ConceptProperty.IS_INFERRED.get(conceptConverter, tx.receive().ok()));
        }
    }

    @Test
    public void whenGettingALabelForANonExistentConcept_Throw() throws InterruptedException {
        ConceptId id = ConceptId.of("V123456");

        when(tx.getConcept(id)).thenReturn(null);

        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.READ));
            tx.receive().ok();

            tx.send(GrpcUtil.getConceptPropertyRequest(id, ConceptProperty.LABEL));

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenGettingALabelForANonSchemaConcept_Throw() throws InterruptedException {
        ConceptId id = ConceptId.of("V123456");

        Concept concept = mock(Concept.class);
        when(tx.getConcept(id)).thenReturn(concept);
        when(concept.isSchemaConcept()).thenReturn(false);
        when(concept.asSchemaConcept()).thenThrow(EXCEPTION);

        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.READ));
            tx.receive().ok();

            tx.send(GrpcUtil.getConceptPropertyRequest(id, ConceptProperty.LABEL));

            exception.expect(hasStatus(Status.UNKNOWN.withDescription(EXCEPTION_MESSAGE)));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenCommittingBeforeOpeningTx_Throw() throws Throwable {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(commitRequest());

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenExecutingAQueryBeforeOpeningTx_Throw() throws Throwable {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(execQueryRequest(QUERY));

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenOpeningTxTwice_Throw() throws Throwable {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(openRequest(MYKS, GraknTxType.WRITE));

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenOpeningTxFails_Throw() throws Throwable {
        String message = "the backend went wrong";
        GraknException error = GraknBackendException.create(message);

        when(txFactory.tx(MYKS, GraknTxType.WRITE)).thenThrow(error);

        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.WRITE));

            exception.expect(hasStatus(Status.UNKNOWN.withDescription(message)));
            exception.expect(hasMetadata(ErrorType.KEY, ErrorType.GRAKN_BACKEND_EXCEPTION));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenCommittingFails_Throw() throws Throwable {
        doThrow(EXCEPTION).when(tx).commit();

        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(commitRequest());

            exception.expect(hasStatus(Status.UNKNOWN.withDescription(EXCEPTION_MESSAGE)));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenParsingQueryFails_Throw() throws Throwable {
        String message = "you forgot a semicolon";
        GraknException error = GraqlSyntaxException.create(message);

        when(tx.graql().parse(QUERY)).thenThrow(error);

        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(execQueryRequest(QUERY));

            exception.expect(hasStatus(Status.UNKNOWN.withDescription(message)));
            exception.expect(hasMetadata(ErrorType.KEY, ErrorType.GRAQL_SYNTAX_EXCEPTION));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenExecutingQueryFails_Throw() throws Throwable {
        String message = "your query is dumb";
        GraknException error = GraqlQueryException.create(message);

        when(query.results(any())).thenThrow(error);

        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(execQueryRequest(QUERY));

            exception.expect(hasStatus(Status.UNKNOWN.withDescription(message)));
            exception.expect(hasMetadata(ErrorType.KEY, ErrorType.GRAQL_QUERY_EXCEPTION));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenSendingNextBeforeQuery_Throw() throws Throwable {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(nextRequest(IteratorId.getDefaultInstance()));

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenSendingStopBeforeQuery_Throw() throws Throwable {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(stopRequest(IteratorId.getDefaultInstance()));

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenSendingNextAfterStop_Throw() throws Throwable {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(execQueryRequest(QUERY));
            IteratorId iterator = tx.receive().ok().getIteratorId();

            tx.send(stopRequest(iterator));
            tx.receive();

            tx.send(nextRequest(iterator));

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenSendingAnotherQueryDuringQueryExecution_ReturnResultsForBothQueries() throws Throwable {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(openRequest(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(execQueryRequest(QUERY));
            IteratorId iterator1 = tx.receive().ok().getIteratorId();

            tx.send(execQueryRequest(QUERY));
            IteratorId iterator2 = tx.receive().ok().getIteratorId();

            tx.send(nextRequest(iterator1));
            tx.receive().ok();

            tx.send(nextRequest(iterator2));
            tx.receive().ok();

            tx.send(stopRequest(iterator1));
            tx.receive().ok();

            tx.send(stopRequest(iterator2));
            tx.receive().ok();
        }
    }
}

