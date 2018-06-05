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

package ai.grakn.engine.rpc;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.Label;
import ai.grakn.concept.Role;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.task.postprocessing.PostProcessor;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.GraknException;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.query.ComputeQueryImpl;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.kb.log.CommitLog;
import ai.grakn.rpc.util.ConceptMethod;
import ai.grakn.rpc.GrpcClient;
import ai.grakn.rpc.util.TxConceptReader;
import ai.grakn.rpc.GrpcOpenRequestExecutor;
import ai.grakn.rpc.RolePlayer;
import ai.grakn.rpc.TxGrpcCommunicator;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.rpc.generated.GraknGrpc.GraknBlockingStub;
import ai.grakn.rpc.generated.GraknGrpc.GraknStub;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcConcept.BaseType;
import ai.grakn.rpc.generated.GrpcGrakn;
import ai.grakn.rpc.generated.GrpcGrakn.Open;
import ai.grakn.rpc.generated.GrpcGrakn.TxRequest;
import ai.grakn.rpc.generated.GrpcGrakn.TxResponse;
import ai.grakn.rpc.generated.GrpcGrakn.TxType;
import ai.grakn.rpc.generated.GrpcIterator.IteratorId;
import ai.grakn.rpc.util.RequestBuilder;
import ai.grakn.rpc.util.ResponseBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static ai.grakn.rpc.GrpcTestUtil.hasMetadata;
import static ai.grakn.rpc.GrpcTestUtil.hasStatus;
import static ai.grakn.rpc.util.RequestBuilder.commit;
import static ai.grakn.rpc.util.RequestBuilder.delete;
import static ai.grakn.rpc.util.RequestBuilder.execQuery;
import static ai.grakn.rpc.util.RequestBuilder.next;
import static ai.grakn.rpc.util.RequestBuilder.open;
import static ai.grakn.rpc.util.RequestBuilder.stop;
import static ai.grakn.rpc.util.ResponseBuilder.done;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
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
    private static final GrpcConcept.ConceptId V123 =
            GrpcConcept.ConceptId.newBuilder().setValue("V123").build();
    private static final GrpcConcept.ConceptId V456 =
            GrpcConcept.ConceptId.newBuilder().setValue("V456").build();

    private final EngineGraknTxFactory txFactory = mock(EngineGraknTxFactory.class);
    private final EmbeddedGraknTx tx = mock(EmbeddedGraknTx.class);
    private final GetQuery query = mock(GetQuery.class);
    private final TxConceptReader conceptConverter = mock(TxConceptReader.class);
    private final GrpcClient client = mock(GrpcClient.class);
    private final PostProcessor mockedPostProcessor = mock(PostProcessor.class);

    private GrpcServer grpcServer;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    // TODO: usePlainText is not secure
    private final ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", PORT).usePlaintext(true).build();
    private final GraknStub stub = GraknGrpc.newStub(channel);
    private final GraknBlockingStub blockingStub = GraknGrpc.newBlockingStub(channel);

    @Before
    public void setUp() throws IOException {
        doNothing().when(mockedPostProcessor).submit(any(CommitLog.class));

        GrpcOpenRequestExecutor requestExecutor = new GrpcOpenRequestExecutorImpl(txFactory);
        Server server = ServerBuilder.forPort(PORT).addService(new GraknRPCService(requestExecutor, mockedPostProcessor)).build();
        grpcServer = GrpcServer.create(server);
        grpcServer.start();

        QueryBuilder qb = mock(QueryBuilder.class);

        when(tx.admin()).thenReturn(tx);
        when(txFactory.tx(eq(MYKS), any())).thenReturn(tx);
        when(tx.graql()).thenReturn(qb);
        when(qb.parse(QUERY)).thenReturn(query);
        when(qb.infer(anyBoolean())).thenReturn(qb);

        when(query.execute()).thenAnswer(params -> Stream.of(new QueryAnswer()));
    }

    @After
    public void tearDown() throws InterruptedException {
        grpcServer.close();
    }

    @Test
    public void whenOpeningAReadTransactionRemotely_AReadTransactionIsOpened() {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.READ));
        }

        verify(txFactory).tx(MYKS, GraknTxType.READ);
    }

    @Test
    public void whenOpeningAWriteTransactionRemotely_AWriteTransactionIsOpened() {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
        }

        verify(txFactory).tx(MYKS, GraknTxType.WRITE);
    }

    @Test
    public void whenOpeningABatchTransactionRemotely_ABatchTransactionIsOpened() {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.BATCH));
        }

        verify(txFactory).tx(MYKS, GraknTxType.BATCH);
    }

    @Test
    public void whenOpeningATransactionRemotely_ReceiveADoneMessage() throws InterruptedException {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.READ));
            TxResponse response = tx.receive().ok();

            assertEquals(done(), response);
        }
    }

    @Test
    public void whenCommittingATransactionRemotely_TheTransactionIsCommitted() {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.send(commit());
        }

        verify(tx).commitSubmitNoLogs();
    }

    @Test
    public void whenCommittingATransactionRemotely_ReceiveADoneMessage() throws InterruptedException {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(commit());
            TxResponse response = tx.receive().ok();

            assertEquals(done(), response);
        }
    }

    @Test
    public void whenOpeningTwoTransactions_TransactionsAreOpenedInDifferentThreads() throws InterruptedException {
        List<Thread> threads = new ArrayList<>();

        when(txFactory.tx(MYKS, GraknTxType.WRITE)).thenAnswer(invocation -> {
            threads.add(Thread.currentThread());
            return tx;
        });

        try (
                TxGrpcCommunicator tx1 = TxGrpcCommunicator.create(stub);
                TxGrpcCommunicator tx2 = TxGrpcCommunicator.create(stub)
        ) {
            tx1.send(open(MYKS, GraknTxType.WRITE));
            tx2.send(open(MYKS, GraknTxType.WRITE));

            tx1.receive();
            tx2.receive();
        }

        assertNotEquals(threads.get(0), threads.get(1));
    }

    @Test
    public void whenOpeningATransactionRemotelyWithAnInvalidKeyspace_Throw() throws Throwable {
        GrpcGrakn.Keyspace keyspace = GrpcGrakn.Keyspace.newBuilder().setValue("not!@akeyspace").build();
        Open open = Open.newBuilder().setKeyspace(keyspace).setTxType(TxType.Write).build();

        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(TxRequest.newBuilder().setOpen(open).build());

            exception.expect(hasStatus(Status.UNKNOWN.withDescription(GraknTxOperationException.invalidKeyspace("not!@akeyspace").getMessage())));
            exception.expect(hasMetadata(ResponseBuilder.ErrorType.KEY, ResponseBuilder.ErrorType.GRAKN_TX_OPERATION_EXCEPTION));

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
            tx.send(open(MYKS, GraknTxType.WRITE));
        }

        verify(tx).close();
        assertEquals(threadOpenedWith[0], threadClosedWith[0]);
    }

    @Test
    public void whenExecutingAQueryRemotely_TheQueryIsParsedAndExecuted() {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.send(execQuery(QUERY, null));
        }

        GetQuery query = tx.graql().parse(QUERY);
        verify(query).stream();
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

        when(query.stream()).thenAnswer(params -> answers.stream());

        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(execQuery(QUERY, null));
            IteratorId iterator = tx.receive().ok().getIteratorId();

            tx.send(next(iterator));
            TxResponse response1 = tx.receive().ok();

            GrpcConcept.Concept rpcX =
                    GrpcConcept.Concept.newBuilder().setId(V123).setBaseType(BaseType.Relationship).build();
            GrpcGrakn.QueryAnswer.Builder answerX = GrpcGrakn.QueryAnswer.newBuilder().putQueryAnswer("x", rpcX);
            GrpcGrakn.Answer.Builder resultX = GrpcGrakn.Answer.newBuilder().setQueryAnswer(answerX);
            assertEquals(TxResponse.newBuilder().setAnswer(resultX).build(), response1);

            tx.send(next(iterator));
            TxResponse response2 = tx.receive().ok();

            GrpcConcept.Concept rpcY =
                    GrpcConcept.Concept.newBuilder().setId(V456).setBaseType(BaseType.Attribute).build();
            GrpcGrakn.QueryAnswer.Builder answerY = GrpcGrakn.QueryAnswer.newBuilder().putQueryAnswer("y", rpcY);
            GrpcGrakn.Answer.Builder resultY = GrpcGrakn.Answer.newBuilder().setQueryAnswer(answerY);
            assertEquals(TxResponse.newBuilder().setAnswer(resultY).build(), response2);

            tx.send(next(iterator));
            TxResponse response3 = tx.receive().ok();

            TxResponse expected = done();
            assertEquals(expected, response3);

            tx.send(stop(iterator));
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

        // Produce an endless stream of results - this means if the behaviour is not lazy this will never terminate
        when(query.stream()).thenAnswer(params -> Stream.generate(answers::stream).flatMap(Function.identity()));

        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(execQuery(QUERY, null));
            IteratorId iterator = tx.receive().ok().getIteratorId();

            tx.send(next(iterator));
            tx.receive().ok();

            tx.send(next(iterator));
            tx.receive().ok();

            tx.send(stop(iterator));

            TxResponse response = tx.receive().ok();

            assertEquals(done(), response);
        }
    }

    @Ignore
    @Test
    public void whenExecutingAQueryRemotelyThatReturnsOneResult_ReturnOneResult() throws InterruptedException {
        String COUNT_QUERY = "compute count;";
        ComputeQuery countQuery = mock(ComputeQuery.class);
        when(tx.graql().parse(COUNT_QUERY)).thenReturn(countQuery);

        when(countQuery.execute()).thenReturn(new ComputeQueryImpl.AnswerImpl().setNumber(100L));

        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(execQuery(COUNT_QUERY, null));

            TxResponse expected =
                    TxResponse.newBuilder().setAnswer(GrpcGrakn.Answer.newBuilder().setOtherResult("100")).build();

            assertEquals(expected, tx.receive().ok());
        }
    }

    @Test
    public void whenExecutingAQueryRemotelyWithNoResult_ReturnDone() throws InterruptedException {
        String DELETE_QUERY = "match $x isa person; delete $x";
        DeleteQuery deleteQuery = mock(DeleteQuery.class);
        when(tx.graql().parse(DELETE_QUERY)).thenReturn(deleteQuery);

        when(deleteQuery.execute()).thenReturn(null);

        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(execQuery(DELETE_QUERY, null));
            assertEquals(ResponseBuilder.done(), tx.receive().ok());
        }
    }

    @Test
    public void whenExecutingQueryWithoutInferenceSet_InferenceIsNotSet() throws InterruptedException {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.send(execQuery(QUERY, null));
            IteratorId iterator = tx.receive().ok().getIteratorId();

            tx.send(next(iterator));
            tx.send(stop(iterator));
        }

        verify(tx.graql(), times(0)).infer(anyBoolean());
    }

    @Test
    public void whenExecutingQueryWithInferenceOff_InferenceIsTurnedOff() throws InterruptedException {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.send(execQuery(QUERY, false));
            IteratorId iterator = tx.receive().ok().getIteratorId();

            tx.send(next(iterator));
            tx.send(stop(iterator));
        }

        verify(tx.graql()).infer(false);
    }

    @Test
    public void whenExecutingQueryWithInferenceOn_InferenceIsTurnedOn() throws InterruptedException {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.send(execQuery(QUERY, true));
            IteratorId iterator = tx.receive().ok().getIteratorId();

            tx.send(next(iterator));
            tx.send(stop(iterator));
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
            tx.send(open(MYKS, GraknTxType.READ));
            tx.receive().ok();

            tx.send(RequestBuilder.runConceptMethod(id, ConceptMethod.GET_LABEL));

            assertEquals(label, ConceptMethod.GET_LABEL.readResponse(conceptConverter, client, tx.receive().ok()));
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
            tx.send(open(MYKS, GraknTxType.READ));
            tx.receive().ok();

            tx.send(RequestBuilder.runConceptMethod(id, ConceptMethod.IS_IMPLICIT));

            assertTrue(ConceptMethod.IS_IMPLICIT.readResponse(conceptConverter, client, tx.receive().ok()));
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
            tx.send(open(MYKS, GraknTxType.READ));
            tx.receive().ok();

            tx.send(RequestBuilder.runConceptMethod(id, ConceptMethod.IS_INFERRED));

            assertFalse(ConceptMethod.IS_INFERRED.readResponse(conceptConverter, client, tx.receive().ok()));
        }
    }

    @Test
    public void whenRemovingRolePlayer_RolePlayerIsRemoved() throws InterruptedException {
        ConceptId conceptId = ConceptId.of("V123456");
        ConceptId roleId = ConceptId.of("ROLE");
        ConceptId playerId = ConceptId.of("PLAYER");

        Concept concept = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(tx.getConcept(conceptId)).thenReturn(concept);
        when(concept.isRelationship()).thenReturn(true);

        Role role = mock(Role.class, RETURNS_DEEP_STUBS);
        when(tx.getConcept(roleId)).thenReturn(role);
        when(role.isRole()).thenReturn(true);
        when(role.asRole()).thenReturn(role);
        when(role.getId()).thenReturn(roleId);

        Entity player = mock(Entity.class, RETURNS_DEEP_STUBS);
        when(tx.getConcept(playerId)).thenReturn(player);
        when(player.isEntity()).thenReturn(true);
        when(player.asEntity()).thenReturn(player);
        when(player.isThing()).thenReturn(true);
        when(player.asThing()).thenReturn(player);
        when(player.getId()).thenReturn(playerId);

        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.READ));
            tx.receive().ok();

            ConceptMethod<Void> conceptMethod = ConceptMethod.removeRolePlayer(RolePlayer.create(role, player));
            tx.send(RequestBuilder.runConceptMethod(conceptId, conceptMethod));
            tx.receive().ok();

            verify(concept.asRelationship()).removeRolePlayer(role, player);
        }
    }

    @Test
    public void whenGettingALabelForANonExistentConcept_Throw() throws InterruptedException {
        ConceptId id = ConceptId.of("V123456");

        when(tx.getConcept(id)).thenReturn(null);

        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.READ));
            tx.receive().ok();

            tx.send(RequestBuilder.runConceptMethod(id, ConceptMethod.GET_LABEL));

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
            tx.send(open(MYKS, GraknTxType.READ));
            tx.receive().ok();

            tx.send(RequestBuilder.runConceptMethod(id, ConceptMethod.GET_LABEL));

            exception.expect(hasStatus(Status.UNKNOWN.withDescription(EXCEPTION_MESSAGE)));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenGettingAConcept_ReturnTheConcept() throws InterruptedException {
        ConceptId id = ConceptId.of("V123456");

        Concept concept = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(concept.getId()).thenReturn(id);
        when(concept.isRelationship()).thenReturn(true);

        when(tx.getConcept(id)).thenReturn(concept);

        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.READ));
            tx.receive().ok();

            tx.send(RequestBuilder.getConcept(id));

            GrpcConcept.OptionalConcept response = tx.receive().ok().getOptionalConcept();

            assertEquals(id.getValue(), response.getPresent().getId().getValue());
            assertEquals(BaseType.Relationship, response.getPresent().getBaseType());
        }
    }

    @Test
    public void whenGettingANonExistentConcept_ReturnNothing() throws InterruptedException {
        ConceptId id = ConceptId.of("V123456");

        when(tx.getConcept(id)).thenReturn(null);

        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.READ));
            tx.receive().ok();

            tx.send(RequestBuilder.getConcept(id));

            GrpcConcept.OptionalConcept response = tx.receive().ok().getOptionalConcept();

            assertEquals(GrpcConcept.OptionalConcept.ValueCase.ABSENT, response.getValueCase());
        }
    }

    @Test
    public void whenCommittingBeforeOpeningTx_Throw() throws Throwable {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(commit());

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenExecutingAQueryBeforeOpeningTx_Throw() throws Throwable {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(execQuery(QUERY, null));

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenOpeningTxTwice_Throw() throws Throwable {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(open(MYKS, GraknTxType.WRITE));

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
            tx.send(open(MYKS, GraknTxType.WRITE));

            exception.expect(hasStatus(Status.UNKNOWN.withDescription(message)));
            exception.expect(hasMetadata(ResponseBuilder.ErrorType.KEY, ResponseBuilder.ErrorType.GRAKN_BACKEND_EXCEPTION));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenCommittingFails_Throw() throws Throwable {
        doThrow(EXCEPTION).when(tx).commitSubmitNoLogs();

        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(commit());

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
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(execQuery(QUERY, null));

            exception.expect(hasStatus(Status.UNKNOWN.withDescription(message)));
            exception.expect(hasMetadata(ResponseBuilder.ErrorType.KEY, ResponseBuilder.ErrorType.GRAQL_SYNTAX_EXCEPTION));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenExecutingQueryFails_Throw() throws Throwable {
        String message = "your query is dumb";
        GraknException error = GraqlQueryException.create(message);

        when(query.stream()).thenThrow(error);

        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(execQuery(QUERY, null));

            exception.expect(hasStatus(Status.UNKNOWN.withDescription(message)));
            exception.expect(hasMetadata(ResponseBuilder.ErrorType.KEY, ResponseBuilder.ErrorType.GRAQL_QUERY_EXCEPTION));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenSendingNextBeforeQuery_Throw() throws Throwable {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(next(IteratorId.getDefaultInstance()));

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenSendingStopWithNonExistentIterator_IgnoreRequest() throws Throwable {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(stop(IteratorId.getDefaultInstance()));
            assertEquals(ResponseBuilder.done(), tx.receive().ok());
        }
    }

    @Test
    public void whenSendingNextAfterStop_Throw() throws Throwable {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(execQuery(QUERY, null));
            IteratorId iterator = tx.receive().ok().getIteratorId();

            tx.send(stop(iterator));
            tx.receive();

            tx.send(next(iterator));

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenSendingAnotherQueryDuringQueryExecution_ReturnResultsForBothQueries() throws Throwable {
        try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(execQuery(QUERY, null));
            IteratorId iterator1 = tx.receive().ok().getIteratorId();

            tx.send(execQuery(QUERY, null));
            IteratorId iterator2 = tx.receive().ok().getIteratorId();

            tx.send(next(iterator1));
            tx.receive().ok();

            tx.send(next(iterator2));
            tx.receive().ok();

            tx.send(stop(iterator1));
            tx.receive().ok();

            tx.send(stop(iterator2));
            tx.receive().ok();
        }
    }

    @Test
    public void whenSendingDeleteRequest_CallDeleteOnEmbeddedTx() {
        GrpcGrakn.Keyspace keyspaceRPC = GrpcGrakn.Keyspace.newBuilder().setValue(MYKS.getValue()).build();
        Open open = Open.newBuilder().setKeyspace(keyspaceRPC).setTxType(TxType.Write).build();

        blockingStub.delete(delete(open));

        verify(tx).delete();
    }

    @Test
    public void whenSendingDeleteRequestWithInvalidKeyspace_CallDeleteOnEmbeddedTx() {
        GrpcGrakn.Keyspace keyspace = GrpcGrakn.Keyspace.newBuilder().setValue("not!@akeyspace").build();

        Open open = Open.newBuilder().setKeyspace(keyspace).setTxType(TxType.Write).build();

        String message = GraknTxOperationException.invalidKeyspace("not!@akeyspace").getMessage();
        exception.expect(hasStatus(Status.UNKNOWN.withDescription(message)));
        exception.expect(hasMetadata(ResponseBuilder.ErrorType.KEY, ResponseBuilder.ErrorType.GRAKN_TX_OPERATION_EXCEPTION));

        blockingStub.delete(delete(open));
    }
}

