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
import ai.grakn.client.rpc.RequestBuilder;
import ai.grakn.client.rpc.Transceiver;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.Label;
import ai.grakn.concept.Role;
import ai.grakn.engine.ServerRPC;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.task.postprocessing.PostProcessor;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.GraknException;
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
import ai.grakn.rpc.proto.ConceptProto;
import ai.grakn.rpc.proto.ConceptProto.BaseType;
import ai.grakn.rpc.proto.IteratorProto.IteratorId;
import ai.grakn.rpc.proto.KeyspaceGrpc;
import ai.grakn.rpc.proto.SessionGrpc;
import ai.grakn.rpc.proto.SessionProto;
import ai.grakn.rpc.proto.SessionProto.Open;
import ai.grakn.rpc.proto.SessionProto.TxRequest;
import ai.grakn.rpc.proto.SessionProto.TxResponse;
import ai.grakn.rpc.proto.SessionProto.TxType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
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

import static ai.grakn.client.rpc.RequestBuilder.Keyspace.delete;
import static ai.grakn.client.rpc.RequestBuilder.Transaction.commit;
import static ai.grakn.client.rpc.RequestBuilder.Transaction.next;
import static ai.grakn.client.rpc.RequestBuilder.Transaction.open;
import static ai.grakn.client.rpc.RequestBuilder.Transaction.query;
import static ai.grakn.client.rpc.RequestBuilder.Transaction.stop;
import static ai.grakn.engine.rpc.ResponseBuilder.Transaction.done;
import static ai.grakn.rpc.GrpcTestUtil.hasStatus;
import static org.junit.Assert.assertEquals;
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
 * Unit Tests for {@link ai.grakn.engine.ServerRPC}
 */
public class ServerRPCTest {

    private static final String EXCEPTION_MESSAGE = "OH DEAR";
    private static final GraknException EXCEPTION = GraqlQueryException.create(EXCEPTION_MESSAGE);

    private static final int PORT = 5555;
    private static final Keyspace MYKS = Keyspace.of("myks");
    private static final String QUERY = "match $x isa person; get;";
    private static final String V123 = "V123";
    private static final String V456 = "V456";

    private final EngineGraknTxFactory txFactory = mock(EngineGraknTxFactory.class);
    private final EmbeddedGraknTx tx = mock(EmbeddedGraknTx.class);
    private final GetQuery query = mock(GetQuery.class);
    //private final GrpcClient client = mock(GrpcClient.class);
    private final PostProcessor mockedPostProcessor = mock(PostProcessor.class);

    private ServerRPC rpcServerRPC;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    // TODO: usePlainText is not secure
    private final ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", PORT).usePlaintext(true).build();
    private final SessionGrpc.SessionStub stub = SessionGrpc.newStub(channel);
    private final KeyspaceGrpc.KeyspaceBlockingStub keyspaceBlockingStub = KeyspaceGrpc.newBlockingStub(channel);

    @Before
    public void setUp() throws IOException {
        doNothing().when(mockedPostProcessor).submit(any(CommitLog.class));

        OpenRequest requestOpener = new ServerOpenRequest(txFactory);
        io.grpc.Server server = ServerBuilder.forPort(PORT)
                .addService(new SessionService(requestOpener, mockedPostProcessor))
                .addService(new KeyspaceService(requestOpener))
                .build();
        rpcServerRPC = ServerRPC.create(server);
        rpcServerRPC.start();

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
        rpcServerRPC.close();
    }

    @Test
    public void whenOpeningAReadTransactionRemotely_AReadTransactionIsOpened() {
        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.READ));
        }

        verify(txFactory).tx(MYKS, GraknTxType.READ);
    }

    @Test
    public void whenOpeningAWriteTransactionRemotely_AWriteTransactionIsOpened() {
        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
        }

        verify(txFactory).tx(MYKS, GraknTxType.WRITE);
    }

    @Test
    public void whenOpeningABatchTransactionRemotely_ABatchTransactionIsOpened() {
        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.BATCH));
        }

        verify(txFactory).tx(MYKS, GraknTxType.BATCH);
    }

    @Test
    public void whenOpeningATransactionRemotely_ReceiveADoneMessage() throws InterruptedException {
        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.READ));
            TxResponse response = tx.receive().ok();

            assertEquals(ResponseBuilder.Transaction.open(), response);
        }
    }

    @Test
    public void whenCommittingATransactionRemotely_TheTransactionIsCommitted() {
        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.send(commit());
        }

        verify(tx).commitSubmitNoLogs();
    }

    @Test
    public void whenCommittingATransactionRemotely_ReceiveADoneMessage() throws InterruptedException {
        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(commit());
            TxResponse response = tx.receive().ok();

            assertEquals(ResponseBuilder.Transaction.commit(), response);
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
                Transceiver tx1 = Transceiver.create(stub);
                Transceiver tx2 = Transceiver.create(stub)
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
        String keyspace = "not!@akeyspace";
        Open.Req openRequest = Open.Req.newBuilder().setKeyspace(keyspace).setTxType(TxType.Write).build();

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(TxRequest.newBuilder().setOpen(openRequest).build());
            exception.expect(hasStatus(Status.INVALID_ARGUMENT));
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

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
        }

        verify(tx).close();
        assertEquals(threadOpenedWith[0], threadClosedWith[0]);
    }

    @Test
    public void whenExecutingAQueryRemotely_TheQueryIsParsedAndExecuted() {
        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.send(query(QUERY, false));
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

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(query(QUERY, false));
            IteratorId iterator = tx.receive().ok().getQuery().getIteratorId();

            tx.send(next(iterator));
            TxResponse response1 = tx.receive().ok();

            ConceptProto.Concept rpcX =
                    ConceptProto.Concept.newBuilder().setId(V123).setBaseType(BaseType.RELATIONSHIP).build();
            SessionProto.QueryAnswer.Builder answerX = SessionProto.QueryAnswer.newBuilder().putQueryAnswer("x", rpcX);
            SessionProto.Answer.Builder resultX = SessionProto.Answer.newBuilder().setQueryAnswer(answerX);
            assertEquals(TxResponse.newBuilder().setAnswer(resultX).build(), response1);

            tx.send(next(iterator));
            TxResponse response2 = tx.receive().ok();

            ConceptProto.Concept rpcY =
                    ConceptProto.Concept.newBuilder().setId(V456).setBaseType(BaseType.ATTRIBUTE).build();
            SessionProto.QueryAnswer.Builder answerY = SessionProto.QueryAnswer.newBuilder().putQueryAnswer("y", rpcY);
            SessionProto.Answer.Builder resultY = SessionProto.Answer.newBuilder().setQueryAnswer(answerY);
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

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(query(QUERY, false));
            IteratorId iterator = tx.receive().ok().getQuery().getIteratorId();

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

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(query(COUNT_QUERY, false));

            TxResponse expected =
                    TxResponse.newBuilder().setAnswer(SessionProto.Answer.newBuilder().setOtherResult("100")).build();

            assertEquals(expected, tx.receive().ok());
        }
    }

    @Test
    public void whenExecutingAQueryRemotelyWithNoResult_ReturnDone() throws InterruptedException {
        String DELETE_QUERY = "match $x isa person; delete $x";
        DeleteQuery deleteQuery = mock(DeleteQuery.class);
        when(tx.graql().parse(DELETE_QUERY)).thenReturn(deleteQuery);

        when(deleteQuery.execute()).thenReturn(null);

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(query(DELETE_QUERY, false));
            assertEquals(ResponseBuilder.Transaction.query(null), tx.receive().ok());
        }
    }

    @Test @Ignore //TODO this test needs to be removed as it is no longer needed
    public void whenExecutingQueryWithoutInferenceSet_InferenceIsNotSet() throws InterruptedException {
        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.send(query(QUERY, false));
            IteratorId iterator = tx.receive().ok().getQuery().getIteratorId();

            tx.send(next(iterator));
            tx.send(stop(iterator));
        }

        verify(tx.graql(), times(0)).infer(anyBoolean());
    }

    @Test
    public void whenExecutingQueryWithInferenceOff_InferenceIsTurnedOff() throws InterruptedException {
        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.send(query(QUERY, false));
            IteratorId iterator = tx.receive().ok().getQuery().getIteratorId();

            tx.send(next(iterator));
            tx.send(stop(iterator));
        }

        verify(tx.graql()).infer(false);
    }

    @Test @Ignore // TODO: re-enable this test after investigating the possibility of a race condition
    public void whenExecutingQueryWithInferenceOn_InferenceIsTurnedOn() throws InterruptedException {
        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.send(query(QUERY, true));
            IteratorId iterator = tx.receive().ok().getQuery().getIteratorId();

            tx.send(next(iterator));
            tx.send(stop(iterator));
        }

        verify(tx.graql()).infer(true);
    }

    @Test @Ignore
    public void whenGettingALabel_TheLabelIsReturned() throws InterruptedException {
        ConceptId id = ConceptId.of("V123456");
        Label label = Label.of("Dunstan");

        Concept concept = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(tx.getConcept(id)).thenReturn(concept);
        when(concept.isSchemaConcept()).thenReturn(true);
        when(concept.asSchemaConcept().getLabel()).thenReturn(label);

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.READ));
            tx.receive().ok();

            //tx.send(RequestBuilder.runConceptMethod(id, ConceptMethod.getLabel));

            //assertEquals(label, ConceptMethod.getLabel.readResponse(conceptConverter, client, tx.receive().ok()));
        }
    }

    @Test @Ignore
    public void whenGettingIsImplicitProperty_IsImplicitIsReturned() throws InterruptedException {
        ConceptId id = ConceptId.of("V123456");

        Concept concept = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(tx.getConcept(id)).thenReturn(concept);
        when(concept.isSchemaConcept()).thenReturn(true);
        when(concept.asSchemaConcept().isImplicit()).thenReturn(true);

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.READ));
            tx.receive().ok();

            //tx.send(RequestBuilder.runConceptMethod(id, ConceptMethod.isImplicit));

            //assertTrue(ConceptMethod.isImplicit.readResponse(conceptConverter, client, tx.receive().ok()));
        }
    }

    @Test @Ignore
    public void whenGettingIsInferredProperty_IsInferredIsReturned() throws InterruptedException {
        ConceptId id = ConceptId.of("V123456");

        Concept concept = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(tx.getConcept(id)).thenReturn(concept);
        when(concept.isThing()).thenReturn(true);
        when(concept.asThing().isInferred()).thenReturn(false);

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.READ));
            tx.receive().ok();

            //tx.send(RequestBuilder.runConceptMethod(id, ConceptMethod.isInferred));

            //assertFalse(ConceptMethod.isInferred.readResponse(conceptConverter, client, tx.receive().ok()));
        }
    }

    @Test @Ignore
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

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.READ));
            tx.receive().ok();

            //ConceptMethod<Void> conceptMethod = ConceptMethod.removeRolePlayer(RolePlayer.create(role, player));
            //tx.send(RequestBuilder.runConceptMethod(conceptId, conceptMethod));
            //tx.receive().ok();

            verify(concept.asRelationship()).removeRolePlayer(role, player);
        }
    }

    @Test @Ignore
    public void whenGettingALabelForANonExistentConcept_Throw() throws InterruptedException {
        ConceptId id = ConceptId.of("V123456");

        when(tx.getConcept(id)).thenReturn(null);

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.READ));
            tx.receive().ok();

            //tx.send(RequestBuilder.runConceptMethod(id, ConceptMethod.getLabel));

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().error();
        }
    }

    @Test @Ignore
    public void whenGettingALabelForANonSchemaConcept_Throw() throws InterruptedException {
        ConceptId id = ConceptId.of("V123456");

        Concept concept = mock(Concept.class);
        when(tx.getConcept(id)).thenReturn(concept);
        when(concept.isSchemaConcept()).thenReturn(false);
        when(concept.asSchemaConcept()).thenThrow(EXCEPTION);

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.READ));
            tx.receive().ok();

            //tx.send(RequestBuilder.runConceptMethod(id, ConceptMethod.getLabel));

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

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.READ));
            tx.receive().ok();

            tx.send(RequestBuilder.Transaction.getConcept(id));

            ConceptProto.Concept response = tx.receive().ok().getGetConcept().getConcept();

            assertEquals(id.getValue(), response.getId());
            assertEquals(BaseType.RELATIONSHIP, response.getBaseType());
        }
    }

    @Test
    public void whenGettingANonExistentConcept_ReturnNothing() throws InterruptedException {
        ConceptId id = ConceptId.of("V123456");

        when(tx.getConcept(id)).thenReturn(null);

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.READ));
            tx.receive().ok();

            tx.send(RequestBuilder.Transaction.getConcept(id));

            assertTrue(tx.receive().ok().getGetConcept().getNull().isInitialized());
        }
    }

    @Test
    public void whenCommittingBeforeOpeningTx_Throw() throws Throwable {
        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(commit());

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenExecutingAQueryBeforeOpeningTx_Throw() throws Throwable {
        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(query(QUERY, false));

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenOpeningTxTwice_Throw() throws Throwable {
        try (Transceiver tx = Transceiver.create(stub)) {
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

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            exception.expect(hasStatus(Status.INTERNAL));
            throw tx.receive().error();
        }
    }

    @Test
    public void whenCommittingFails_Throw() throws Throwable {
        doThrow(EXCEPTION).when(tx).commitSubmitNoLogs();

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(commit());

            exception.expect(hasStatus(Status.INVALID_ARGUMENT));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenParsingQueryFails_Throw() throws Throwable {
        String message = "you forgot a semicolon";
        GraknException error = GraqlSyntaxException.create(message);

        when(tx.graql().parse(QUERY)).thenThrow(error);

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();
            tx.send(query(QUERY, false));

            exception.expect(hasStatus(Status.INVALID_ARGUMENT));
            throw tx.receive().error();
        }
    }

    @Test
    public void whenExecutingQueryFails_Throw() throws Throwable {
        String message = "your query is dumb";
        GraknException expectedException = GraqlQueryException.create(message);

        when(query.stream()).thenThrow(expectedException);

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();
            tx.send(query(QUERY, false));

            exception.expect(hasStatus(Status.INVALID_ARGUMENT));
            throw tx.receive().error();
        }
    }

    @Test
    public void whenSendingNextBeforeQuery_Throw() throws Throwable {
        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(next(IteratorId.getDefaultInstance()));

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenSendingStopWithNonExistentIterator_IgnoreRequest() throws Throwable {
        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(stop(IteratorId.getDefaultInstance()));
            assertEquals(ResponseBuilder.Transaction.done(), tx.receive().ok());
        }
    }

    @Test
    public void whenSendingNextAfterStop_Throw() throws Throwable {
        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(query(QUERY, false));
            IteratorId iterator = tx.receive().ok().getQuery().getIteratorId();

            tx.send(stop(iterator));
            tx.receive();

            tx.send(next(iterator));

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenSendingAnotherQueryDuringQueryExecution_ReturnResultsForBothQueries() throws Throwable {
        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, GraknTxType.WRITE));
            tx.receive();

            tx.send(query(QUERY, false));
            IteratorId iterator1 = tx.receive().ok().getQuery().getIteratorId();

            tx.send(query(QUERY, false));
            IteratorId iterator2 = tx.receive().ok().getQuery().getIteratorId();

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
        keyspaceBlockingStub.delete(delete(MYKS.getValue()));

        verify(tx).delete();
    }

    @Test
    public void whenSendingDeleteRequestWithInvalidKeyspace_CallDeleteOnEmbeddedTx() {
        String keyspace = "not!@akeyspace";
        exception.expect(hasStatus(Status.INVALID_ARGUMENT));
        keyspaceBlockingStub.delete(RequestBuilder.Keyspace.delete(keyspace));
    }
}

