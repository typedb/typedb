/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.server.rpc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import grakn.core.client.rpc.RequestBuilder;
import grakn.core.client.rpc.Transceiver;
import grakn.core.common.exception.GraknException;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.answer.Value;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Entity;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.Role;
import grakn.core.graql.exception.GraqlSyntaxException;
import grakn.core.graql.query.ComputeQuery;
import grakn.core.graql.query.DeleteQuery;
import grakn.core.graql.query.GetQuery;
import grakn.core.graql.query.QueryBuilder;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.protocol.AnswerProto;
import grakn.core.protocol.ConceptProto;
import grakn.core.protocol.KeyspaceProto;
import grakn.core.protocol.KeyspaceServiceGrpc;
import grakn.core.protocol.SessionProto.Transaction;
import grakn.core.protocol.SessionProto.Transaction.Open;
import grakn.core.protocol.SessionServiceGrpc;
import grakn.core.server.QueryExecutor;
import grakn.core.server.deduplicator.AttributeDeduplicatorDaemon;
import grakn.core.server.exception.GraknServerException;
import grakn.core.server.exception.InvalidKBException;
import grakn.core.server.keyspace.Keyspace;
import grakn.core.server.keyspace.KeyspaceManager;
import grakn.core.server.session.SessionStore;
import grakn.core.server.session.TransactionImpl;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static grakn.core.client.rpc.RequestBuilder.Transaction.commit;
import static grakn.core.client.rpc.RequestBuilder.Transaction.iterate;
import static grakn.core.client.rpc.RequestBuilder.Transaction.open;
import static grakn.core.client.rpc.RequestBuilder.Transaction.query;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit Tests for {@link grakn.core.server.rpc}
 */
@SuppressWarnings("Duplicates")
public class ServerRPCTest {

    private static final String EXCEPTION_MESSAGE = "OH DEAR";
    private static final GraknException EXCEPTION = InvalidKBException.create(EXCEPTION_MESSAGE);

    private static final int PORT = 5555;
    private static final Keyspace MYKS = Keyspace.of("myks");
    private static final String QUERY = "match $x isa person; get;";
    private static final String V123 = "V123";
    private static final String V456 = "V456";

    private final SessionStore txFactory = mock(SessionStore.class);
    private final TransactionImpl tx = mock(TransactionImpl.class);
    private final GetQuery query = mock(GetQuery.class);
    private final QueryExecutor executor = mock(QueryExecutor.class);
    private final grakn.core.server.deduplicator.AttributeDeduplicatorDaemon mockedAttributeDeduplicatorDaemon = mock(AttributeDeduplicatorDaemon.class);
    private final KeyspaceManager mockedKeyspaceStore = mock(KeyspaceManager.class);

    private io.grpc.Server serverRPC;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private final ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", PORT).usePlaintext(true).build();
    private final SessionServiceGrpc.SessionServiceStub stub = SessionServiceGrpc.newStub(channel);
    private final KeyspaceServiceGrpc.KeyspaceServiceBlockingStub keyspaceBlockingStub = KeyspaceServiceGrpc.newBlockingStub(channel);

    @Before
    public void setUp() throws IOException {
        OpenRequest requestOpener = new ServerOpenRequest(txFactory);
        this.serverRPC = ServerBuilder.forPort(PORT)
                .addService(new SessionService(requestOpener, mockedAttributeDeduplicatorDaemon))
                .addService(new KeyspaceService(mockedKeyspaceStore))
                .build();
        this.serverRPC.start();

        QueryBuilder qb = mock(QueryBuilder.class);

        when(txFactory.tx(eq(MYKS), any())).thenReturn(tx);
        when(tx.graql()).thenReturn(qb);
        when(qb.parse(QUERY)).thenReturn(query);
        when(query.executor()).thenReturn(executor);

        when(query.execute()).thenAnswer(params -> Stream.of(new ConceptMap()));

        Set<Keyspace> keyspaceSet = new HashSet<>(Arrays.asList(Keyspace.of("testkeyspace1"), Keyspace.of("testkeyspace2")));
        when(mockedKeyspaceStore.keyspaces()).thenReturn(keyspaceSet);
    }

    @After
    public void tearDown() throws InterruptedException {
        serverRPC.shutdown();
        serverRPC.awaitTermination();
    }

    @Test
    public void whenOpeningAReadTransactionRemotely_AReadTransactionIsOpened() {
        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.READ));
        }

        verify(txFactory).tx(MYKS, grakn.core.server.Transaction.Type.READ);
    }

    @Test
    public void whenOpeningAWriteTransactionRemotely_AWriteTransactionIsOpened() {
        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.WRITE));
        }

        verify(txFactory).tx(MYKS, grakn.core.server.Transaction.Type.WRITE);
    }

    @Test
    public void whenOpeningABatchTransactionRemotely_ABatchTransactionIsOpened() {
        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.BATCH));
        }

        verify(txFactory).tx(MYKS, grakn.core.server.Transaction.Type.BATCH);
    }

    @Test
    public void whenOpeningATransactionRemotely_ReceiveADoneMessage() throws InterruptedException {
        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.READ));
            Transaction.Res response = tx.receive().ok();

            assertEquals(ResponseBuilder.Transaction.open(), response);
        }
    }

    @Test
    public void whenCommittingATransactionRemotely_TheTransactionIsCommitted() {
        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.WRITE));
            tx.send(commit());
        }

        verify(tx).commitAndGetLogs();
    }

    @Test
    public void whenCommittingATransactionRemotely_ReceiveADoneMessage() throws InterruptedException {
        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.WRITE));
            tx.receive();

            tx.send(commit());
            Transaction.Res response = tx.receive().ok();

            assertEquals(ResponseBuilder.Transaction.commit(), response);
        }
    }

    @Test
    public void whenOpeningTwoTransactions_TransactionsAreOpenedInDifferentThreads() throws InterruptedException {
        List<Thread> threads = new ArrayList<>();

        when(txFactory.tx(MYKS, grakn.core.server.Transaction.Type.WRITE)).thenAnswer(invocation -> {
            threads.add(Thread.currentThread());
            return tx;
        });

        try (
                Transceiver tx1 = Transceiver.create(stub);
                Transceiver tx2 = Transceiver.create(stub)
        ) {
            tx1.send(open(MYKS, grakn.core.server.Transaction.Type.WRITE));
            tx2.send(open(MYKS, grakn.core.server.Transaction.Type.WRITE));

            tx1.receive();
            tx2.receive();
        }

        assertNotEquals(threads.get(0), threads.get(1));
    }

    @Test
    public void whenOpeningATransactionRemotelyWithAnInvalidKeyspace_Throw() throws Throwable {
        String keyspace = "not!@akeyspace";
        Open.Req openRequest = Open.Req.newBuilder()
                .setKeyspace(keyspace)
                .setType(Transaction.Type.valueOf(grakn.core.server.Transaction.Type.WRITE.getId()))
                .build();

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(Transaction.Req.newBuilder().setOpenReq(openRequest).build());
            exception.expect(hasStatus(Status.INVALID_ARGUMENT));
            throw tx.receive().error();
        }
    }

    @Test
    public void whenClosingATransactionRemotely_TheTransactionIsClosedWithinTheThreadItWasCreated() {
        final Thread[] threadOpenedWith = new Thread[1];
        final Thread[] threadClosedWith = new Thread[1];

        when(txFactory.tx(MYKS, grakn.core.server.Transaction.Type.WRITE)).thenAnswer(invocation -> {
            threadOpenedWith[0] = Thread.currentThread();
            return tx;
        });

        doAnswer(invocation -> {
            threadClosedWith[0] = Thread.currentThread();
            return null;
        }).when(tx).close();

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.WRITE));
        }

        verify(tx).close();
        assertEquals(threadOpenedWith[0], threadClosedWith[0]);
    }

    @Test
    public void whenExecutingAQueryRemotely_AResultIsReturned() throws InterruptedException {
        Concept conceptX = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(conceptX.id()).thenReturn(ConceptId.of("V123"));
        when(conceptX.isRelationship()).thenReturn(true);
        when(conceptX.asRelationship().type().label()).thenReturn(Label.of("L123"));

        Concept conceptY = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(conceptY.id()).thenReturn(ConceptId.of("V456"));
        when(conceptY.isAttribute()).thenReturn(true);
        when(conceptY.asAttribute().type().label()).thenReturn(Label.of("L456"));

        ImmutableList<ConceptMap> answers = ImmutableList.of(
                new ConceptMap(ImmutableMap.of(Pattern.var("x"), conceptX)),
                new ConceptMap(ImmutableMap.of(Pattern.var("y"), conceptY))
        );

        when(tx.stream(query, anyBoolean())).thenAnswer(params -> answers.stream());

        try (Transceiver transceiver = Transceiver.create(stub)) {
            transceiver.send(open(MYKS, grakn.core.server.Transaction.Type.WRITE));
            transceiver.receive();

            transceiver.send(query(QUERY, false));
            int iterator = transceiver.receive().ok().getQueryIter().getId();

            transceiver.send(iterate(iterator));
            Transaction.Res response1 = transceiver.receive().ok();

            ConceptProto.Concept rpcX =
                    ConceptProto.Concept.newBuilder().setId(V123).setBaseType(ConceptProto.Concept.BASE_TYPE.RELATION).build();
            AnswerProto.ConceptMap.Builder conceptMapX = AnswerProto.ConceptMap.newBuilder().putMap("x", rpcX);
            AnswerProto.Answer.Builder answerX = AnswerProto.Answer.newBuilder().setConceptMap(conceptMapX);
            Transaction.Res resX = Transaction.Res.newBuilder()
                    .setIterateRes(Transaction.Iter.Res.newBuilder()
                                           .setQueryIterRes(Transaction.Query.Iter.Res.newBuilder()
                                                                    .setAnswer(answerX))).build();
            assertEquals(resX, response1);

            transceiver.send(iterate(iterator));
            Transaction.Res response2 = transceiver.receive().ok();

            ConceptProto.Concept rpcY =
                    ConceptProto.Concept.newBuilder().setId(V456).setBaseType(ConceptProto.Concept.BASE_TYPE.ATTRIBUTE).build();
            AnswerProto.ConceptMap.Builder conceptMapY = AnswerProto.ConceptMap.newBuilder().putMap("y", rpcY);
            AnswerProto.Answer.Builder answerY = AnswerProto.Answer.newBuilder().setConceptMap(conceptMapY);
            Transaction.Res resY = Transaction.Res.newBuilder()
                    .setIterateRes(Transaction.Iter.Res.newBuilder()
                                           .setQueryIterRes(Transaction.Query.Iter.Res.newBuilder()
                                                                    .setAnswer(answerY))).build();
            assertEquals(resY, response2);

            transceiver.send(iterate(iterator));
            Transaction.Res response3 = transceiver.receive().ok();

            Transaction.Res expected = Transaction.Res.newBuilder().setIterateRes(Transaction.Iter.Res.newBuilder().setDone(true)).build();
            assertEquals(expected, response3);
        }
    }

    @Test(timeout = 1000) // This tests uses an endless stream, so a failure may cause it to never terminate
    public void whenExecutingAQueryRemotelyAndAskingForOneResult_OnlyOneResultIsReturned() throws InterruptedException {
        Concept conceptX = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(conceptX.id()).thenReturn(ConceptId.of("V123"));
        when(conceptX.isEntity()).thenReturn(true);
        when(conceptX.asEntity().type().label()).thenReturn(Label.of("L123"));

        Concept conceptY = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(conceptY.id()).thenReturn(ConceptId.of("V456"));
        when(conceptY.isEntity()).thenReturn(true);
        when(conceptY.asEntity().type().label()).thenReturn(Label.of("L456"));

        ImmutableList<ConceptMap> answers = ImmutableList.of(
                new ConceptMap(ImmutableMap.of(Pattern.var("x"), conceptX)),
                new ConceptMap(ImmutableMap.of(Pattern.var("y"), conceptY))
        );

        // Produce an endless stream of results - this means if the behaviour is not lazy this will never terminate
        when(tx.stream(query, anyBoolean())).thenAnswer(params -> Stream.generate(answers::stream).flatMap(Function.identity()));

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.WRITE));
            tx.receive();

            tx.send(query(QUERY, false));
            int iterator = tx.receive().ok().getQueryIter().getId();

            tx.send(iterate(iterator));
            tx.receive().ok();

            tx.send(iterate(iterator));
            tx.receive().ok();
        }
    }

    @Ignore
    @Test
    public void whenExecutingAQueryRemotelyThatReturnsOneResult_ReturnOneResult() throws InterruptedException {
        String COUNT_QUERY = "compute count;";
        ComputeQuery countQuery = mock(ComputeQuery.class);
        when(tx.graql().parse(COUNT_QUERY)).thenReturn(countQuery);

        when(countQuery.execute()).thenReturn(Collections.singletonList(new Value(100)));

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.WRITE));
            tx.receive();

            tx.send(query(COUNT_QUERY, false));

            Transaction.Res expected = Transaction.Res.newBuilder()
                    .setIterateRes(Transaction.Iter.Res.newBuilder()
                                           .setQueryIterRes(Transaction.Query.Iter.Res.newBuilder()
                                                                    .setAnswer(AnswerProto.Answer.newBuilder()
                                                                                       .setValue(AnswerProto.Value.newBuilder()
                                                                                                         .setNumber(AnswerProto.Number.newBuilder().setValue("100")))))).build();

            assertEquals(expected, tx.receive().ok());
        }
    }

    @Test
    public void whenExecutingAQueryRemotelyWithNoResult_ReturnDone() throws InterruptedException {
        String DELETE_QUERY = "match $x isa person; delete $x";
        DeleteQuery deleteQuery = mock(DeleteQuery.class);
        when(tx.graql().parse(DELETE_QUERY)).thenReturn(deleteQuery);

        when(deleteQuery.execute()).thenReturn(Collections.emptyList());

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.WRITE));
            tx.receive();

            tx.send(query(DELETE_QUERY, false));

            Transaction.Res expected = Transaction.Res.newBuilder()
                    .setQueryIter(Transaction.Query.Iter.newBuilder().setId(1))
                    .build();
            assertEquals(expected, tx.receive().ok());
        }
    }

    @Test
    @Ignore
    public void whenGettingALabel_TheLabelIsReturned() throws InterruptedException {
        ConceptId id = ConceptId.of("V123456");
        Label label = Label.of("Dunstan");

        Concept concept = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(tx.getConcept(id)).thenReturn(concept);
        when(concept.isSchemaConcept()).thenReturn(true);
        when(concept.asSchemaConcept().label()).thenReturn(label);

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.READ));
            tx.receive().ok();

            //tx.send(RequestBuilder.runConceptMethod(id, ConceptMethod.label));

            //assertEquals(label, ConceptMethod.label.readResponse(conceptConverter, client, tx.receive().ok()));
        }
    }

    @Test
    @Ignore
    public void whenGettingIsImplicitProperty_IsImplicitIsReturned() throws InterruptedException {
        ConceptId id = ConceptId.of("V123456");

        Concept concept = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(tx.getConcept(id)).thenReturn(concept);
        when(concept.isSchemaConcept()).thenReturn(true);
        when(concept.asSchemaConcept().isImplicit()).thenReturn(true);

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.READ));
            tx.receive().ok();

            //tx.send(RequestBuilder.runConceptMethod(id, ConceptMethod.isImplicit));

            //assertTrue(ConceptMethod.isImplicit.readResponse(conceptConverter, client, tx.receive().ok()));
        }
    }

    @Test
    @Ignore
    public void whenGettingIsInferredProperty_IsInferredIsReturned() throws InterruptedException {
        ConceptId id = ConceptId.of("V123456");

        Concept concept = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(tx.getConcept(id)).thenReturn(concept);
        when(concept.isThing()).thenReturn(true);
        when(concept.asThing().isInferred()).thenReturn(false);

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.READ));
            tx.receive().ok();

            //tx.send(RequestBuilder.runConceptMethod(id, ConceptMethod.isInferred));

            //assertFalse(ConceptMethod.isInferred.readResponse(conceptConverter, client, tx.receive().ok()));
        }
    }

    @Test
    @Ignore
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
        when(role.id()).thenReturn(roleId);

        Entity player = mock(Entity.class, RETURNS_DEEP_STUBS);
        when(tx.getConcept(playerId)).thenReturn(player);
        when(player.isEntity()).thenReturn(true);
        when(player.asEntity()).thenReturn(player);
        when(player.isThing()).thenReturn(true);
        when(player.asThing()).thenReturn(player);
        when(player.id()).thenReturn(playerId);

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.READ));
            tx.receive().ok();

            //ConceptMethod<Void> conceptMethod = ConceptMethod.unassign(RolePlayer.create(role, player));
            //tx.send(RequestBuilder.runConceptMethod(conceptId, conceptMethod));
            //tx.receive().ok();

            verify(concept.asRelationship()).unassign(role, player);
        }
    }

    @Test
    @Ignore
    public void whenGettingALabelForANonExistentConcept_Throw() throws InterruptedException {
        ConceptId id = ConceptId.of("V123456");

        when(tx.getConcept(id)).thenReturn(null);

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.READ));
            tx.receive().ok();

            //tx.send(RequestBuilder.runConceptMethod(id, ConceptMethod.label));

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().error();
        }
    }

    @Test
    @Ignore
    public void whenGettingALabelForANonSchemaConcept_Throw() throws InterruptedException {
        ConceptId id = ConceptId.of("V123456");

        Concept concept = mock(Concept.class);
        when(tx.getConcept(id)).thenReturn(concept);
        when(concept.isSchemaConcept()).thenReturn(false);
        when(concept.asSchemaConcept()).thenThrow(EXCEPTION);

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.READ));
            tx.receive().ok();

            //tx.send(RequestBuilder.runConceptMethod(id, ConceptMethod.label));

            exception.expect(hasStatus(Status.UNKNOWN.withDescription(EXCEPTION_MESSAGE)));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenGettingAConcept_ReturnTheConcept() throws InterruptedException {
        ConceptId id = ConceptId.of("V123456");

        Concept concept = mock(Concept.class, RETURNS_DEEP_STUBS);
        when(concept.id()).thenReturn(id);
        when(concept.isRelationship()).thenReturn(true);

        when(tx.getConcept(id)).thenReturn(concept);

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.READ));
            tx.receive().ok();

            tx.send(RequestBuilder.Transaction.getConcept(id));

            ConceptProto.Concept response = tx.receive().ok().getGetConceptRes().getConcept();

            assertEquals(id.getValue(), response.getId());
            assertEquals(ConceptProto.Concept.BASE_TYPE.RELATION, response.getBaseType());
        }
    }

    @Test
    public void whenGettingANonExistentConcept_ReturnNothing() throws InterruptedException {
        ConceptId id = ConceptId.of("V123456");

        when(tx.getConcept(id)).thenReturn(null);

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.READ));
            tx.receive().ok();

            tx.send(RequestBuilder.Transaction.getConcept(id));

            assertTrue(tx.receive().ok().getGetConceptRes().getNull().isInitialized());
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
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.WRITE));
            tx.receive();

            tx.send(open(MYKS, grakn.core.server.Transaction.Type.WRITE));

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenOpeningTxFails_Throw() throws Throwable {
        String message = "the backend went wrong";
        GraknException error = GraknServerException.create(message);

        when(txFactory.tx(MYKS, grakn.core.server.Transaction.Type.WRITE)).thenThrow(error);

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.WRITE));
            exception.expect(hasStatus(Status.INTERNAL));
            throw tx.receive().error();
        }
    }

    @Test
    public void whenCommittingFails_Throw() throws Throwable {
        doThrow(EXCEPTION).when(tx).commitAndGetLogs();

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.WRITE));
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
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.WRITE));
            tx.receive();
            tx.send(query(QUERY, false));

            exception.expect(hasStatus(Status.INVALID_ARGUMENT));
            throw tx.receive().error();
        }
    }

    @Test
    public void whenExecutingQueryFails_Throw() throws Throwable {
        String message = "your query is dumb";
        GraknException expectedException = InvalidKBException.create(message);

        when(tx.stream(query, anyBoolean())).thenThrow(expectedException);

        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.WRITE));
            tx.receive();
            tx.send(query(QUERY, false));

            exception.expect(hasStatus(Status.INVALID_ARGUMENT));
            throw tx.receive().error();
        }
    }

    @Test
    public void whenSendingNextBeforeQuery_Throw() throws Throwable {
        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.WRITE));
            tx.receive();

            tx.send(iterate(0));

            exception.expect(hasStatus(Status.FAILED_PRECONDITION));

            throw tx.receive().error();
        }
    }

    @Test
    public void whenSendingAnotherQueryDuringQueryExecution_ReturnResultsForBothQueries() throws Throwable {
        try (Transceiver tx = Transceiver.create(stub)) {
            tx.send(open(MYKS, grakn.core.server.Transaction.Type.WRITE));
            tx.receive();

            tx.send(query(QUERY, false));
            int iterator1 = tx.receive().ok().getQueryIter().getId();

            tx.send(query(QUERY, false));
            int iterator2 = tx.receive().ok().getQueryIter().getId();

            tx.send(iterate(iterator1));
            tx.receive().ok();

            tx.send(iterate(iterator2));
            tx.receive().ok();
        }
    }

    @Test
    public void whenSendingDeleteRequestWithInvalidKeyspace_CallDeleteOnEmbeddedTx() {
        String keyspace = "not!@akeyspace";
        exception.expect(hasStatus(Status.INVALID_ARGUMENT));
        keyspaceBlockingStub.delete(RequestBuilder.Keyspace.delete(keyspace));
    }

    @Test
    public void whenSendingRetrieveKeyspacesReq_ReturnListOfExistingKeyspaces() {
        KeyspaceProto.Keyspace.Retrieve.Res response = keyspaceBlockingStub.retrieve(KeyspaceProto.Keyspace.Retrieve.Req.getDefaultInstance());
        assertThat(new ArrayList<>(response.getNamesList()), containsInAnyOrder("testkeyspace1", "testkeyspace2"));
    }

    private static Matcher<StatusRuntimeException> hasStatus(Status status) {

        Matcher<Status> hasCode = hasProperty("code", is(status.getCode()));
        Matcher<Status> statusMatcher;

        String description = status.getDescription();

        if (description == null) {
            statusMatcher = hasCode;
        } else {
            Matcher<Status> hasDescription = hasProperty("description", is(description));
            statusMatcher = allOf(hasCode, hasDescription);
        }

        return allOf(isA(StatusRuntimeException.class), hasProperty("status", statusMatcher));
    }

}
