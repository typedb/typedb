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

package ai.grakn.client;

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.client.concept.RemoteConcept;
import ai.grakn.client.rpc.RequestBuilder;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.GraknException;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.exception.PropertyNotUniqueException;
import ai.grakn.exception.TemporaryWriteException;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.rpc.proto.AnswerProto;
import ai.grakn.rpc.proto.ConceptProto;
import ai.grakn.rpc.proto.KeyspaceServiceGrpc;
import ai.grakn.rpc.proto.SessionProto;
import ai.grakn.rpc.proto.SessionProto.Transaction;
import ai.grakn.rpc.proto.SessionServiceGrpc;
import com.google.common.collect.ImmutableSet;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcServerRule;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.function.Consumer;

import static ai.grakn.graql.Graql.match;
import static ai.grakn.graql.Graql.var;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit Tests for {@link ai.grakn.client.Grakn.Transaction}
 */
public class TransactionTest {

    private final static SessionServiceGrpc.SessionServiceImplBase sessionService = mock(SessionServiceGrpc.SessionServiceImplBase.class);
    private final static KeyspaceServiceGrpc.KeyspaceServiceImplBase keyspaceService = mock(KeyspaceServiceGrpc.KeyspaceServiceImplBase.class);

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    // The gRPC server itself is "real" and can be connected to using the {@link #channel()}
    @Rule
    public final static GrpcServerRule serverRule = new GrpcServerRule().directExecutor();


    @Rule
    public final ServerRPCMock server = new ServerRPCMock(sessionService, keyspaceService);

    private final Grakn.Session session = mock(Grakn.Session.class);

    private static final Keyspace KEYSPACE = Keyspace.of("blahblah");
    private static final String V123 = "V123";
    private static final int ITERATOR = 100;

    @Before
    public void setUp() {
        serverRule.getServiceRegistry().addService(sessionService);
        serverRule.getServiceRegistry().addService(keyspaceService);
        when(session.sessionStub()).thenReturn(SessionServiceGrpc.newStub(serverRule.getChannel()));
        when(session.keyspaceBlockingStub()).thenReturn(KeyspaceServiceGrpc.newBlockingStub(serverRule.getChannel()));
        when(session.keyspace()).thenReturn(KEYSPACE);
        when(session.transaction(any())).thenCallRealMethod();
    }

    @Test
    public void whenCreatingAGraknRemoteTx_MakeATxCallToGrpc() {
        try (GraknTx ignored = session.transaction(GraknTxType.WRITE)) {
            verify(server.sessionService()).transaction(any());
        }
    }

    @Test
    public void whenCreatingAGraknRemoteTx_SendAnOpenMessageToGrpc() {
        try (GraknTx ignored = session.transaction(GraknTxType.WRITE)) {
            verify(server.requestListener()).onNext(RequestBuilder.Transaction.open(Keyspace.of(KEYSPACE.getValue()), GraknTxType.WRITE));
        }
    }

    @Test
    public void whenCreatingABatchGraknRemoteTx_SendAnOpenMessageWithBatchSpecifiedToGrpc() {
        try (GraknTx ignored = session.transaction(GraknTxType.BATCH)) {
            verify(server.requestListener()).onNext(RequestBuilder.Transaction.open(Keyspace.of(KEYSPACE.getValue()), GraknTxType.BATCH));
        }
    }

    @Test
    public void whenClosingAGraknRemoteTx_SendCompletedMessageToGrpc() {
        try (GraknTx ignored = session.transaction(GraknTxType.WRITE)) {
            verify(server.requestListener(), never()).onCompleted(); // Make sure transaction is still open here
        }

        verify(server.requestListener()).onCompleted();
    }

    @Test
    public void whenCreatingAGraknRemoteTxWithSession_SetKeyspaceOnTx() {
        try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
            assertEquals(session, tx.session());
        }
    }

    @Test
    public void whenCreatingAGraknRemoteTxWithSession_SetTxTypeOnTx() {
        try (Grakn.Transaction tx = session.transaction(GraknTxType.BATCH)) {
            assertEquals(GraknTxType.BATCH, tx.txType());
        }
    }

    @Test(timeout = 5_000)
    public void whenStreamingAQueryWithInfiniteAnswers_Terminate() {
        Transaction.Res queryIterator = SessionProto.Transaction.Res.newBuilder()
                .setQueryIter(SessionProto.Transaction.Query.Iter.newBuilder().setId(ITERATOR))
                .build();

        Query<?> query = match(var("x").sub("thing")).get();
        String queryString = query.toString();
        ConceptProto.Concept v123 = ConceptProto.Concept.newBuilder().setId(V123).build();
        Transaction.Res iteratorNext = Transaction.Res.newBuilder()
                .setIterateRes(SessionProto.Transaction.Iter.Res.newBuilder()
                        .setQueryIterRes(SessionProto.Transaction.Query.Iter.Res.newBuilder()
                                .setAnswer(AnswerProto.Answer.newBuilder()
                                        .setConceptMap(AnswerProto.ConceptMap.newBuilder().putMap("x", v123))))).build();

        server.setResponse(RequestBuilder.Transaction.query(query), queryIterator);
        server.setResponse(RequestBuilder.Transaction.iterate(ITERATOR), iteratorNext);

        List<ConceptMap> answers;
        int numAnswers = 10;

        try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
            verify(server.requestListener()).onNext(any()); // The open request
            answers = tx.graql().<GetQuery>parse(queryString).stream().limit(numAnswers).collect(toList());
        }

        assertEquals(10, answers.size());

        for (ConceptMap answer : answers) {
            assertEquals(answer.vars(), ImmutableSet.of(var("x")));
            assertEquals(ConceptId.of("V123"), answer.get(var("x")).id());
        }
    }

    @Test
    public void whenCommitting_SendACommitMessageToGrpc() {
        try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
            verify(server.requestListener()).onNext(any()); // The open request

            tx.commit();
        }

        verify(server.requestListener()).onNext(RequestBuilder.Transaction.commit());
    }

    @Test
    public void whenCreatingAGraknRemoteTxWithKeyspace_SetsKeyspaceOnTx() {
        try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
            assertEquals(KEYSPACE, tx.keyspace());
        }
    }

    @Test
    public void whenOpeningATxFails_Throw() {
        Transaction.Req openRequest = RequestBuilder.Transaction.open(KEYSPACE, GraknTxType.WRITE);
        GraknException expectedException = GraknBackendException.create("well something went wrong");
        throwOn(openRequest, expectedException);

        exception.expect(RuntimeException.class);
        exception.expectMessage(expectedException.getName());
        exception.expectMessage(expectedException.getMessage());

        Grakn.Transaction tx = session.transaction(GraknTxType.WRITE);
        tx.close();
    }

    @Test
    public void whenCommittingATxFails_Throw() {
        GraknException expectedException = InvalidKBException.create("do it better next time");
        throwOn(RequestBuilder.Transaction.commit(), expectedException);

        try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
            exception.expect(RuntimeException.class);
            exception.expectMessage(expectedException.getName());
            exception.expectMessage(expectedException.getMessage());
            tx.commit();
        }
    }

    @Test
    public void whenAnErrorOccurs_TheTxCloses() {
        Query<?> query = match(var("x")).get();

        Transaction.Req execQueryRequest = RequestBuilder.Transaction.query(query);
        GraknException expectedException = GraqlQueryException.create("well something went wrong.");
        throwOn(execQueryRequest, expectedException);

        try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
            try {
                tx.graql().match(var("x")).get().execute();
            } catch (RuntimeException e) {
                System.out.println(e.getMessage());
                assertTrue(e.getMessage().contains(expectedException.getName()));
            }

            assertTrue(tx.isClosed());
        }
    }

    @Test
    public void whenAnErrorOccurs_AllFutureActionsThrow() {
        Query<?> query = match(var("x")).get();

        Transaction.Req execQueryRequest = RequestBuilder.Transaction.query(query);
        GraknException expectedException = GraqlQueryException.create("well something went wrong.");
        throwOn(execQueryRequest, expectedException);

        try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
            try {
                tx.graql().match(var("x")).get().execute();
            } catch (RuntimeException e) {
                System.out.println(e.getMessage());
                assertTrue(e.getMessage().contains(expectedException.getName()));
            }

            exception.expect(GraknTxOperationException.class);
            exception.expectMessage(GraknTxOperationException.transactionClosed(null, "The gRPC connection closed").getMessage());
            tx.admin().getMetaConcept();
        }
    }

    @Test
    public void whenPuttingEntityType_EnsureCorrectRequestIsSent() {
        ConceptId id = ConceptId.of(V123);
        Label label = Label.of("foo");

        try (Grakn.Transaction tx = session.transaction(GraknTxType.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            ConceptProto.Concept protoConcept = ConceptProto.Concept.newBuilder()
                    .setId(id.getValue()).setBaseType(ConceptProto.Concept.BASE_TYPE.ENTITY_TYPE).build();

            Transaction.Res response = Transaction.Res.newBuilder().setPutEntityTypeRes(SessionProto.Transaction.PutEntityType.Res.newBuilder()
                    .setEntityType(protoConcept)).build();
            server.setResponse(RequestBuilder.Transaction.putEntityType(label), response);

            assertEquals(RemoteConcept.of(protoConcept, tx), tx.putEntityType(label));
        }
    }

    @Test
    public void whenPuttingRelationshipType_EnsureCorrectRequestIsSent() {
        ConceptId id = ConceptId.of(V123);
        Label label = Label.of("foo");

        try (Grakn.Transaction tx = session.transaction(GraknTxType.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            ConceptProto.Concept protoConcept = ConceptProto.Concept.newBuilder()
                    .setId(id.getValue()).setBaseType(ConceptProto.Concept.BASE_TYPE.RELATION_TYPE).build();

            Transaction.Res response = Transaction.Res.newBuilder()
                    .setPutRelationTypeRes(SessionProto.Transaction.PutRelationType.Res.newBuilder()
                            .setRelationType(protoConcept)).build();
            server.setResponse(RequestBuilder.Transaction.putRelationshipType(label), response);

            assertEquals(RemoteConcept.of(protoConcept, tx), tx.putRelationshipType(label));
        }
    }

    @Test
    public void whenPuttingAttributeType_EnsureCorrectRequestIsSent() {
        ConceptId id = ConceptId.of(V123);
        Label label = Label.of("foo");
        AttributeType.DataType<?> dataType = AttributeType.DataType.STRING;

        try (Grakn.Transaction tx = session.transaction(GraknTxType.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            ConceptProto.Concept protoConcept = ConceptProto.Concept.newBuilder()
                    .setId(id.getValue()).setBaseType(ConceptProto.Concept.BASE_TYPE.ATTRIBUTE_TYPE).build();

            Transaction.Res response = Transaction.Res.newBuilder()
                    .setPutAttributeTypeRes(SessionProto.Transaction.PutAttributeType.Res.newBuilder()
                            .setAttributeType(protoConcept)).build();
            server.setResponse(RequestBuilder.Transaction.putAttributeType(label, dataType), response);

            assertEquals(RemoteConcept.of(protoConcept, tx), tx.putAttributeType(label, dataType));
        }
    }

    @Test
    public void whenPuttingRole_EnsureCorrectRequestIsSent() {
        ConceptId id = ConceptId.of(V123);
        Label label = Label.of("foo");

        try (Grakn.Transaction tx = session.transaction(GraknTxType.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            ConceptProto.Concept protoConcept = ConceptProto.Concept.newBuilder()
                    .setId(id.getValue()).setBaseType(ConceptProto.Concept.BASE_TYPE.ROLE).build();

            Transaction.Res response = Transaction.Res.newBuilder()
                    .setPutRoleRes(SessionProto.Transaction.PutRole.Res.newBuilder()
                            .setRole(protoConcept)).build();
            server.setResponse(RequestBuilder.Transaction.putRole(label), response);

            assertEquals(RemoteConcept.of(protoConcept, tx), tx.putRole(label));
        }
    }

    @Test
    public void whenPuttingRule_EnsureCorrectRequestIsSent() {
        ConceptId id = ConceptId.of(V123);
        Label label = Label.of("foo");
        Pattern when = var("x").isa("person");
        Pattern then = var("y").isa("person");

        try (Grakn.Transaction tx = session.transaction(GraknTxType.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            ConceptProto.Concept protoConcept = ConceptProto.Concept.newBuilder()
                    .setId(id.getValue()).setBaseType(ConceptProto.Concept.BASE_TYPE.RULE).build();

            Transaction.Res response = Transaction.Res.newBuilder()
                    .setPutRuleRes(SessionProto.Transaction.PutRule.Res.newBuilder()
                            .setRule(protoConcept)).build();
            server.setResponse(RequestBuilder.Transaction.putRule(label, when, then), response);

            assertEquals(RemoteConcept.of(protoConcept, tx), tx.putRule(label, when, then));
        }
    }

    @Test
    public void whenGettingConceptViaID_EnsureCorrectRequestIsSent() {
        ConceptId id = ConceptId.of(V123);

        try (Grakn.Transaction tx = session.transaction(GraknTxType.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            ConceptProto.Concept protoConcept = ConceptProto.Concept.newBuilder()
                    .setId(id.getValue()).setBaseType(ConceptProto.Concept.BASE_TYPE.ENTITY).build();

            SessionProto.Transaction.Res response = SessionProto.Transaction.Res.newBuilder()
                    .setGetConceptRes(SessionProto.Transaction.GetConcept.Res.newBuilder()
                            .setConcept(protoConcept)).build();
            server.setResponse(RequestBuilder.Transaction.getConcept(id), response);

            assertEquals(RemoteConcept.of(protoConcept, tx), tx.getConcept(id));
        }
    }

    @Test
    public void whenGettingNonExistentConceptViaID_ReturnNull() {
        ConceptId id = ConceptId.of(V123);

        try (Grakn.Transaction tx = session.transaction(GraknTxType.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            SessionProto.Transaction.Res response = SessionProto.Transaction.Res.newBuilder()
                    .setGetConceptRes(SessionProto.Transaction.GetConcept.Res.newBuilder()
                            .setNull(ConceptProto.Null.getDefaultInstance()))
                    .build();
            server.setResponse(RequestBuilder.Transaction.getConcept(id), response);

            assertNull(tx.getConcept(id));
        }
    }

    @Test
    public void whenGettingSchemaConceptViaLabel_EnsureCorrectRequestIsSent() {
        Label label = Label.of("foo");
        ConceptId id = ConceptId.of(V123);

        try (Grakn.Transaction tx = session.transaction(GraknTxType.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            ConceptProto.Concept protoConcept = ConceptProto.Concept.newBuilder()
                    .setId(id.getValue()).setBaseType(ConceptProto.Concept.BASE_TYPE.ATTRIBUTE_TYPE).build();

            SessionProto.Transaction.Res response = SessionProto.Transaction.Res.newBuilder()
                    .setGetSchemaConceptRes(SessionProto.Transaction.GetSchemaConcept.Res.newBuilder()
                            .setSchemaConcept(protoConcept))
                    .build();
            server.setResponse(RequestBuilder.Transaction.getSchemaConcept(label), response);

            assertEquals(RemoteConcept.of(protoConcept, tx), tx.getSchemaConcept(label));
        }
    }

    @Test
    public void whenGettingNonExistentSchemaConceptViaLabel_ReturnNull() {
        Label label = Label.of("foo");

        try (Grakn.Transaction tx = session.transaction(GraknTxType.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            SessionProto.Transaction.Res response = SessionProto.Transaction.Res.newBuilder()
                    .setGetSchemaConceptRes(SessionProto.Transaction.GetSchemaConcept.Res.newBuilder()
                            .setNull(ConceptProto.Null.getDefaultInstance()))
                    .build();
            server.setResponse(RequestBuilder.Transaction.getSchemaConcept(label), response);

            assertNull(tx.getSchemaConcept(label));
        }
    }

    @Test
    @Ignore
    public void whenGettingAttributesViaID_EnsureCorrectRequestIsSent() {
        String value = "Hello Oli";

        try (Grakn.Transaction tx = session.transaction(GraknTxType.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            ConceptProto.Concept attribute1 = ConceptProto.Concept.newBuilder()
                    .setId("A").setBaseType(ConceptProto.Concept.BASE_TYPE.ATTRIBUTE).build();
            ConceptProto.Concept attribute2 = ConceptProto.Concept.newBuilder()
                    .setId("B").setBaseType(ConceptProto.Concept.BASE_TYPE.ATTRIBUTE).build();

            assertThat(tx.getAttributesByValue(value), containsInAnyOrder(attribute1, attribute2));
        }
    }

    @Test
    public void whenClosingTheTransaction_EnsureItIsFlaggedAsClosed() {
        assertTransactionClosedAfterAction(GraknTx::close);
    }

    @Test
    public void whenCommittingTheTransaction_EnsureItIsFlaggedAsClosed() {
        assertTransactionClosedAfterAction(GraknTx::commit);
    }

    @Test
    public void whenAbortingTheTransaction_EnsureItIsFlaggedAsClosed() {
        assertTransactionClosedAfterAction(GraknTx::abort);
    }

    private void assertTransactionClosedAfterAction(Consumer<GraknTx> action) {
        Grakn.Transaction tx = session.transaction(GraknTxType.WRITE);
        assertFalse(tx.isClosed());
        action.accept(tx);
        assertTrue(tx.isClosed());
    }

    private void throwOn(Transaction.Req request, GraknException e) {
        StatusRuntimeException exception;

        if (e instanceof TemporaryWriteException) {
            exception = error(Status.RESOURCE_EXHAUSTED, e);
        } else if (e instanceof GraknBackendException) {
            exception = error(Status.INTERNAL, e);
        } else if (e instanceof PropertyNotUniqueException) {
            exception = error(Status.ALREADY_EXISTS, e);
        } else if (e instanceof GraknTxOperationException || e instanceof GraqlQueryException || e instanceof GraqlSyntaxException || e instanceof InvalidKBException) {
            exception = error(Status.INVALID_ARGUMENT, e);
        } else {
            exception = error(Status.UNKNOWN, e);
        }

        server.setResponse(request, exception);
    }

    private static StatusRuntimeException error(Status status, GraknException e) {
        return status.withDescription(e.getName() + " - " + e.getMessage()).asRuntimeException();
    }
}
