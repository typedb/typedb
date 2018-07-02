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

package ai.grakn.client;

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.client.concept.RemoteAttribute;
import ai.grakn.client.concept.RemoteAttributeType;
import ai.grakn.client.concept.RemoteEntity;
import ai.grakn.client.concept.RemoteEntityType;
import ai.grakn.client.concept.RemoteRelationshipType;
import ai.grakn.client.concept.RemoteRole;
import ai.grakn.client.concept.RemoteRule;
import ai.grakn.client.rpc.ConceptBuilder;
import ai.grakn.client.rpc.RequestBuilder;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
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
import ai.grakn.graql.admin.Answer;
import ai.grakn.rpc.proto.ConceptProto;
import ai.grakn.rpc.proto.IteratorProto.IteratorId;
import ai.grakn.rpc.proto.KeyspaceGrpc;
import ai.grakn.rpc.proto.KeyspaceProto;
import ai.grakn.rpc.proto.SessionGrpc;
import ai.grakn.rpc.proto.SessionProto;
import ai.grakn.rpc.proto.SessionProto.Transaction;
import com.google.common.collect.ImmutableSet;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Before;
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit Tests for {@link ai.grakn.client.Grakn.Transaction}
 */
public class TransactionTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Rule
    public final ServerRPCMock server = ServerRPCMock.create();

    private final Grakn.Session session = mock(Grakn.Session.class);

    private static final Keyspace KEYSPACE = Keyspace.of("blahblah");
    private static final String V123 = "V123";
    private static final IteratorId ITERATOR = IteratorId.newBuilder().setId(100).build();

    @Before
    public void setUp() {
        when(session.sessionStub()).thenReturn(SessionGrpc.newStub(server.channel()));
        when(session.keyspaceBlockingStub()).thenReturn(KeyspaceGrpc.newBlockingStub(server.channel()));
        when(session.keyspace()).thenReturn(KEYSPACE);
        when(session.transaction(any())).thenCallRealMethod();
    }
    
    private static SessionProto.Transaction.Res response(Concept concept) {
        return SessionProto.Transaction.Res.newBuilder().setConcept(ConceptBuilder.concept(concept)).build();
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

    @Test
    public void whenExecutingAQueryWithAVoidResult_GetANullBack() {
        Query<?> query = match(var("x").isa("person")).delete("x");
        String queryString = query.toString();

        Transaction.Res response = SessionProto.Transaction.Res.newBuilder()
                .setQuery(SessionProto.Query.Res.newBuilder().setNull(SessionProto.Null.getDefaultInstance()))
                .build();

        server.setResponse(RequestBuilder.Transaction.query(query), response);

        try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
            verify(server.requestListener()).onNext(any()); // The open request
            assertNull(tx.graql().parse(queryString).execute());
        }
    }

    @Test(timeout = 5_000)
    public void whenStreamingAQueryWithInfiniteAnswers_Terminate() {
        Transaction.Res queryIterator = SessionProto.Transaction.Res.newBuilder()
                .setQuery(SessionProto.Query.Res.newBuilder().setIteratorId(ITERATOR))
                .build();

        Query<?> query = match(var("x").sub("thing")).get();
        String queryString = query.toString();
        ConceptProto.Concept v123 = ConceptProto.Concept.newBuilder().setId(V123).build();
        Transaction.Res iteratorNext = Transaction.Res.newBuilder().setAnswer(ConceptProto.Answer.newBuilder()
                .setQueryAnswer(ConceptProto.QueryAnswer.newBuilder().putQueryAnswer("x", v123))).build();

        server.setResponse(RequestBuilder.Transaction.query(query), queryIterator);
        server.setResponse(RequestBuilder.Transaction.next(ITERATOR), iteratorNext);

        List<Answer> answers;
        int numAnswers = 10;

        try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
            verify(server.requestListener()).onNext(any()); // The open request
            answers = tx.graql().<GetQuery>parse(queryString).stream().limit(numAnswers).collect(toList());
        }

        assertEquals(10, answers.size());

        for (Answer answer : answers) {
            assertEquals(answer.vars(), ImmutableSet.of(var("x")));
            assertEquals(ConceptId.of("V123"), answer.get(var("x")).getId());
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
    public void whenPuttingEntityType_EnsureCorrectRequestIsSent(){
        ConceptId id = ConceptId.of(V123);
        Label label = Label.of("foo");

        try (Grakn.Transaction tx = session.transaction(GraknTxType.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            Concept concept = RemoteEntityType.create(tx, id);
            Transaction.Res response = Transaction.Res.newBuilder().setPutEntityType(SessionProto.PutEntityType.Res.newBuilder()
                    .setConcept(ConceptBuilder.concept(concept))).build();
            server.setResponse(RequestBuilder.Transaction.putEntityType(label), response);

            assertEquals(concept, tx.putEntityType(label));
        }
    }

    @Test
    public void whenPuttingRelationshipType_EnsureCorrectRequestIsSent(){
        ConceptId id = ConceptId.of(V123);
        Label label = Label.of("foo");

        try (Grakn.Transaction tx = session.transaction(GraknTxType.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            Concept concept = RemoteRelationshipType.create(tx, id);
            Transaction.Res response = Transaction.Res.newBuilder()
                    .setPutRelationshipType(SessionProto.PutRelationshipType.Res.newBuilder()
                            .setConcept(ConceptBuilder.concept(concept))).build();
            server.setResponse(RequestBuilder.Transaction.putRelationshipType(label), response);

            assertEquals(concept, tx.putRelationshipType(label));
        }
    }

    @Test
    public void whenPuttingAttributeType_EnsureCorrectRequestIsSent(){
        ConceptId id = ConceptId.of(V123);
        Label label = Label.of("foo");
        AttributeType.DataType<?> dataType = AttributeType.DataType.STRING;

        try (Grakn.Transaction tx = session.transaction(GraknTxType.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            Concept concept = RemoteAttributeType.create(tx, id);
            Transaction.Res response = Transaction.Res.newBuilder()
                    .setPutAttributeType(SessionProto.PutAttributeType.Res.newBuilder()
                            .setConcept(ConceptBuilder.concept(concept))).build();
            server.setResponse(RequestBuilder.Transaction.putAttributeType(label, dataType), response);

            assertEquals(concept, tx.putAttributeType(label, dataType));
        }
    }

    @Test
    public void whenPuttingRole_EnsureCorrectRequestIsSent(){
        ConceptId id = ConceptId.of(V123);
        Label label = Label.of("foo");

        try (Grakn.Transaction tx = session.transaction(GraknTxType.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            Concept concept = RemoteRole.create(tx, id);
            Transaction.Res response = Transaction.Res.newBuilder()
                    .setPutRole(SessionProto.PutRole.Res.newBuilder()
                            .setConcept(ConceptBuilder.concept(concept))).build();
            server.setResponse(RequestBuilder.Transaction.putRole(label), response);

            assertEquals(concept, tx.putRole(label));
        }
    }

    @Test
    public void whenPuttingRule_EnsureCorrectRequestIsSent(){
        ConceptId id = ConceptId.of(V123);
        Label label = Label.of("foo");
        Pattern when = var("x").isa("person");
        Pattern then = var("y").isa("person");

        try (Grakn.Transaction tx = session.transaction(GraknTxType.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            Concept concept = RemoteRule.create(tx, id);
            Transaction.Res response = Transaction.Res.newBuilder()
                    .setPutRule(SessionProto.PutRule.Res.newBuilder()
                            .setConcept(ConceptBuilder.concept(concept))).build();
            server.setResponse(RequestBuilder.Transaction.putRule(label, when, then), response);

            assertEquals(concept, tx.putRule(label, when, then));
        }
    }

    @Test
    public void whenGettingConceptViaID_EnsureCorrectRequestIsSent(){
        ConceptId id = ConceptId.of(V123);

        try (Grakn.Transaction tx = session.transaction(GraknTxType.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            Concept concept = RemoteEntity.create(tx, id);

            SessionProto.Transaction.Res response = SessionProto.Transaction.Res.newBuilder()
                    .setGetConcept(SessionProto.GetConcept.Res.newBuilder()
                            .setConcept(ConceptBuilder.concept(concept))).build();
            server.setResponse(RequestBuilder.Transaction.getConcept(id), response);

            assertEquals(concept, tx.getConcept(id));
        }
    }

    @Test
    public void whenGettingNonExistentConceptViaID_ReturnNull(){
        ConceptId id = ConceptId.of(V123);

        try (Grakn.Transaction tx = session.transaction(GraknTxType.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            SessionProto.Transaction.Res response = SessionProto.Transaction.Res.newBuilder()
                    .setGetConcept(SessionProto.GetConcept.Res.newBuilder()
                            .setNull(SessionProto.Null.getDefaultInstance()))
                    .build();
            server.setResponse(RequestBuilder.Transaction.getConcept(id), response);

            assertNull(tx.getConcept(id));
        }
    }

    @Test
    public void whenGettingSchemaConceptViaLabel_EnsureCorrectRequestIsSent(){
        Label label = Label.of("foo");
        ConceptId id = ConceptId.of(V123);

        try (Grakn.Transaction tx = session.transaction(GraknTxType.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            Concept concept = RemoteAttributeType.create(tx, id);
            SessionProto.Transaction.Res response = SessionProto.Transaction.Res.newBuilder()
                    .setGetSchemaConcept(SessionProto.GetSchemaConcept.Res.newBuilder()
                            .setConcept(ConceptBuilder.concept(concept)))
                    .build();
            server.setResponse(RequestBuilder.Transaction.getSchemaConcept(label), response);

            assertEquals(concept, tx.getSchemaConcept(label));
        }
    }

    @Test
    public void whenGettingNonExistentSchemaConceptViaLabel_ReturnNull(){
        Label label = Label.of("foo");

        try (Grakn.Transaction tx = session.transaction(GraknTxType.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            SessionProto.Transaction.Res response = SessionProto.Transaction.Res.newBuilder()
                    .setGetSchemaConcept(SessionProto.GetSchemaConcept.Res.newBuilder()
                            .setNull(SessionProto.Null.getDefaultInstance()))
                    .build();
            server.setResponse(RequestBuilder.Transaction.getSchemaConcept(label), response);

            assertNull(tx.getSchemaConcept(label));
        }
    }

    @Test
    public void whenGettingAttributesViaID_EnsureCorrectRequestIsSent(){
        String value = "Hello Oli";

        try (Grakn.Transaction tx = session.transaction(GraknTxType.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            Attribute<?> attribute1 = RemoteAttribute.create(tx, ConceptId.of("A"));
            Attribute<?> attribute2 = RemoteAttribute.create(tx, ConceptId.of("B"));

            server.setResponseSequence(
                    RequestBuilder.Transaction.getAttributes(value),
                    response(attribute1),
                    response(attribute2)
            );

            assertThat(tx.getAttributesByValue(value), containsInAnyOrder(attribute1, attribute2));
        }
    }

    @Test
    public void whenClosingTheTransaction_EnsureItIsFlaggedAsClosed(){
        assertTransactionClosedAfterAction(GraknTx::close);
    }

    @Test
    public void whenCommittingTheTransaction_EnsureItIsFlaggedAsClosed(){
        assertTransactionClosedAfterAction(GraknTx::commit);
    }

    @Test
    public void whenAbortingTheTransaction_EnsureItIsFlaggedAsClosed(){
        assertTransactionClosedAfterAction(GraknTx::abort);
    }

    @Test
    public void whenDeletingTheTransaction_CallDeleteOverGrpc(){
        KeyspaceProto.Delete.Req request = RequestBuilder.Keyspace.delete(KEYSPACE.getValue());

        try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
            tx.admin().delete();
        }

        verify(server.keyspaceService()).delete(eq(request), any());
    }

    private void assertTransactionClosedAfterAction(Consumer<GraknTx> action){
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
