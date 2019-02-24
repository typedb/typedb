/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package grakn.core.client.test;

import grakn.core.client.GraknClient;
import grakn.core.client.concept.RemoteConcept;
import grakn.core.client.rpc.RequestBuilder;
import grakn.core.common.exception.GraknException;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.concept.AttributeType;
import grakn.core.concept.ConceptId;
import grakn.core.concept.Label;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.protocol.AnswerProto;
import grakn.core.protocol.ConceptProto;
import grakn.core.protocol.KeyspaceServiceGrpc;
import grakn.core.protocol.SessionProto;
import grakn.core.protocol.SessionServiceGrpc;
import grakn.core.server.Transaction;
import grakn.core.server.exception.GraknServerException;
import grakn.core.server.exception.InvalidKBException;
import grakn.core.server.exception.PropertyNotUniqueException;
import grakn.core.server.exception.TemporaryWriteException;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.keyspace.Keyspace;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Variable;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcServerRule;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static graql.lang.Graql.match;
import static graql.lang.Graql.var;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Unit Tests for GraknClient.Transaction
 */
@SuppressWarnings("Duplicates")
public class GraknClientTest {

    private final static SessionServiceGrpc.SessionServiceImplBase sessionService = mock(SessionServiceGrpc.SessionServiceImplBase.class);
    private final static KeyspaceServiceGrpc.KeyspaceServiceImplBase keyspaceService = mock(KeyspaceServiceGrpc.KeyspaceServiceImplBase.class);
    private static final Keyspace KEYSPACE = Keyspace.of("grakn");
    private static final String V123 = "V123";
    private static final int ITERATOR = 100;
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    // The gRPC server itself is "real" and can be connected to using the #channel()
    @Rule
    public final GrpcServerRule serverRule = new GrpcServerRule().directExecutor();
    @Rule
    public final GraknServerRPCMock server = new GraknServerRPCMock(sessionService, keyspaceService);
    private GraknClient.Session session;

    private static StatusRuntimeException error(Status status, GraknException e) {
        return status.withDescription(e.getName() + " - " + e.getMessage()).asRuntimeException();
    }

    @Before
    public void setUp() {
        serverRule.getServiceRegistry().addService(sessionService);
        serverRule.getServiceRegistry().addService(keyspaceService);
        session = new GraknClient().overrideChannel(serverRule.getChannel()).session(KEYSPACE.getName());
    }

    @Test
    public void whenCreatingAGraknRemoteTx_MakeATxCallToGrpc() {
        try (Transaction ignored = session.transaction(Transaction.Type.WRITE)) {
            verify(server.sessionService(), atLeast(1)).transaction(any());
        }
    }

    @Test
    public void whenCreatingAGraknRemoteTx_SendAnOpenMessageToGrpc() {
        try (Transaction ignored = session.transaction(Transaction.Type.WRITE)) {
            verify(server.requestListener()).onNext(RequestBuilder.Transaction.open("randomID", Transaction.Type.WRITE));
        }
    }

    @Test
    public void whenClosingAGraknRemoteTx_SendCompletedMessageToGrpc() {
        try (Transaction ignored = session.transaction(Transaction.Type.WRITE)) {
            verify(server.requestListener(), never()).onCompleted(); // Make sure transaction is still open here
        }

        verify(server.requestListener()).onCompleted();
    }

    @Test
    public void whenCreatingAGraknRemoteTxWithSession_SetKeyspaceOnTx() {
        try (GraknClient.Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            assertEquals(session.keyspace(), tx.session().keyspace());
        }
    }

    @Test
    public void whenCreatingAGraknRemoteTxWithSession_SetTxTypeOnTx() {
        try (GraknClient.Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            assertEquals(Transaction.Type.WRITE, tx.type());
        }
    }

    @Test(timeout = 5_000)
    public void whenStreamingAQueryWithInfiniteAnswers_Terminate() {
        SessionProto.Transaction.Res queryIterator = SessionProto.Transaction.Res.newBuilder()
                .setQueryIter(SessionProto.Transaction.Query.Iter.newBuilder().setId(ITERATOR))
                .build();

        GraqlGet query = match(var("x").sub("thing")).get();
        String queryString = query.toString();
        ConceptProto.Concept v123 = ConceptProto.Concept.newBuilder().setId(V123).build();
        SessionProto.Transaction.Res iteratorNext = SessionProto.Transaction.Res.newBuilder()
                .setIterateRes(SessionProto.Transaction.Iter.Res.newBuilder()
                                       .setQueryIterRes(SessionProto.Transaction.Query.Iter.Res.newBuilder()
                                                                .setAnswer(AnswerProto.Answer.newBuilder()
                                                                                   .setConceptMap(AnswerProto.ConceptMap.newBuilder().putMap("x", v123))))).build();

        server.setResponse(RequestBuilder.Transaction.query(query), queryIterator);
        server.setResponse(RequestBuilder.Transaction.iterate(ITERATOR), iteratorNext);

        List<ConceptMap> answers;
        int numAnswers = 10;

        try (GraknClient.Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            verify(server.requestListener()).onNext(any()); // The open request
            answers = tx.stream(Graql.parse(queryString).asGet()).limit(numAnswers).collect(toList());
        }

        assertEquals(10, answers.size());

        for (ConceptMap answer : answers) {
            assertEquals(answer.vars(), Collections.singleton(new Variable("x")));
            assertEquals(ConceptId.of("V123"), answer.get("x").id());
        }
    }

    @Test
    public void whenCommitting_SendACommitMessageToGrpc() {
        try (GraknClient.Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            verify(server.requestListener()).onNext(any()); // The open request

            tx.commit();
        }

        verify(server.requestListener()).onNext(RequestBuilder.Transaction.commit());
    }

    @Test
    public void whenCreatingAGraknRemoteTxWithKeyspace_SetsKeyspaceOnTx() {
        try (GraknClient.Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            assertEquals(KEYSPACE, tx.keyspace());
        }
    }

    @Test
    public void whenOpeningATxFails_Throw() {
        SessionProto.Transaction.Req openRequest = RequestBuilder.Transaction.open("randomID", Transaction.Type.WRITE);
        GraknException expectedException = GraknServerException.create("well something went wrong");
        throwOn(openRequest, expectedException);

        exception.expect(RuntimeException.class);
        exception.expectMessage(expectedException.getName());
        exception.expectMessage(expectedException.getMessage());

        GraknClient.Transaction tx = session.transaction(Transaction.Type.WRITE);
        tx.close();
    }

    @Test
    public void whenCommittingATxFails_Throw() {
        GraknException expectedException = InvalidKBException.create("do it better next time");
        throwOn(RequestBuilder.Transaction.commit(), expectedException);

        try (GraknClient.Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            exception.expect(RuntimeException.class);
            exception.expectMessage(expectedException.getName());
            exception.expectMessage(expectedException.getMessage());
            tx.commit();
        }
    }

    @SuppressWarnings("CheckReturnValue")
    @Test
    public void whenAnErrorOccurs_TheTxCloses() {
        GraqlGet query = match(var("x").isa("thing")).get();

        SessionProto.Transaction.Req execQueryRequest = RequestBuilder.Transaction.query(query);
        GraknException expectedException = GraqlQueryException.create("well something went wrong.");
        throwOn(execQueryRequest, expectedException);

        try (GraknClient.Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            try {
                tx.execute(Graql.match(var("x").isa("thing")).get());
            } catch (RuntimeException e) {
                System.out.println(e.getMessage());
                assertTrue(e.getMessage().contains(expectedException.getName()));
            }

            assertTrue(tx.isClosed());
        }
    }

    @SuppressWarnings("CheckReturnValue")
    @Test
    public void whenAnErrorOccurs_AllFutureActionsThrow() {
        GraqlGet query = match(var("x").isa("thing")).get();

        SessionProto.Transaction.Req execQueryRequest = RequestBuilder.Transaction.query(query);
        GraknException expectedException = GraqlQueryException.create("well something went wrong.");
        throwOn(execQueryRequest, expectedException);

        try (GraknClient.Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            try {
                tx.execute(Graql.match(var("x").isa("thing")).get());
            } catch (RuntimeException e) {
                System.out.println(e.getMessage());
                assertTrue(e.getMessage().contains(expectedException.getName()));
            }

            exception.expect(TransactionException.class);
            exception.expectMessage(TransactionException.transactionClosed(null, "The gRPC connection closed").getMessage());
            tx.getMetaConcept();
        }
    }

    @Test
    public void whenPuttingEntityType_EnsureCorrectRequestIsSent() {
        ConceptId id = ConceptId.of(V123);
        Label label = Label.of("foo");

        try (GraknClient.Transaction tx = session.transaction(Transaction.Type.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            ConceptProto.Concept protoConcept = ConceptProto.Concept.newBuilder()
                    .setId(id.getValue()).setBaseType(ConceptProto.Concept.BASE_TYPE.ENTITY_TYPE).build();

            SessionProto.Transaction.Res response = SessionProto.Transaction.Res.newBuilder().setPutEntityTypeRes(SessionProto.Transaction.PutEntityType.Res.newBuilder()
                                                                                                                          .setEntityType(protoConcept)).build();
            server.setResponse(RequestBuilder.Transaction.putEntityType(label), response);

            assertEquals(RemoteConcept.of(protoConcept, tx), tx.putEntityType(label));
        }
    }

    @Test
    public void whenPuttingRelationType_EnsureCorrectRequestIsSent() {
        ConceptId id = ConceptId.of(V123);
        Label label = Label.of("foo");

        try (GraknClient.Transaction tx = session.transaction(Transaction.Type.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            ConceptProto.Concept protoConcept = ConceptProto.Concept.newBuilder()
                    .setId(id.getValue()).setBaseType(ConceptProto.Concept.BASE_TYPE.RELATION_TYPE).build();

            SessionProto.Transaction.Res response = SessionProto.Transaction.Res.newBuilder()
                    .setPutRelationTypeRes(SessionProto.Transaction.PutRelationType.Res.newBuilder()
                                                   .setRelationType(protoConcept)).build();
            server.setResponse(RequestBuilder.Transaction.putRelationType(label), response);

            assertEquals(RemoteConcept.of(protoConcept, tx), tx.putRelationType(label));
        }
    }

    @Test
    public void whenPuttingAttributeType_EnsureCorrectRequestIsSent() {
        ConceptId id = ConceptId.of(V123);
        Label label = Label.of("foo");
        AttributeType.DataType<?> dataType = AttributeType.DataType.STRING;

        try (GraknClient.Transaction tx = session.transaction(Transaction.Type.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            ConceptProto.Concept protoConcept = ConceptProto.Concept.newBuilder()
                    .setId(id.getValue()).setBaseType(ConceptProto.Concept.BASE_TYPE.ATTRIBUTE_TYPE).build();

            SessionProto.Transaction.Res response = SessionProto.Transaction.Res.newBuilder()
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

        try (GraknClient.Transaction tx = session.transaction(Transaction.Type.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            ConceptProto.Concept protoConcept = ConceptProto.Concept.newBuilder()
                    .setId(id.getValue()).setBaseType(ConceptProto.Concept.BASE_TYPE.ROLE).build();

            SessionProto.Transaction.Res response = SessionProto.Transaction.Res.newBuilder()
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

        try (GraknClient.Transaction tx = session.transaction(Transaction.Type.READ)) {
            verify(server.requestListener()).onNext(any()); // The open request

            ConceptProto.Concept protoConcept = ConceptProto.Concept.newBuilder()
                    .setId(id.getValue()).setBaseType(ConceptProto.Concept.BASE_TYPE.RULE).build();

            SessionProto.Transaction.Res response = SessionProto.Transaction.Res.newBuilder()
                    .setPutRuleRes(SessionProto.Transaction.PutRule.Res.newBuilder()
                                           .setRule(protoConcept)).build();
            server.setResponse(RequestBuilder.Transaction.putRule(label, when, then), response);

            assertEquals(RemoteConcept.of(protoConcept, tx), tx.putRule(label, when, then));
        }
    }

    @Test
    public void whenGettingConceptViaID_EnsureCorrectRequestIsSent() {
        ConceptId id = ConceptId.of(V123);

        try (GraknClient.Transaction tx = session.transaction(Transaction.Type.READ)) {
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

        try (GraknClient.Transaction tx = session.transaction(Transaction.Type.READ)) {
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

        try (GraknClient.Transaction tx = session.transaction(Transaction.Type.READ)) {
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

        try (GraknClient.Transaction tx = session.transaction(Transaction.Type.READ)) {
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

        try (GraknClient.Transaction tx = session.transaction(Transaction.Type.READ)) {
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
        assertTransactionClosedAfterAction(Transaction::close);
    }

    @Test
    public void whenCommittingTheTransaction_EnsureItIsFlaggedAsClosed() {
        assertTransactionClosedAfterAction(Transaction::commit);
    }

    @Test
    public void whenAbortingTheTransaction_EnsureItIsFlaggedAsClosed() {
        assertTransactionClosedAfterAction(Transaction::abort);
    }

    private void assertTransactionClosedAfterAction(Consumer<Transaction> action) {
        GraknClient.Transaction tx = session.transaction(Transaction.Type.WRITE);
        assertFalse(tx.isClosed());
        action.accept(tx);
        assertTrue(tx.isClosed());
    }

    private void throwOn(SessionProto.Transaction.Req request, GraknException e) {
        StatusRuntimeException exception;

        if (e instanceof TemporaryWriteException) {
            exception = error(Status.RESOURCE_EXHAUSTED, e);
        } else if (e instanceof GraknServerException) {
            exception = error(Status.INTERNAL, e);
        } else if (e instanceof PropertyNotUniqueException) {
            exception = error(Status.ALREADY_EXISTS, e);
        } else if (e instanceof TransactionException || e instanceof GraqlQueryException || e instanceof InvalidKBException) {
            exception = error(Status.INVALID_ARGUMENT, e);
        } else {
            exception = error(Status.UNKNOWN, e);
        }

        server.setResponse(request, exception);
    }
}
