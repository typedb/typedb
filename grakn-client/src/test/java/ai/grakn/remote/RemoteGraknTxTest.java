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
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.graql.DefineQuery;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Answer;
import ai.grakn.grpc.GrpcUtil;
import ai.grakn.grpc.GrpcUtil.ErrorType;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.rpc.generated.GraknOuterClass;
import ai.grakn.rpc.generated.GraknOuterClass.QueryResult;
import ai.grakn.rpc.generated.GraknOuterClass.TxRequest;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static ai.grakn.graql.Graql.define;
import static ai.grakn.graql.Graql.var;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Felix Chapman
 */
public class RemoteGraknTxTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Rule
    public final GrpcServerMock server = new GrpcServerMock();

    private final RemoteGraknSession session = mock(RemoteGraknSession.class);

    private static final Keyspace KEYSPACE = Keyspace.of("blahblah");
    private static final GraknOuterClass.ConceptId V123 =
            GraknOuterClass.ConceptId.newBuilder().setValue("V123").build();

    @Before
    public void setUp() {
        when(session.stub()).thenReturn(GraknGrpc.newStub(server.channel()));
        when(session.keyspace()).thenReturn(KEYSPACE);
    }

    @Test
    public void whenCreatingAGraknRemoteTx_MakeATxCallToGrpc() {
        try (GraknTx ignored = RemoteGraknTx.create(session, GraknTxType.WRITE)) {
            verify(server.service()).tx(any());
        }
    }

    @Test
    public void whenCreatingAGraknRemoteTx_SendAnOpenMessageToGrpc() {
        try (GraknTx ignored = RemoteGraknTx.create(session, GraknTxType.WRITE)) {
            verify(server.requests()).onNext(GrpcUtil.openRequest(Keyspace.of(KEYSPACE.getValue()), GraknTxType.WRITE));
        }
    }

    @Test
    public void whenCreatingABatchGraknRemoteTx_SendAnOpenMessageWithBatchSpecifiedToGrpc() {
        try (GraknTx ignored = RemoteGraknTx.create(session, GraknTxType.BATCH)) {
            verify(server.requests()).onNext(GrpcUtil.openRequest(Keyspace.of(KEYSPACE.getValue()), GraknTxType.BATCH));
        }
    }

    @Test
    public void whenClosingAGraknRemoteTx_SendCompletedMessageToGrpc() {
        try (GraknTx ignored = RemoteGraknTx.create(session, GraknTxType.WRITE)) {
            verify(server.requests(), never()).onCompleted(); // Make sure transaction is still open here
        }

        verify(server.requests()).onCompleted();
    }

    @Test
    public void whenCreatingAGraknRemoteTxWithSession_SetKeyspaceOnTx() {
        try (GraknTx tx = RemoteGraknTx.create(session, GraknTxType.WRITE)) {
            assertEquals(session, tx.session());
        }
    }

    @Test
    public void whenCreatingAGraknRemoteTxWithSession_SetTxTypeOnTx() {
        try (GraknTx tx = RemoteGraknTx.create(session, GraknTxType.BATCH)) {
            assertEquals(GraknTxType.BATCH, tx.txType());
        }
    }

    @Test
    public void whenExecutingAQuery_SendAnExecQueryMessageToGrpc() {
        String queryString = "match $x isa person; get $x;";

        try (GraknTx tx = RemoteGraknTx.create(session, GraknTxType.WRITE)) {
            verify(server.requests()).onNext(any()); // The open request

            tx.graql().parse(queryString).execute();
        }

        verify(server.requests()).onNext(GrpcUtil.execQueryRequest(queryString));
    }

    @Test
    public void whenExecutingAQuery_GetAResultBack() {
        String queryString = "match $x isa person; get $x;";

        GraknOuterClass.Concept v123 = GraknOuterClass.Concept.newBuilder().setId(V123).build();
        GraknOuterClass.Answer grpcAnswer = GraknOuterClass.Answer.newBuilder().putAnswer("x", v123).build();
        QueryResult queryResult = QueryResult.newBuilder().setAnswer(grpcAnswer).build();
        TxResponse response = TxResponse.newBuilder().setQueryResult(queryResult).build();

        server.setResponse(GrpcUtil.execQueryRequest(queryString), response);
        server.setResponse(GrpcUtil.nextRequest(), GrpcUtil.doneResponse());

        List<Answer> results;

        try (GraknTx tx = RemoteGraknTx.create(session, GraknTxType.WRITE)) {
            verify(server.requests()).onNext(any()); // The open request
            results = tx.graql().<GetQuery>parse(queryString).execute();
        }

        Answer answer = Iterables.getOnlyElement(results);
        assertEquals(answer.vars(), ImmutableSet.of(var("x")));
        assertEquals(ConceptId.of("V123"), answer.get(var("x")).getId());
    }

    @Test
    public void whenExecutingAQueryWithAVoidResult_GetANullBack() {
        String queryString = "match $x isa person; delete $x;";

        server.setResponse(GrpcUtil.execQueryRequest(queryString), GrpcUtil.doneResponse());

        try (GraknTx tx = RemoteGraknTx.create(session, GraknTxType.WRITE)) {
            verify(server.requests()).onNext(any()); // The open request
            assertNull(tx.graql().parse(queryString).execute());
        }
    }

    @Test
    public void whenExecutingAQueryWithABooleanResult_GetABoolBack() {
        String queryString = "match $x isa person; aggregate ask;";

        server.setResponse(
                GrpcUtil.execQueryRequest(queryString),
                TxResponse.newBuilder().setQueryResult(QueryResult.newBuilder().setOtherResult("true")).build()
        );

        server.setResponse(GrpcUtil.nextRequest(), GrpcUtil.doneResponse());

        try (GraknTx tx = RemoteGraknTx.create(session, GraknTxType.WRITE)) {
            verify(server.requests()).onNext(any()); // The open request
            assertTrue(tx.graql().<Query<Boolean>>parse(queryString).execute());
        }
    }

    @Test
    public void whenExecutingAQueryWithASingleAnswer_GetAnAnswerBack() {
        String queryString = "define label person sub entity;";

        GraknOuterClass.Concept v123 = GraknOuterClass.Concept.newBuilder().setId(V123).build();
        GraknOuterClass.Answer grpcAnswer = GraknOuterClass.Answer.newBuilder().putAnswer("x", v123).build();
        QueryResult queryResult = QueryResult.newBuilder().setAnswer(grpcAnswer).build();
        TxResponse response = TxResponse.newBuilder().setQueryResult(queryResult).build();

        server.setResponse(GrpcUtil.execQueryRequest(queryString), response);
        server.setResponse(GrpcUtil.nextRequest(), GrpcUtil.doneResponse());

        Answer answer;

        try (GraknTx tx = RemoteGraknTx.create(session, GraknTxType.WRITE)) {
            verify(server.requests()).onNext(any()); // The open request
            answer = tx.graql().<DefineQuery>parse(queryString).execute();
        }

        assertEquals(answer.vars(), ImmutableSet.of(var("x")));
        assertEquals(ConceptId.of("V123"), answer.get(var("x")).getId());
    }

    @Test(timeout = 5_000)
    public void whenStreamingAQueryWithInfiniteAnswers_Terminate() {
        String queryString = "match $x sub thing; get $x;";

        GraknOuterClass.Concept v123 = GraknOuterClass.Concept.newBuilder().setId(V123).build();
        GraknOuterClass.Answer grpcAnswer = GraknOuterClass.Answer.newBuilder().putAnswer("x", v123).build();
        QueryResult queryResult = QueryResult.newBuilder().setAnswer(grpcAnswer).build();
        TxResponse response = TxResponse.newBuilder().setQueryResult(queryResult).build();

        server.setResponse(GrpcUtil.execQueryRequest(queryString), response);
        server.setResponse(GrpcUtil.nextRequest(), response);

        List<Answer> answers;
        int numAnswers = 10;

        try (GraknTx tx = RemoteGraknTx.create(session, GraknTxType.WRITE)) {
            verify(server.requests()).onNext(any()); // The open request
            answers = tx.graql().<GetQuery>parse(queryString).stream().limit(numAnswers).collect(toList());
        }

        assertEquals(10, answers.size());

        for (Answer answer : answers) {
            assertEquals(answer.vars(), ImmutableSet.of(var("x")));
            assertEquals(ConceptId.of("V123"), answer.get(var("x")).getId());
        }
    }

    @Ignore // TODO: dream about supporting this
    @Test
    public void whenExecutingAQueryWithInferenceSet_SendAnExecQueryWithInferenceSetMessageToGrpc() {
        String queryString = "match $x isa person; get $x;";

        try (GraknTx tx = RemoteGraknTx.create(session, GraknTxType.WRITE)) {
            verify(server.requests()).onNext(any()); // The open request

            QueryBuilder graql = tx.graql();

            graql.infer(true).parse(queryString).execute();
            verify(server.requests()).onNext(GrpcUtil.execQueryRequest(queryString, true));

            graql.infer(false).parse(queryString).execute();
            verify(server.requests()).onNext(GrpcUtil.execQueryRequest(queryString, false));
        }
    }

    @Test
    public void whenCommitting_SendACommitMessageToGrpc() {
        try (GraknTx tx = RemoteGraknTx.create(session, GraknTxType.WRITE)) {
            verify(server.requests()).onNext(any()); // The open request

            tx.commit();
        }

        verify(server.requests()).onNext(GrpcUtil.commitRequest());
    }

    @Test
    public void whenCreatingAGraknRemoteTxWithKeyspace_SetsKeyspaceOnTx() {
        try (GraknTx tx = RemoteGraknTx.create(session, GraknTxType.WRITE)) {
            assertEquals(KEYSPACE, tx.keyspace());
        }
    }

    @Test
    public void whenOpeningATxFails_Throw() {
        TxRequest openRequest = GrpcUtil.openRequest(Keyspace.of(KEYSPACE.getValue()), GraknTxType.WRITE);
        throwOn(openRequest, ErrorType.GRAKN_BACKEND_EXCEPTION, "well something went wrong");

        exception.expect(GraknBackendException.class);
        exception.expectMessage("well something went wrong");

        GraknTx tx = RemoteGraknTx.create(session, GraknTxType.WRITE);
        tx.close();
    }

    @Test
    public void whenCommittingATxFails_Throw() {
        throwOn(GrpcUtil.commitRequest(), ErrorType.INVALID_KB_EXCEPTION, "do it better next time");

        try (GraknTx tx = RemoteGraknTx.create(session, GraknTxType.WRITE)) {

            exception.expect(InvalidKBException.class);
            exception.expectMessage("do it better next time");

            tx.commit();
        }
    }

    @Test
    public void whenPuttingEntityType_EnsureCorrectQueryIsSent(){
        assertConceptLabelInsertion("oliver", Schema.MetaSchema.ENTITY, GraknTx::putEntityType, null);
    }

    @Test
    public void whenPuttingRelationType_EnsureCorrectQueryIsSent(){
        assertConceptLabelInsertion("oliver", Schema.MetaSchema.RELATIONSHIP, GraknTx::putRelationshipType, null);
    }

    @Test
    public void whenPuttingAttributeType_EnsureCorrectQueryIsSent(){
        AttributeType.DataType<String> string = AttributeType.DataType.STRING;
        assertConceptLabelInsertion("oliver", Schema.MetaSchema.ATTRIBUTE,
                (tx, label) -> tx.putAttributeType(label, string),
                var -> var.datatype(string));
    }

    @Test
    public void whenPuttingRule_EnsureCorrectQueryIsSent(){
        Pattern when = Graql.parser().parsePattern("$x isa Your-Type");
        Pattern then = Graql.parser().parsePattern("$x isa Your-Other-Type");
        assertConceptLabelInsertion("oliver", Schema.MetaSchema.RULE, (tx, label) -> tx.putRule(label, when, then), var -> var.when(when).then(then));
    }

    @Test
    public void whenPuttingRole_EnsureCorrectQueryIsSent(){
        assertConceptLabelInsertion("oliver", Schema.MetaSchema.ROLE, GraknTx::putRole, null);
    }

    private void assertConceptLabelInsertion(String label, Schema.MetaSchema metaSchema, BiConsumer<GraknTx, Label> adder, @Nullable Function<VarPattern, VarPattern> extender){
        VarPattern var = var().label(label).sub(metaSchema.getLabel().getValue());
        if(extender != null) var = extender.apply(var);
        String expectedQuery = define(var).toString();

        GraknOuterClass.Concept v123 = GraknOuterClass.Concept.newBuilder().setId(V123).build();
        GraknOuterClass.Answer grpcAnswer = GraknOuterClass.Answer.newBuilder().putAnswer("x", v123).build();
        QueryResult queryResult = QueryResult.newBuilder().setAnswer(grpcAnswer).build();
        TxResponse response = TxResponse.newBuilder().setQueryResult(queryResult).build();

        doAnswer(args -> {
            assert server.requests() != null;
            server.responses().onNext(response);
            return null;
        }).when(server.requests()).onNext(GrpcUtil.execQueryRequest(expectedQuery));

        doAnswer(args -> {
            assert server.responses() != null;
            server.responses().onNext(GrpcUtil.doneResponse());
            return null;
        }).when(server.requests()).onNext(GrpcUtil.nextRequest());

        try (GraknTx tx = RemoteGraknTx.create(session, GraknTxType.WRITE)) {
            verify(server.requests()).onNext(any()); // The open request
            adder.accept(tx, Label.of(label));
        }

        verify(server.requests()).onNext(GrpcUtil.execQueryRequest(expectedQuery));
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

    private void assertTransactionClosedAfterAction(Consumer<GraknTx> action){
        GraknTx tx = RemoteGraknTx.create(session, GraknTxType.WRITE);
        assertFalse(tx.isClosed());
        action.accept(tx);
        assertTrue(tx.isClosed());
    }

    private void throwOn(TxRequest request, ErrorType errorType, String message) {
        Metadata trailers = new Metadata();
        trailers.put(ErrorType.KEY, errorType);
        StatusRuntimeException exception = Status.UNKNOWN.withDescription(message).asRuntimeException(trailers);

        server.setResponse(request, responses -> responses.onError(exception));
    }
}
