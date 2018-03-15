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

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.grpc.ConceptMethod;
import ai.grakn.grpc.GrpcConceptConverter;
import ai.grakn.grpc.GrpcUtil;
import ai.grakn.grpc.GrpcUtil.ErrorType;
import ai.grakn.grpc.TxGrpcCommunicator;
import ai.grakn.grpc.TxGrpcCommunicator.Response;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.rpc.generated.GrpcGrakn;
import ai.grakn.rpc.generated.GrpcGrakn.IteratorId;
import ai.grakn.rpc.generated.GrpcGrakn.TxResponse;
import ai.grakn.util.CommonUtil;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import mjson.Json;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Communicates with a Grakn gRPC server, translating requests and responses to and from their gRPC representations.
 *
 * <p>
 *     This class is a light abstraction layer over gRPC - it understands how the sequence of calls should execute and
 *     how to translate gRPC objects into Java objects and back. However, any logic is kept in {@link RemoteGraknTx}.
 * </p>
 *
 * @author Felix Chapman
 */
public final class GrpcClient implements AutoCloseable {

    private final GrpcConceptConverter conceptConverter;
    private final TxGrpcCommunicator communicator;

    private GrpcClient(GrpcConceptConverter conceptConverter, TxGrpcCommunicator communicator) {
        this.conceptConverter = conceptConverter;
        this.communicator = communicator;
    }

    public static GrpcClient create(GrpcConceptConverter conceptConverter, GraknGrpc.GraknStub stub) {
        TxGrpcCommunicator observer = TxGrpcCommunicator.create(stub);
        return new GrpcClient(conceptConverter, observer);
    }

    public void open(Keyspace keyspace, GraknTxType txType) {
        communicator.send(GrpcUtil.openRequest(keyspace, txType));
        responseOrThrow();
    }

    public Iterator<Object> execQuery(RemoteGraknTx tx, Query<?> query) {
        communicator.send(GrpcUtil.execQueryRequest(query.toString(), query.inferring()));

        IteratorId iteratorId = responseOrThrow().getIteratorId();

        return new AbstractIterator<Object>() {
            @Override
            protected Object computeNext() {
                communicator.send(GrpcUtil.nextRequest(iteratorId));

                TxResponse response = responseOrThrow();

                switch (response.getResponseCase()) {
                    case QUERYRESULT:
                        return convert(response.getQueryResult());
                    case DONE:
                        return endOfData();
                    default:
                    case RESPONSE_NOT_SET:
                        throw CommonUtil.unreachableStatement("Unexpected " + response);
                }
            }
        };
    }

    public void commit() {
        communicator.send(GrpcUtil.commitRequest());
        responseOrThrow();
    }

    @Nullable
    public <T> T runConceptMethod(ConceptId id, ConceptMethod<T> conceptMethod) {
        communicator.send(GrpcUtil.runConceptMethodRequest(id, conceptMethod));
        return conceptMethod.get(conceptConverter, responseOrThrow());
    }

    public Optional<Concept> getConcept(ConceptId id) {
        communicator.send(GrpcUtil.getConceptRequest(id));
        return conceptConverter.convert(responseOrThrow().getOptionalConcept());
    }

    public Optional<Concept> getSchemaConcept(Label label) {
        communicator.send(GrpcUtil.getSchemaConceptRequest(label));
        return conceptConverter.convert(responseOrThrow().getOptionalConcept());
    }

    public Stream<? extends Concept> getAttributesByValue(Object value) {
        communicator.send(GrpcUtil.getAttributesByValueRequest(value));
        return GrpcUtil.convert(conceptConverter, responseOrThrow().getConcepts());
    }

    public Concept putEntityType(Label label) {
        communicator.send(GrpcUtil.putEntityTypeRequest(label));
        return conceptConverter.convert(responseOrThrow().getConcept());
    }

    public Concept putRelationshipType(Label label) {
        communicator.send(GrpcUtil.putRelationshipTypeRequest(label));
        return conceptConverter.convert(responseOrThrow().getConcept());
    }

    public Concept putAttributeType(Label label, AttributeType.DataType<?> dataType) {
        communicator.send(GrpcUtil.putAttributeTypeRequest(label, dataType));
        return conceptConverter.convert(responseOrThrow().getConcept());
    }

    public Concept putRole(Label label) {
        communicator.send(GrpcUtil.putRoleRequest(label));
        return conceptConverter.convert(responseOrThrow().getConcept());
    }

    public Concept putRule(Label label, Pattern when, Pattern then) {
        communicator.send(GrpcUtil.putRuleRequest(label, when, then));
        return conceptConverter.convert(responseOrThrow().getConcept());
    }

    @Override
    public void close() {
        communicator.close();
    }

    public boolean isClosed(){
        return communicator.isClosed();
    }

    private TxResponse responseOrThrow() {
        Response response;

        try {
            response = communicator.receive();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // This is called from classes like RemoteGraknTx, that impl methods which do not throw InterruptedException
            // Therefore, we have to wrap it in a RuntimeException.
            throw new RuntimeException(e);
        }

        switch (response.type()) {
            case OK:
                return response.ok();
            case ERROR:
                throw convertStatusRuntimeException(response.error());
            case COMPLETED:
            default:
                throw CommonUtil.unreachableStatement("Unexpected response " + response);
        }
    }

    private static RuntimeException convertStatusRuntimeException(StatusRuntimeException error) {
        Status status = error.getStatus();
        Metadata trailers = error.getTrailers();

        ErrorType errorType = trailers.get(ErrorType.KEY);

        if (errorType != null) {
            String message = status.getDescription();
            return errorType.toException(message);
        } else {
            return error;
        }
    }

    private Object convert(GrpcGrakn.QueryResult queryResult) {
        switch (queryResult.getQueryResultCase()) {
            case ANSWER:
                return convert(queryResult.getAnswer());
            case OTHERRESULT:
                return Json.read(queryResult.getOtherResult()).getValue();
            default:
            case QUERYRESULT_NOT_SET:
                throw new IllegalArgumentException("Unexpected " + queryResult);
        }
    }

    private Answer convert(GrpcGrakn.Answer answer) {
        ImmutableMap.Builder<Var, Concept> map = ImmutableMap.builder();

        answer.getAnswerMap().forEach((grpcVar, grpcConcept) -> {
            map.put(Graql.var(grpcVar), conceptConverter.convert(grpcConcept));
        });

        return new QueryAnswer(map.build());
    }
}
