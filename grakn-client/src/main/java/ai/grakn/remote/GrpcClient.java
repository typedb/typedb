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
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Query;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.grpc.GrpcUtil;
import ai.grakn.grpc.GrpcUtil.ErrorType;
import ai.grakn.grpc.TxGrpcCommunicator;
import ai.grakn.grpc.TxGrpcCommunicator.Response;
import ai.grakn.remote.concept.RemoteConcepts;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.rpc.generated.GraknOuterClass;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;
import ai.grakn.util.CommonUtil;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import mjson.Json;

import javax.annotation.Nullable;
import java.util.Iterator;

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
final class GrpcClient implements AutoCloseable {

    private final TxGrpcCommunicator communicator;

    private GrpcClient(TxGrpcCommunicator communicator) {
        this.communicator = communicator;
    }

    public static GrpcClient create(GraknGrpc.GraknStub stub) {
        TxGrpcCommunicator observer = TxGrpcCommunicator.create(stub);
        return new GrpcClient(observer);
    }

    public void open(Keyspace keyspace, GraknTxType txType) {
        communicator.send(GrpcUtil.openRequest(keyspace, txType));
        responseOrThrow();
    }

    public Iterator<Object> execQuery(RemoteGraknTx tx, Query<?> query, @Nullable Boolean infer) {
        communicator.send(GrpcUtil.execQueryRequest(query.toString(), infer));

        return new AbstractIterator<Object>() {
            private boolean firstElem = true;

            @Override
            protected Object computeNext() {
                if (firstElem) {
                    firstElem = false;
                } else {
                    communicator.send(GrpcUtil.nextRequest());
                }

                TxResponse response = responseOrThrow();

                switch (response.getResponseCase()) {
                    case QUERYRESULT:
                        return convert(tx, response.getQueryResult());
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

    private static Object convert(RemoteGraknTx tx, GraknOuterClass.QueryResult queryResult) {
        switch (queryResult.getQueryResultCase()) {
            case ANSWER:
                return convert(tx, queryResult.getAnswer());
            case OTHERRESULT:
                return Json.read(queryResult.getOtherResult()).getValue();
            default:
            case QUERYRESULT_NOT_SET:
                throw new IllegalArgumentException("Unexpected " + queryResult);
        }
    }

    private static Answer convert(RemoteGraknTx tx, GraknOuterClass.Answer answer) {
        ImmutableMap.Builder<Var, Concept> map = ImmutableMap.builder();

        answer.getAnswerMap().forEach((grpcVar, grpcConcept) -> {
            map.put(Graql.var(grpcVar), convert(tx, grpcConcept));
        });

        return new QueryAnswer(map.build());
    }

    private static Concept convert(RemoteGraknTx tx, GraknOuterClass.Concept concept) {
        ConceptId id = ConceptId.of(concept.getId().getValue());

        switch (concept.getBaseType()) {
            case Entity:
                return RemoteConcepts.createEntity(tx, id);
            case Relationship:
                return RemoteConcepts.createRelationship(tx, id);
            case Attribute:
                return RemoteConcepts.createAttribute(tx, id);
            case EntityType:
                return RemoteConcepts.createEntityType(tx, id);
            case RelationshipType:
                return RemoteConcepts.createRelationshipType(tx, id);
            case AttributeType:
                return RemoteConcepts.createAttributeType(tx, id);
            case Role:
                return RemoteConcepts.createRole(tx, id);
            case Rule:
                return RemoteConcepts.createRule(tx, id);
            case MetaType:
                return RemoteConcepts.createMetaType(tx, id);
            default:
            case UNRECOGNIZED:
                throw new IllegalArgumentException("Unrecognised " + concept);
        }
    }
}
