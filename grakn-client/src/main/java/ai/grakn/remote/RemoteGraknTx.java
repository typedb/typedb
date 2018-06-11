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

package ai.grakn.remote;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.QueryExecutor;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Rule;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.internal.query.QueryBuilderImpl;
import ai.grakn.kb.admin.GraknAdmin;
import ai.grakn.rpc.TxGrpcCommunicator;
import ai.grakn.rpc.generated.GrpcGrakn;
import ai.grakn.rpc.util.ConceptMethod;
import ai.grakn.rpc.GrpcClient;
import ai.grakn.rpc.generated.GraknGrpc.GraknStub;
import ai.grakn.rpc.generated.GrpcGrakn.DeleteRequest;
import ai.grakn.rpc.generated.GrpcGrakn.TxRequest;
import ai.grakn.rpc.util.RequestBuilder;
import ai.grakn.remote.rpc.RemoteConceptReader;
import ai.grakn.rpc.util.ResponseBuilder;
import ai.grakn.rpc.util.TxConceptReader;
import ai.grakn.util.CommonUtil;
import io.grpc.StatusRuntimeException;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

import static ai.grakn.util.CommonUtil.toImmutableSet;

/**
 * Remote implementation of {@link GraknTx} and {@link GraknAdmin} that communicates with a Grakn server using gRPC.
 *
 * Most of the gRPC legwork is handled in the embedded {@link GrpcClient}. This class is an adapter to that,
 * translating Java calls into gRPC messages.
 *
 * @author Felix Chapman
 */
public final class RemoteGraknTx implements GraknTx, GraknAdmin {

    private final RemoteGraknSession session;
    private final GraknTxType txType;
    private final GrpcClient client;
    private final TxGrpcCommunicator communicator;
    private final TxConceptReader conceptReader;

    private RemoteGraknTx(RemoteGraknSession session, GraknTxType txType, TxRequest openRequest, GraknStub stub) {
        this.session = session;
        this.txType = txType;
        this.communicator = TxGrpcCommunicator.create(stub);
        this.conceptReader = new RemoteConceptReader(this);
        this.client = GrpcClient.create(conceptReader, communicator);
        communicator.send(openRequest);
        responseOrThrow();
    }

    // TODO: ideally the transaction should not hold a reference to the session or at least depend on a session interface
    public static RemoteGraknTx create(RemoteGraknSession session, TxRequest openRequest) {
        GraknStub stub = session.stub();
        return new RemoteGraknTx(session, GraknTxType.of(openRequest.getOpen().getTxType().getNumber()), openRequest, stub);
    }


    private GrpcGrakn.TxResponse responseOrThrow() {
        TxGrpcCommunicator.Response response;

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
        ResponseBuilder.ErrorType errorType = error.getTrailers().get(ResponseBuilder.ErrorType.KEY);

        if (errorType != null) return errorType.toException(error.getStatus().getDescription());
        else return error;
    }

    public GrpcClient client() {
        return client;
    }

    public GrpcGrakn.TxResponse runConceptMethod(GrpcGrakn.TxRequest conceptMethodRequest) {
        communicator.send(conceptMethodRequest);
        return responseOrThrow();
    }

    @Override
    public EntityType putEntityType(Label label) {
        communicator.send(RequestBuilder.putEntityType(label));
        return conceptReader.concept(responseOrThrow().getConcept()).asEntityType();
    }

    @Override
    public <V> AttributeType<V> putAttributeType(Label label, AttributeType.DataType<V> dataType) {
        communicator.send(RequestBuilder.putAttributeType(label, dataType));
        return conceptReader.concept(responseOrThrow().getConcept()).asAttributeType();
    }

    @Override
    public Rule putRule(Label label, Pattern when, Pattern then) {
        communicator.send(RequestBuilder.putRule(label, when, then));
        return conceptReader.concept(responseOrThrow().getConcept()).asRule();
    }

    @Override
    public RelationshipType putRelationshipType(Label label) {
        communicator.send(RequestBuilder.putRelationshipType(label));
        return conceptReader.concept(responseOrThrow().getConcept()).asRelationshipType();
    }

    @Override
    public Role putRole(Label label) {
        communicator.send(RequestBuilder.putRole(label));
        return conceptReader.concept(responseOrThrow().getConcept()).asRole();
    }

    @Nullable
    @Override
    public <T extends Concept> T getConcept(ConceptId id) {
        communicator.send(RequestBuilder.getConcept(id));
        return (T) conceptReader.optionalConcept(responseOrThrow().getOptionalConcept()).orElse(null);
    }

    @Nullable
    @Override
    public <T extends SchemaConcept> T getSchemaConcept(Label label) {
        communicator.send(RequestBuilder.getSchemaConcept(label));
        return (T) conceptReader.optionalConcept(responseOrThrow().getOptionalConcept()).orElse(null);
    }

    @Nullable
    @Override
    public <T extends Type> T getType(Label label) {
        return getSchemaConcept(label);
    }

    @Override
    public <V> Collection<Attribute<V>> getAttributesByValue(V value) {
        return client().getAttributesByValue(value).map(Concept::<V>asAttribute).collect(toImmutableSet());
    }

    @Nullable
    @Override
    public EntityType getEntityType(String label) {
        return getSchemaConcept(Label.of(label));
    }

    @Nullable
    @Override
    public RelationshipType getRelationshipType(String label) {
        return getSchemaConcept(Label.of(label));
    }

    @Nullable
    @Override
    public <V> AttributeType<V> getAttributeType(String label) {
        return getSchemaConcept(Label.of(label));
    }

    @Nullable
    @Override
    public Role getRole(String label) {
        return getSchemaConcept(Label.of(label));
    }

    @Nullable
    @Override
    public Rule getRule(String label) {
        return getSchemaConcept(Label.of(label));
    }

    @Override
    public GraknAdmin admin() {
        return this;
    }

    @Override
    public GraknTxType txType() {
        return txType;
    }

    @Override
    public GraknSession session() {
        return session;
    }

    @Override
    public boolean isClosed() {
        return communicator.isClosed();
    }

    @Override
    public QueryBuilder graql() {
        return new QueryBuilderImpl(this);
    }

    @Override
    public void close() {
        communicator.close();
    }

    @Override
    public void commit() throws InvalidKBException {
        communicator.send(RequestBuilder.commit());
        responseOrThrow();
        close();
    }

    @Override
    public Stream<SchemaConcept> sups(SchemaConcept schemaConcept) {
        Stream<? extends Concept> sups = client.runConceptMethod(schemaConcept.getId(), ConceptMethod.GET_SUPER_CONCEPTS);
        return Objects.requireNonNull(sups).map(Concept::asSchemaConcept);
    }

    @Override
    public void delete() {
        DeleteRequest request = RequestBuilder.delete(RequestBuilder.open(keyspace(), GraknTxType.WRITE).getOpen());
        session.blockingStub().delete(request);
        close();
    }

    @Override
    public QueryExecutor queryExecutor() {
        return RemoteQueryExecutor.create(client);
    }
}
