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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.remote;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.QueryRunner;
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
import ai.grakn.grpc.ConceptMethods;
import ai.grakn.grpc.GrpcClient;
import ai.grakn.grpc.GrpcUtil;
import ai.grakn.kb.admin.GraknAdmin;
import ai.grakn.remote.concept.RemoteConcepts;
import ai.grakn.rpc.generated.GraknGrpc.GraknStub;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcGrakn.DeleteRequest;
import ai.grakn.rpc.generated.GrpcGrakn.TxRequest;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

import static ai.grakn.util.CommonUtil.toImmutableSet;

/**
 * Remote implementation of {@link GraknTx} and {@link GraknAdmin} that communicates with a Grakn server using gRPC.
 *
 * <p>
 *     Most of the gRPC legwork is handled in the embedded {@link GrpcClient}. This class is an adapter to that,
 *     translating Java calls into gRPC messages.
 * </p>
 *
 * @author Felix Chapman
 */
public final class RemoteGraknTx implements GraknTx, GraknAdmin {

    private final RemoteGraknSession session;
    private final GraknTxType txType;
    private final GrpcClient client;

    private RemoteGraknTx(RemoteGraknSession session, GraknTxType txType, TxRequest openRequest, GraknStub stub) {
        this.session = session;
        this.txType = txType;
        this.client = GrpcClient.create(this::convert, stub);
        client.open(openRequest);
    }

    // TODO: ideally the transaction should not hold a reference to the session or at least depend on a session interface
    public static RemoteGraknTx create(RemoteGraknSession session, TxRequest openRequest) {
        GraknStub stub = session.stub();
        return new RemoteGraknTx(session, GrpcUtil.convert(openRequest.getOpen().getTxType()), openRequest, stub);
    }

    public GrpcClient client() {
        return client;
    }

    @Override
    public EntityType putEntityType(Label label) {
        return client().putEntityType(label).asEntityType();
    }

    @Override
    public <V> AttributeType<V> putAttributeType(Label label, AttributeType.DataType<V> dataType) {
        return client().putAttributeType(label, dataType).asAttributeType();
    }

    @Override
    public Rule putRule(Label label, Pattern when, Pattern then) {
        return client().putRule(label, when, then).asRule();
    }

    @Override
    public RelationshipType putRelationshipType(Label label) {
        return client().putRelationshipType(label).asRelationshipType();
    }

    @Override
    public Role putRole(Label label) {
        return client().putRole(label).asRole();
    }

    @Nullable
    @Override
    public <T extends Concept> T getConcept(ConceptId id) {
        return (T) client().getConcept(id).orElse(null);
    }

    @Nullable
    @Override
    public <T extends SchemaConcept> T getSchemaConcept(Label label) {
        return (T) client().getSchemaConcept(label).orElse(null);
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
        return client.isClosed();
    }

    @Override
    public QueryBuilder graql() {
        return new QueryBuilderImpl(this);
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public void commit() throws InvalidKBException {
        client.commit();
        close();
    }

    @Override
    public Stream<SchemaConcept> sups(SchemaConcept schemaConcept) {
        Stream<? extends Concept> sups = client.runConceptMethod(schemaConcept.getId(), ConceptMethods.GET_SUPER_CONCEPTS);
        return Objects.requireNonNull(sups).map(Concept::asSchemaConcept);
    }

    @Override
    public void delete() {
        DeleteRequest request = GrpcUtil.deleteRequest(GrpcUtil.openRequest(keyspace(), GraknTxType.WRITE).getOpen());
        session.blockingStub().delete(request);
        close();
    }

    @Override
    public QueryRunner queryRunner() {
        return RemoteQueryRunner.create(client);
    }

    private Concept convert(GrpcConcept.Concept concept) {
        ConceptId id = ConceptId.of(concept.getId().getValue());

        switch (concept.getBaseType()) {
            case Entity:
                return RemoteConcepts.createEntity(this, id);
            case Relationship:
                return RemoteConcepts.createRelationship(this, id);
            case Attribute:
                return RemoteConcepts.createAttribute(this, id);
            case EntityType:
                return RemoteConcepts.createEntityType(this, id);
            case RelationshipType:
                return RemoteConcepts.createRelationshipType(this, id);
            case AttributeType:
                return RemoteConcepts.createAttributeType(this, id);
            case Role:
                return RemoteConcepts.createRole(this, id);
            case Rule:
                return RemoteConcepts.createRule(this, id);
            case MetaType:
                return RemoteConcepts.createMetaType(this, id);
            default:
            case UNRECOGNIZED:
                throw new IllegalArgumentException("Unrecognised " + concept);
        }
    }
}
