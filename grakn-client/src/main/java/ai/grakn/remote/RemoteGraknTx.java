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
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.internal.query.QueryBuilderImpl;
import ai.grakn.kb.admin.GraknAdmin;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.util.Schema;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.Schema.MetaSchema.ATTRIBUTE;
import static ai.grakn.util.Schema.MetaSchema.ENTITY;
import static ai.grakn.util.Schema.MetaSchema.RELATIONSHIP;
import static ai.grakn.util.Schema.MetaSchema.ROLE;
import static ai.grakn.util.Schema.MetaSchema.RULE;

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

    private final GraknSession session;
    private final GraknTxType txType;
    private final GrpcClient client;

    private RemoteGraknTx(GraknSession session, GraknTxType txType, GrpcClient client) {
        this.session = session;
        this.txType = txType;
        this.client = client;
    }

    static RemoteGraknTx create(RemoteGraknSession session, GraknTxType txType) {
        GraknGrpc.GraknStub stub = session.stub();
        GrpcClient client = GrpcClient.create(stub);
        client.open(session.keyspace(), txType);
        return new RemoteGraknTx(session, txType, client);
    }

    public GrpcClient client() {
        return client;
    }

    @Override
    public EntityType putEntityType(Label label) {
        return putSchemaConcept(label, ENTITY);
    }

    @Override
    public <V> AttributeType<V> putAttributeType(Label label, AttributeType.DataType<V> dataType) {
        return putSchemaConcept(label, ATTRIBUTE, var -> var.datatype(dataType));
    }

    @Override
    public Rule putRule(Label label, Pattern when, Pattern then) {
        return putSchemaConcept(label, RULE, var -> var.when(when).then(then));
    }

    @Override
    public RelationshipType putRelationshipType(Label label) {
        return putSchemaConcept(label, RELATIONSHIP);
    }

    @Override
    public Role putRole(Label label) {
        return putSchemaConcept(label, ROLE);
    }

    private <X extends Concept> X putSchemaConcept(Label label, Schema.MetaSchema meta){
        return putSchemaConcept(label, meta, null);
    }

    private <X extends Concept> X putSchemaConcept(Label label, Schema.MetaSchema meta, @Nullable Function<VarPattern, VarPattern> extender){
        VarPattern var = var().label(label).sub(var().label(meta.getLabel()));
        if(extender != null) var = extender.apply(var);
        queryRunner().run(Graql.define(var));
        return null;
    }

    @Nullable
    @Override
    public <T extends Concept> T getConcept(ConceptId id) {
        throw new UnsupportedOperationException(); // TODO
    }

    @Nullable
    @Override
    public <T extends SchemaConcept> T getSchemaConcept(Label label) {
        throw new UnsupportedOperationException(); // TODO
    }

    @Nullable
    @Override
    public <T extends Type> T getType(Label label) {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public <V> Collection<Attribute<V>> getAttributesByValue(V value) {
        throw new UnsupportedOperationException(); // TODO
    }

    @Nullable
    @Override
    public EntityType getEntityType(String label) {
        throw new UnsupportedOperationException(); // TODO
    }

    @Nullable
    @Override
    public RelationshipType getRelationshipType(String label) {
        throw new UnsupportedOperationException(); // TODO
    }

    @Nullable
    @Override
    public <V> AttributeType<V> getAttributeType(String label) {
        throw new UnsupportedOperationException(); // TODO
    }

    @Nullable
    @Override
    public Role getRole(String label) {
        throw new UnsupportedOperationException(); // TODO
    }

    @Nullable
    @Override
    public Rule getRule(String label) {
        throw new UnsupportedOperationException(); // TODO
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
    public Type getMetaConcept() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public RelationshipType getMetaRelationType() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public Role getMetaRole() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public AttributeType getMetaAttributeType() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public EntityType getMetaEntityType() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public Rule getMetaRule() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public Stream<SchemaConcept> sups(SchemaConcept schemaConcept) {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public void delete() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public QueryRunner queryRunner() {
        return RemoteQueryRunner.create(this, client, null);
    }
}
