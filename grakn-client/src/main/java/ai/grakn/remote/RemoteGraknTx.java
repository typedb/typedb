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
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.query.QueryBuilderImpl;
import ai.grakn.kb.admin.GraknAdmin;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.util.Schema;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Optional;
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

    private <X extends SchemaConcept> X putSchemaConcept(Label label, Schema.MetaSchema meta){
        return putSchemaConcept(label, meta, null);
    }

    private <X extends SchemaConcept> X putSchemaConcept(Label label, Schema.MetaSchema meta,
                                                   @Nullable Function<VarPattern, VarPattern> extender){
        Var var = var("x");
        VarPattern pattern = var.label(label).sub(var().label(meta.getLabel()));
        if(extender != null) pattern = extender.apply(pattern);
        return (X) queryRunner().run(Graql.define(pattern)).get(var);
    }

    @Nullable
    @Override
    public <T extends Concept> T getConcept(ConceptId id) {
        throw new UnsupportedOperationException(); // TODO
    }

    @Nullable
    @Override
    public <T extends SchemaConcept> T getSchemaConcept(Label label) {
        return getSchemaConcept(label, null);
    }

    @Nullable
    @Override
    public <T extends Type> T getType(Label label) {
        return getSchemaConcept(label);
    }

    @Override
    public <V> Collection<Attribute<V>> getAttributesByValue(V value) {
        throw new UnsupportedOperationException(); // TODO
    }

    @Nullable
    @Override
    public EntityType getEntityType(String label) {
        return getSchemaConcept(Label.of(label), Schema.MetaSchema.ENTITY);
    }

    @Nullable
    @Override
    public RelationshipType getRelationshipType(String label) {
        return getSchemaConcept(Label.of(label), Schema.MetaSchema.RELATIONSHIP);
    }

    @Nullable
    @Override
    public <V> AttributeType<V> getAttributeType(String label) {
        return getSchemaConcept(Label.of(label), Schema.MetaSchema.ATTRIBUTE);
    }

    @Nullable
    @Override
    public Role getRole(String label) {
        return getSchemaConcept(Label.of(label), Schema.MetaSchema.ROLE);
    }

    @Nullable
    @Override
    public Rule getRule(String label) {
        return getSchemaConcept(Label.of(label), Schema.MetaSchema.RULE);
    }

    @Nullable
    private <X extends SchemaConcept> X getSchemaConcept(Label label, @Nullable Schema.MetaSchema meta){
        Var var = var("x");
        VarPattern pattern = var.label(label);
        if(meta != null) pattern = pattern.sub(var().label(meta.getLabel()));
        Optional<Answer> result = queryRunner().run(Graql.match(pattern).get()).findAny();
        return result.map(answer -> (X) answer.get(var)).orElse(null);
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
        return getSchemaConcept(Schema.MetaSchema.THING.getLabel());
    }

    @Override
    public RelationshipType getMetaRelationType() {
        return getSchemaConcept(Schema.MetaSchema.RELATIONSHIP.getLabel());
    }

    @Override
    public Role getMetaRole() {
        return getSchemaConcept(Schema.MetaSchema.ROLE.getLabel());
    }

    @Override
    public AttributeType getMetaAttributeType() {
        return getSchemaConcept(Schema.MetaSchema.ATTRIBUTE.getLabel());
    }

    @Override
    public EntityType getMetaEntityType() {
        return getSchemaConcept(Schema.MetaSchema.ENTITY.getLabel());
    }

    @Override
    public Rule getMetaRule() {
        return getSchemaConcept(Schema.MetaSchema.RULE.getLabel());
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
