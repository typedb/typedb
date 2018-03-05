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
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.query.QueryBuilderImpl;
import ai.grakn.grpc.GrpcUtil;
import ai.grakn.kb.admin.GraknAdmin;
import ai.grakn.remote.concept.RemoteConcepts;
import ai.grakn.rpc.generated.GraknGrpc.GraknStub;
import ai.grakn.rpc.generated.GraknOuterClass;
import ai.grakn.rpc.generated.GraknOuterClass.DeleteRequest;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
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

    private final RemoteGraknSession session;
    private final GraknTxType txType;
    private final GrpcClient client;

    private RemoteGraknTx(RemoteGraknSession session, GraknTxType txType, GraknStub stub) {
        this.session = session;
        this.txType = txType;
        client = GrpcClient.create(this::convert, stub);
        client.open(session.keyspace(), txType);
    }

    static RemoteGraknTx create(RemoteGraknSession session, GraknTxType txType) {
        GraknStub stub = session.stub();
        return new RemoteGraknTx(session, txType, stub);
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
        Var var = var("x");
        VarPattern pattern = var.id(id);
        Optional<Answer> answer = queryRunner().run(Graql.match(pattern).get(ImmutableSet.of(var))).findAny();
        return answer.map(answer1 -> (T) answer1.get(var)).orElse(null);
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
        Var var = var("x");
        VarPattern pattern = var.val(value);
        Stream<Answer> answer = queryRunner().run(Graql.match(pattern).get(ImmutableSet.of(var)));
        return answer.map(a -> (Attribute<V>) a.get(var)).collect(Collectors.toList());
    }

    @Nullable
    @Override
    public EntityType getEntityType(String label) {
        return getSchemaConcept(Label.of(label), ENTITY);
    }

    @Nullable
    @Override
    public RelationshipType getRelationshipType(String label) {
        return getSchemaConcept(Label.of(label), RELATIONSHIP);
    }

    @Nullable
    @Override
    public <V> AttributeType<V> getAttributeType(String label) {
        return getSchemaConcept(Label.of(label), ATTRIBUTE);
    }

    @Nullable
    @Override
    public Role getRole(String label) {
        return getSchemaConcept(Label.of(label), ROLE);
    }

    @Nullable
    @Override
    public Rule getRule(String label) {
        return getSchemaConcept(Label.of(label), RULE);
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
    public Stream<SchemaConcept> sups(SchemaConcept schemaConcept) {
        Var me = Graql.var("me");
        Var target = Graql.var("target");
        GetQuery query = graql().match(me.id(schemaConcept.getId()), me.sub(target)).get();
        return query.stream().map(answer -> answer.get(target).asSchemaConcept());
    }

    @Override
    public void delete() {
        DeleteRequest request = GrpcUtil.deleteRequest(GrpcUtil.openRequest(keyspace(), GraknTxType.WRITE).getOpen());
        session.blockingStub().delete(request);
        close();
    }

    @Override
    public QueryRunner queryRunner() {
        return RemoteQueryRunner.create(this, client, null);
    }

    private Concept convert(GraknOuterClass.Concept concept) {
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
