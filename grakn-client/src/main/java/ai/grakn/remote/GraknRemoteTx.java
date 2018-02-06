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
import ai.grakn.Keyspace;
import ai.grakn.QueryRunner;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
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
import ai.grakn.kb.log.CommitLog;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author Felix Chapman
 */
public class GraknRemoteTx implements GraknTx, GraknAdmin {

    private final GraknSession session;
    private final GrpcClient client;

    private GraknRemoteTx(GraknSession session, GrpcClient client) {
        this.session = session;
        this.client = client;
    }

    static GraknRemoteTx create(GraknRemoteSession session, GraknTxType txType) {
        GraknGrpc.GraknStub stub = session.stub();
        GrpcClient client = GrpcClient.create(stub);
        client.open(session.keyspace(), txType);
        return new GraknRemoteTx(session, client);
    }

    @Override
    public EntityType putEntityType(String label) {
        throw new UnsupportedOperationException();
    }

    @Override
    public EntityType putEntityType(Label label) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V> AttributeType<V> putAttributeType(String label, AttributeType.DataType<V> dataType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V> AttributeType<V> putAttributeType(Label label, AttributeType.DataType<V> dataType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Rule putRule(String label, Pattern when, Pattern then) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Rule putRule(Label label, Pattern when, Pattern then) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelationshipType putRelationshipType(String label) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelationshipType putRelationshipType(Label label) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Role putRole(String label) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Role putRole(Label label) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public <T extends Concept> T getConcept(ConceptId id) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public <T extends SchemaConcept> T getSchemaConcept(Label label) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public <T extends Type> T getType(Label label) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V> Collection<Attribute<V>> getAttributesByValue(V value) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public EntityType getEntityType(String label) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public RelationshipType getRelationshipType(String label) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public <V> AttributeType<V> getAttributeType(String label) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Role getRole(String label) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Rule getRule(String label) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GraknAdmin admin() {
        return this;
    }

    @Override
    public GraknTxType txType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public GraknSession session() {
        return session;
    }

    @Override
    public Keyspace keyspace() {
        return session.keyspace();
    }

    @Override
    public boolean isClosed() {
        throw new UnsupportedOperationException();
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
    public void abort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit() throws InvalidKBException {
        client.commit();
    }

    @Override
    public <T extends Concept> T buildConcept(Vertex vertex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends Concept> T buildConcept(Edge edge) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GraphTraversalSource getTinkerTraversal() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isBatchTx() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type getMetaConcept() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelationshipType getMetaRelationType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Role getMetaRole() {
        throw new UnsupportedOperationException();
    }

    @Override
    public AttributeType getMetaAttributeType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public EntityType getMetaEntityType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Rule getMetaRule() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LabelId convertToId(Label label) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<CommitLog> commitSubmitNoLogs() throws InvalidKBException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean duplicateResourcesExist(String index, Set<ConceptId> resourceVertexIds) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean fixDuplicateResources(String index, Set<ConceptId> resourceVertexIds) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shard(ConceptId conceptId) {
        throw new UnsupportedOperationException();

    }

    @Override
    public long shardingThreshold() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends Concept> Optional<T> getConcept(Schema.VertexProperty key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Stream<SchemaConcept> sups(SchemaConcept schemaConcept) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getShardCount(Type type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryRunner queryRunner() {
        return RemoteQueryRunner.create(client, null);
    }
}
