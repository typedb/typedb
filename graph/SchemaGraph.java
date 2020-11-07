/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.graph;

import grakn.core.common.concurrent.ManagedReadWriteLock;
import grakn.core.common.exception.GraknException;
import grakn.core.graph.iid.IndexIID;
import grakn.core.graph.iid.VertexIID;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.util.KeyGenerator;
import grakn.core.graph.util.Storage;
import grakn.core.graph.vertex.RuleVertex;
import grakn.core.graph.vertex.SchemaVertex;
import grakn.core.graph.vertex.TypeVertex;
import grakn.core.graph.vertex.impl.RuleVertexImpl;
import grakn.core.graph.vertex.impl.TypeVertexImpl;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.pattern.variable.ThingVariable;

import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.SchemaGraph.INVALID_SCHEMA_WRITE;
import static grakn.core.graph.util.Encoding.Vertex.Type.scopedLabel;

public class SchemaGraph implements Graph {

    private final Storage.Schema storage;
    private final KeyGenerator.Schema.Buffered keyGenerator;
    private final ConcurrentMap<String, TypeVertex> typesByLabel;
    private final ConcurrentMap<String, RuleVertex> rulesByLabel;
    private final ConcurrentMap<VertexIID.Type, TypeVertex> typesByIID;
    private final ConcurrentMap<VertexIID.Rule, RuleVertex> rulesByIID;
    private final ConcurrentMap<String, ManagedReadWriteLock> singleLabelLocks;
    private final ManagedReadWriteLock multiLabelLock;
    private boolean isModified;
    private long snapshot;

    public SchemaGraph(final Storage.Schema storage) {
        this.storage = storage;
        keyGenerator = new KeyGenerator.Schema.Buffered();
        typesByLabel = new ConcurrentHashMap<>();
        rulesByLabel = new ConcurrentHashMap<>();
        typesByIID = new ConcurrentHashMap<>();
        rulesByIID = new ConcurrentHashMap<>();
        singleLabelLocks = new ConcurrentHashMap<>();
        multiLabelLock = new ManagedReadWriteLock();
        isModified = false;
    }

    @Override
    public Storage.Schema storage() {
        return storage;
    }

    public long snapshot() {
        return snapshot++; // TODO: update snapshot everytime we update type statistics
    }

    public boolean isInitialised() throws GraknException {
        return getType(Encoding.Vertex.Type.Root.THING.label()) != null;
    }

    public void initialise() throws GraknException {
        final TypeVertex rootThingType = create(
                Encoding.Vertex.Type.THING_TYPE,
                Encoding.Vertex.Type.Root.THING.label()).isAbstract(true);
        final TypeVertex rootEntityType = create(
                Encoding.Vertex.Type.ENTITY_TYPE,
                Encoding.Vertex.Type.Root.ENTITY.label()).isAbstract(true);
        final TypeVertex rootAttributeType = create(
                Encoding.Vertex.Type.ATTRIBUTE_TYPE,
                Encoding.Vertex.Type.Root.ATTRIBUTE.label()).isAbstract(true).valueType(Encoding.ValueType.OBJECT);
        final TypeVertex rootRelationType = create(
                Encoding.Vertex.Type.RELATION_TYPE,
                Encoding.Vertex.Type.Root.RELATION.label()).isAbstract(true);
        final TypeVertex rootRoleType = create(
                Encoding.Vertex.Type.ROLE_TYPE,
                Encoding.Vertex.Type.Root.ROLE.label(),
                Encoding.Vertex.Type.Root.RELATION.label()).isAbstract(true);

        rootEntityType.outs().put(Encoding.Edge.Type.SUB, rootThingType);
        rootAttributeType.outs().put(Encoding.Edge.Type.SUB, rootThingType);
        rootRelationType.outs().put(Encoding.Edge.Type.SUB, rootThingType);
        rootRelationType.outs().put(Encoding.Edge.Type.RELATES, rootRoleType);
    }

    public Stream<TypeVertex> types() {
        return typesByIID.values().stream();
    }

    public Stream<RuleVertex> rules() {
        return rulesByIID.values().stream();
    }

    public SchemaVertex<?, ?> convert(final VertexIID.Schema iid) {
        if (iid.isType()) return convert(iid.asType());
        if (iid.isRule()) return convert(iid.asRule());
        throw GraknException.of(ILLEGAL_STATE);
    }

    public TypeVertex convert(final VertexIID.Type iid) {
        return typesByIID.computeIfAbsent(iid, i -> {
            final TypeVertex vertex = new TypeVertexImpl.Persisted(this, i);
            typesByLabel.putIfAbsent(vertex.scopedLabel(), vertex);
            return vertex;
        });
    }

    public RuleVertex convert(final VertexIID.Rule iid) {
        return rulesByIID.computeIfAbsent(iid, i -> {
            final RuleVertex vertex = new RuleVertexImpl.Persisted(this, i);
            rulesByLabel.putIfAbsent(vertex.label(), vertex);
            return vertex;
        });
    }

    public TypeVertex getType(final String label) {
        return getType(label, null);
    }

    public TypeVertex getType(final String label, @Nullable final String scope) {
        assert storage.isOpen();
        final String scopedLabel = scopedLabel(label, scope);
        try {
            multiLabelLock.lockRead();
            singleLabelLocks.computeIfAbsent(scopedLabel, x -> new ManagedReadWriteLock()).lockRead();

            TypeVertex vertex = typesByLabel.get(scopedLabel);
            if (vertex != null) return vertex;

            final IndexIID.Type index = IndexIID.Type.of(label, scope);
            final byte[] iid = storage.get(index.bytes());
            if (iid != null) {
                vertex = typesByIID.computeIfAbsent(
                        VertexIID.Type.of(iid), i -> new TypeVertexImpl.Persisted(this, i, label, scope)
                );
                typesByLabel.putIfAbsent(scopedLabel, vertex);
            }

            return vertex;
        } catch (InterruptedException e) {
            throw new GraknException(e);
        } finally {
            singleLabelLocks.get(scopedLabel).unlockRead();
            multiLabelLock.unlockRead();
        }
    }

    public RuleVertex getRule(final String label) {
        assert storage.isOpen();
        try {
            multiLabelLock.lockRead();
            singleLabelLocks.computeIfAbsent(label, x -> new ManagedReadWriteLock()).lockRead();

            RuleVertex vertex = rulesByLabel.get(label);
            if (vertex != null) return vertex;

            final IndexIID.Rule index = IndexIID.Rule.of(label);
            final byte[] iid = storage.get(index.bytes());
            if (iid != null) {
                vertex = rulesByIID.computeIfAbsent(
                        VertexIID.Rule.of(iid), i -> new RuleVertexImpl.Persisted(this, i)
                );
                rulesByLabel.putIfAbsent(label, vertex);
            }

            return vertex;
        } catch (InterruptedException e) {
            throw new GraknException(e);
        } finally {
            singleLabelLocks.get(label).unlockRead();
            multiLabelLock.unlockRead();
        }
    }

    public TypeVertex create(final Encoding.Vertex.Type type, final String label) {
        return create(type, label, null);
    }

    public TypeVertex create(final Encoding.Vertex.Type type, final String label, @Nullable final String scope) {
        assert storage.isOpen();
        final String scopedLabel = scopedLabel(label, scope);
        try { // we intentionally use READ on multiLabelLock, as put() only concerns one label
            multiLabelLock.lockRead();
            singleLabelLocks.computeIfAbsent(scopedLabel, x -> new ManagedReadWriteLock()).lockWrite();

            final TypeVertex typeVertex = typesByLabel.computeIfAbsent(scopedLabel, i -> new TypeVertexImpl.Buffered(
                    this, VertexIID.Type.generate(keyGenerator, type), label, scope
            ));
            typesByIID.put(typeVertex.iid(), typeVertex);
            return typeVertex;
        } catch (InterruptedException e) {
            throw new GraknException(e);
        } finally {
            singleLabelLocks.get(scopedLabel).unlockWrite();
            multiLabelLock.unlockRead();
        }
    }

    public RuleVertex create(final String label, final Conjunction<? extends Pattern> when, final ThingVariable<?> then) {
        assert storage.isOpen();
        try {
            multiLabelLock.lockRead();
            singleLabelLocks.computeIfAbsent(label, x -> new ManagedReadWriteLock()).lockWrite();

            final RuleVertex ruleVertex = rulesByLabel.computeIfAbsent(label, i -> new RuleVertexImpl.Buffered(
                    this, VertexIID.Rule.generate(keyGenerator, Encoding.Vertex.Rule.RULE),
                    label, when, then
            ));
            rulesByIID.put(ruleVertex.iid(), ruleVertex);
            return ruleVertex;
        } catch (InterruptedException e) {
            throw new GraknException(e);
        } finally {
            singleLabelLocks.get(label).unlockWrite();
            multiLabelLock.unlockRead();
        }
    }

    public TypeVertex update(final TypeVertex vertex, final String oldLabel, @Nullable final String oldScope, final String newLabel, @Nullable final String newScope) {
        assert storage.isOpen();
        final String oldScopedLabel = scopedLabel(oldLabel, oldScope);
        final String newScopedLabel = scopedLabel(newLabel, newScope);
        try {
            multiLabelLock.lockWrite();
            final TypeVertex type = getType(newLabel, newScope);
            if (type != null) throw GraknException.of(INVALID_SCHEMA_WRITE.message(newScopedLabel));
            typesByLabel.remove(oldScopedLabel);
            typesByLabel.put(newScopedLabel, vertex);
            return vertex;
        } catch (InterruptedException e) {
            throw new GraknException(e);
        } finally {
            multiLabelLock.unlockWrite();
        }
    }

    public RuleVertex update(final RuleVertex vertex, final String oldLabel, final String newLabel) {
        assert storage.isOpen();
        try {
            multiLabelLock.lockWrite();
            final RuleVertex rule = getRule(newLabel);
            if (rule != null) throw GraknException.of(INVALID_SCHEMA_WRITE.message(newLabel));
            rulesByLabel.remove(oldLabel);
            rulesByLabel.put(newLabel, vertex);
            return vertex;
        } catch (InterruptedException e) {
            throw new GraknException(e);
        } finally {
            multiLabelLock.unlockWrite();
        }
    }

    public void delete(final TypeVertex vertex) {
        assert storage.isOpen();
        try { // we intentionally use READ on multiLabelLock, as delete() only concerns one label
            multiLabelLock.lockRead();
            singleLabelLocks.computeIfAbsent(vertex.scopedLabel(), x -> new ManagedReadWriteLock()).lockWrite();

            typesByLabel.remove(vertex.scopedLabel());
            typesByIID.remove(vertex.iid());
        } catch (InterruptedException e) {
            throw new GraknException(e);
        } finally {
            singleLabelLocks.get(vertex.scopedLabel()).unlockWrite();
            multiLabelLock.unlockRead();
        }
    }

    public void delete(final RuleVertex vertex) {
        assert storage.isOpen();
        try { // we intentionally use READ on multiLabelLock, as delete() only concerns one label
            // TODO do we need all these locks here? Are they applicable for this method?
            multiLabelLock.lockRead();
            singleLabelLocks.computeIfAbsent(vertex.label(), x -> new ManagedReadWriteLock()).lockWrite();

            rulesByLabel.remove(vertex.label());
            rulesByIID.remove(vertex.iid());
        } catch (InterruptedException e) {
            throw new GraknException(e);
        } finally {
            singleLabelLocks.get(vertex.label()).unlockWrite();
            multiLabelLock.unlockRead();
        }
    }

    public void setModified() {
        if (!isModified) isModified = true;
    }

    public boolean isModified() {
        return isModified;
    }

    public void incrementReference() {
        storage.incrementReference();
    }

    public void decrementReference() {
        storage.decrementReference();
    }

    public void mayRefreshStorage() {
        storage.mayRefresh();
    }

    /**
     * Commits all the writes captured in this graph into storage.
     *
     * First, for every {@code TypeVertex} that is held in {@code typeByIID},
     * we generate a unique {@code IID} to be persisted in storage. Then, we
     * commit each vertex into storage by calling {@code vertex.commit()}.
     *
     * We repeat the same process for rules.
     */
    @Override
    public void commit() {
        typesByIID.values().parallelStream().filter(v -> v.status().equals(Encoding.Status.BUFFERED)).forEach(
                vertex -> vertex.iid(VertexIID.Type.generate(storage.schemaKeyGenerator(), vertex.encoding()))
        ); // typeByIID no longer contains valid mapping from IID to TypeVertex
        rulesByIID.values().parallelStream().filter(v -> v.status().equals(Encoding.Status.BUFFERED)).forEach(
                vertex -> vertex.iid(VertexIID.Rule.generate(storage.schemaKeyGenerator(), vertex.encoding()))
        ); // rulesByIID no longer contains valid mapping from IID to TypeVertex
        typesByIID.values().forEach(SchemaVertex::commit);
        rulesByIID.values().forEach(SchemaVertex::commit);
        clear(); // we now flush the indexes after commit, and we do not expect this Graph.Type to be used again
    }

    @Override
    public void clear() {
        typesByIID.clear();
        typesByLabel.clear();
        rulesByIID.clear();
        rulesByLabel.clear();
    }
}
