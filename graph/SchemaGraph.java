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

import grakn.common.collection.Pair;
import grakn.core.common.concurrent.ManagedReadWriteLock;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Label;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.pair;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.SchemaGraph.INVALID_SCHEMA_WRITE;
import static grakn.core.common.iterator.Iterators.tree;
import static grakn.core.graph.util.Encoding.Edge.Type.RELATES;
import static grakn.core.graph.util.Encoding.Edge.Type.SUB;
import static grakn.core.graph.util.Encoding.ValueType.OBJECT;
import static grakn.core.graph.util.Encoding.Vertex.Type.ATTRIBUTE_TYPE;
import static grakn.core.graph.util.Encoding.Vertex.Type.ENTITY_TYPE;
import static grakn.core.graph.util.Encoding.Vertex.Type.RELATION_TYPE;
import static grakn.core.graph.util.Encoding.Vertex.Type.ROLE_TYPE;
import static grakn.core.graph.util.Encoding.Vertex.Type.Root.ATTRIBUTE;
import static grakn.core.graph.util.Encoding.Vertex.Type.Root.ENTITY;
import static grakn.core.graph.util.Encoding.Vertex.Type.Root.RELATION;
import static grakn.core.graph.util.Encoding.Vertex.Type.Root.ROLE;
import static grakn.core.graph.util.Encoding.Vertex.Type.Root.THING;
import static grakn.core.graph.util.Encoding.Vertex.Type.THING_TYPE;
import static grakn.core.graph.util.Encoding.Vertex.Type.scopedLabel;

public class SchemaGraph implements Graph {

    private final Storage.Schema storage;
    private final KeyGenerator.Schema.Buffered keyGenerator;
    private final ConcurrentMap<String, TypeVertex> typesByLabel;
    private final ConcurrentMap<String, RuleVertex> rulesByLabel;
    private final ConcurrentMap<VertexIID.Type, TypeVertex> typesByIID;
    private final ConcurrentMap<VertexIID.Rule, RuleVertex> rulesByIID;
    private final ConcurrentMap<String, ManagedReadWriteLock> singleLabelLocks;
    private final ConcurrentMap<Pair<Label, Boolean>, Long> subTypeCounts;
    private final ManagedReadWriteLock multiLabelLock;
    private final AtomicLong thingTypeCount;
    private final AtomicLong attributeTypeCount;
    private final AtomicLong relationTypeCount;
    private final AtomicLong roleTypeCount;
    private final boolean isReadOnly;
    private boolean isModified;
    private long snapshot;

    public SchemaGraph(Storage.Schema storage, boolean isReadOnly) {
        this.storage = storage;
        this.isReadOnly = isReadOnly;
        keyGenerator = new KeyGenerator.Schema.Buffered();
        typesByLabel = new ConcurrentHashMap<>();
        rulesByLabel = new ConcurrentHashMap<>();
        typesByIID = new ConcurrentHashMap<>();
        rulesByIID = new ConcurrentHashMap<>();
        singleLabelLocks = new ConcurrentHashMap<>();
        multiLabelLock = new ManagedReadWriteLock();
        subTypeCounts = new ConcurrentHashMap<>();
        thingTypeCount = new AtomicLong(0);
        attributeTypeCount = new AtomicLong(0);
        relationTypeCount = new AtomicLong(0);
        roleTypeCount = new AtomicLong(0);
        isModified = false;
        snapshot = 1L;
    }

    @Override
    public Storage.Schema storage() {
        return storage;
    }

    public long snapshot() {
        return ++snapshot; // TODO: update snapshot everytime we update type statistics
    }

    public boolean isInitialised() throws GraknException {
        return rootThingType() != null;
    }

    public void initialise() throws GraknException {
        final TypeVertex rootThingType = create(THING_TYPE, THING.label()).isAbstract(true);
        final TypeVertex rootEntityType = create(ENTITY_TYPE, ENTITY.label()).isAbstract(true);
        final TypeVertex rootAttributeType = create(ATTRIBUTE_TYPE, ATTRIBUTE.label()).isAbstract(true).valueType(OBJECT);
        final TypeVertex rootRelationType = create(RELATION_TYPE, RELATION.label()).isAbstract(true);
        final TypeVertex rootRoleType = create(ROLE_TYPE, ROLE.label(), RELATION.label()).isAbstract(true);

        rootEntityType.outs().put(SUB, rootThingType);
        rootAttributeType.outs().put(SUB, rootThingType);
        rootRelationType.outs().put(SUB, rootThingType);
        rootRelationType.outs().put(RELATES, rootRoleType);
    }

    public long countThingTypes() {
        if (isReadOnly) {
            thingTypeCount.compareAndSet(0, thingTypes().count());
            return thingTypeCount.get();
        } else {
            return thingTypes().count();
        }
    }

    public long countAttributeTypes() {
        if (isReadOnly) {
            attributeTypeCount.compareAndSet(0, attributeTypes().count());
            return attributeTypeCount.get();
        } else {
            return attributeTypes().count();
        }
    }

    public long countRelationTypes() {
        if (isReadOnly) {
            relationTypeCount.compareAndSet(0, relationTypes().count());
            return relationTypeCount.get();
        } else {
            return relationTypes().count();
        }
    }

    public long countRoleTypes() {
        if (isReadOnly) {
            roleTypeCount.compareAndSet(0, roleTypes().count());
            return roleTypeCount.get();
        } else {
            return roleTypes().count();
        }
    }

    public long countSubTypes(Set<Label> labels, boolean isTransitive) {
        long count = 0;
        for (final Label label : labels) {
            count += countSubTypes(label, isTransitive);
        }
        return count;
    }

    public long countSubTypes(Label label, boolean isTransitive) {
        if (isReadOnly) {
            return subTypeCounts.computeIfAbsent(pair(label, isTransitive), k -> subTypes(label, isTransitive).count());
        } else {
            return subTypes(label, isTransitive).count();
        }
    }

    public Stream<TypeVertex> subTypes(Label label, boolean isTransitive) {
        if (!isTransitive) return getType(label).ins().edge(SUB).from().stream();
        else return tree(getType(label), v -> v.ins().edge(SUB).from()).stream();
    }

    public long countInstances(Set<Label> labels, boolean isTransitive) {
        long count = 0;
        for (Label label : labels) {
            TypeVertex typeVertex = getType(label.name(), label.scope().orElse(null));
            if (isTransitive) count += typeVertex.instancesCountTransitive();
            else count += typeVertex.instancesCount();
        }
        return count;
    }

    public Stream<TypeVertex> thingTypes() {
        return tree(rootThingType(), v -> v.ins().edge(SUB).from()).stream();
    }

    public Stream<TypeVertex> entityTypes() {
        return tree(rootEntityType(), v -> v.ins().edge(SUB).from()).stream();
    }

    public Stream<TypeVertex> attributeTypes() {
        return tree(rootAttributeType(), v -> v.ins().edge(SUB).from()).stream();
    }

    private Stream<TypeVertex> relationTypes() {
        return tree(rootRelationType(), v -> v.ins().edge(SUB).from()).stream();
    }

    private Stream<TypeVertex> roleTypes() {
        return tree(rootRoleType(), v -> v.ins().edge(SUB).from()).stream();
    }

    public TypeVertex rootThingType() {
        return getType(THING.label());
    }

    public TypeVertex rootEntityType() {
        return getType(ENTITY.label());
    }

    public TypeVertex rootAttributeType() {
        return getType(ATTRIBUTE.label());
    }

    public TypeVertex rootRelationType() {
        return getType(RELATION.label());
    }

    public TypeVertex rootRoleType() {
        return getType(ROLE.label(), ROLE.scope());
    }

    public Stream<TypeVertex> bufferedTypes() {
        return typesByIID.values().stream();
    }

    public Stream<RuleVertex> bufferedRules() {
        return rulesByIID.values().stream();
    }

    public SchemaVertex<?, ?> convert(VertexIID.Schema iid) {
        if (iid.isType()) return convert(iid.asType());
        if (iid.isRule()) return convert(iid.asRule());
        throw GraknException.of(ILLEGAL_STATE);
    }

    public TypeVertex convert(VertexIID.Type iid) {
        return typesByIID.computeIfAbsent(iid, i -> {
            final TypeVertex vertex = new TypeVertexImpl.Persisted(this, i);
            typesByLabel.putIfAbsent(vertex.scopedLabel(), vertex);
            return vertex;
        });
    }

    public RuleVertex convert(VertexIID.Rule iid) {
        return rulesByIID.computeIfAbsent(iid, i -> {
            final RuleVertex vertex = new RuleVertexImpl.Persisted(this, i);
            rulesByLabel.putIfAbsent(vertex.label(), vertex);
            return vertex;
        });
    }

    public TypeVertex getType(Label label) {
        return getType(label.name(), label.scope().orElse(null));
    }

    public TypeVertex getType(String label) {
        return getType(label, null);
    }

    public TypeVertex getType(String label, @Nullable String scope) {
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

    public RuleVertex getRule(String label) {
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

    public TypeVertex create(Encoding.Vertex.Type type, String label) {
        return create(type, label, null);
    }

    public TypeVertex create(Encoding.Vertex.Type type, String label, @Nullable String scope) {
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

    public RuleVertex create(String label, Conjunction<? extends Pattern> when, ThingVariable<?> then) {
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

    public TypeVertex update(TypeVertex vertex, String oldLabel, @Nullable String oldScope, String newLabel, @Nullable String newScope) {
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

    public RuleVertex update(RuleVertex vertex, String oldLabel, String newLabel) {
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

    public void delete(TypeVertex vertex) {
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

    public void delete(RuleVertex vertex) {
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
