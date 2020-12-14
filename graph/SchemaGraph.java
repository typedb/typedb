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
import grakn.core.common.iterator.Iterators;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Label;
import grakn.core.graph.iid.IndexIID;
import grakn.core.graph.iid.StructureIID;
import grakn.core.graph.iid.VertexIID;
import grakn.core.graph.structure.RuleStructure;
import grakn.core.graph.structure.impl.RuleStructureImpl;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.util.KeyGenerator;
import grakn.core.graph.util.Storage;
import grakn.core.graph.vertex.TypeVertex;
import grakn.core.graph.vertex.impl.TypeVertexImpl;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.pattern.variable.ThingVariable;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.pair;
import static grakn.core.common.exception.ErrorMessage.SchemaGraph.INVALID_SCHEMA_WRITE;
import static grakn.core.common.iterator.Iterators.link;
import static grakn.core.common.iterator.Iterators.tree;
import static grakn.core.graph.util.Encoding.Edge.Type.OWNS;
import static grakn.core.graph.util.Encoding.Edge.Type.OWNS_KEY;
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
import static java.lang.Math.toIntExact;
import static java.util.stream.Collectors.toSet;

public class SchemaGraph implements Graph {

    private final Storage storage;
    private final KeyGenerator.Schema.Buffered keyGenerator;
    private final ConcurrentMap<String, TypeVertex> typesByLabel;
    private final ConcurrentMap<String, RuleStructure> rulesByLabel;
    private final ConcurrentMap<VertexIID.Type, TypeVertex> typesByIID;
    private final ConcurrentMap<StructureIID.Rule, RuleStructure> rulesByIID;
    private final ConcurrentMap<String, ManagedReadWriteLock> singleLabelLocks;
    private final ManagedReadWriteLock multiLabelLock;
    private final Statistics statistics;
    private final Cache cache;
    private final boolean isReadOnly;
    private boolean isModified;

    public SchemaGraph(Storage storage, boolean isReadOnly) {
        this.storage = storage;
        this.isReadOnly = isReadOnly;
        keyGenerator = new KeyGenerator.Schema.Buffered();
        typesByLabel = new ConcurrentHashMap<>();
        rulesByLabel = new ConcurrentHashMap<>();
        typesByIID = new ConcurrentHashMap<>();
        rulesByIID = new ConcurrentHashMap<>();
        singleLabelLocks = new ConcurrentHashMap<>();
        multiLabelLock = new ManagedReadWriteLock();
        statistics = new Statistics();
        cache = new Cache();
        isModified = false;
    }

    static class Cache {

        private final ConcurrentMap<TypeVertex, Set<TypeVertex>> ownedAttributeTypes;

        private final ConcurrentMap<TypeVertex, Set<TypeVertex>> ownersOfAttributeTypes;

        Cache() {
            ownedAttributeTypes = new ConcurrentHashMap<>();
            ownersOfAttributeTypes = new ConcurrentHashMap<>();
        }

    }

    @Override
    public Storage storage() {
        return storage;
    }

    public SchemaGraph.Statistics stats() {
        return statistics;
    }

    public boolean isReadOnly() {
        return isReadOnly;
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

    public ResourceIterator<RuleStructure> rules() {
        Encoding.Prefix index = IndexIID.Rule.prefix();
        ResourceIterator<RuleStructure> persistedRules = storage.iterate(index.bytes(), (key, value) ->
                convert(StructureIID.Rule.of(value)));
        return link(list(Iterators.iterate(rulesByIID.values()), persistedRules)).distinct();
    }

    public ResourceIterator<TypeVertex> thingTypes() {
        return tree(rootThingType(), v -> v.ins().edge(SUB).from());
    }

    public ResourceIterator<TypeVertex> entityTypes() {
        return tree(rootEntityType(), v -> v.ins().edge(SUB).from());
    }

    public ResourceIterator<TypeVertex> attributeTypes() {
        return tree(rootAttributeType(), v -> v.ins().edge(SUB).from());
    }

    public ResourceIterator<TypeVertex> attributeTypes(Encoding.ValueType vt) {
        return attributeTypes().filter(at -> at.valueType().equals(vt));
    }

    public ResourceIterator<TypeVertex> relationTypes() {
        return tree(rootRelationType(), v -> v.ins().edge(SUB).from());
    }

    public ResourceIterator<TypeVertex> roleTypes() {
        return tree(rootRoleType(), v -> v.ins().edge(SUB).from());
    }

    public Set<TypeVertex> ownedAttributeTypes(TypeVertex owner) {
        Supplier<Set<TypeVertex>> fn = () -> link(
                list(owner.outs().edge(OWNS).to(), owner.outs().edge(OWNS_KEY).to())
        ).stream().collect(toSet());
        if (isReadOnly) return cache.ownedAttributeTypes.computeIfAbsent(owner, o -> fn.get());
        else return fn.get();
    }

    public Set<TypeVertex> ownersOfAttributeType(TypeVertex attType) {
        final Supplier<Set<TypeVertex>> fn = () -> link(list(
                attType.ins().edge(OWNS).from(), attType.ins().edge(OWNS_KEY).from()
        )).stream().collect(toSet());
        if (isReadOnly) return cache.ownersOfAttributeTypes.computeIfAbsent(attType, a -> fn.get());
        else return fn.get();
    }

    public Stream<TypeVertex> bufferedTypes() {
        return typesByIID.values().stream();
    }

    public Stream<RuleStructure> bufferedRules() {
        return rulesByIID.values().stream();
    }

    public TypeVertex convert(VertexIID.Type iid) {
        return typesByIID.computeIfAbsent(iid, i -> {
            final TypeVertex vertex = new TypeVertexImpl.Persisted(this, i);
            typesByLabel.putIfAbsent(vertex.scopedLabel(), vertex);
            return vertex;
        });
    }

    public RuleStructure convert(StructureIID.Rule iid) {
        return rulesByIID.computeIfAbsent(iid, i -> {
            final RuleStructure structure = new RuleStructureImpl.Persisted(this, i);
            rulesByLabel.putIfAbsent(structure.label(), structure);
            return structure;
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
            throw GraknException.of(e);
        } finally {
            singleLabelLocks.get(scopedLabel).unlockRead();
            multiLabelLock.unlockRead();
        }
    }

    public RuleStructure getRule(String label) {
        assert storage.isOpen();
        try {
            multiLabelLock.lockRead();
            singleLabelLocks.computeIfAbsent(label, x -> new ManagedReadWriteLock()).lockRead();

            RuleStructure vertex = rulesByLabel.get(label);
            if (vertex != null) return vertex;

            final IndexIID.Rule index = IndexIID.Rule.of(label);
            final byte[] iid = storage.get(index.bytes());
            if (iid != null) {
                vertex = rulesByIID.computeIfAbsent(
                        StructureIID.Rule.of(iid), i -> new RuleStructureImpl.Persisted(this, i)
                );
                rulesByLabel.putIfAbsent(label, vertex);
            }

            return vertex;
        } catch (InterruptedException e) {
            throw GraknException.of(e);
        } finally {
            singleLabelLocks.get(label).unlockRead();
            multiLabelLock.unlockRead();
        }
    }

    public TypeVertex create(Encoding.Vertex.Type encoding, String label) {
        return create(encoding, label, null);
    }

    public TypeVertex create(Encoding.Vertex.Type encoding, String label, @Nullable String scope) {
        assert storage.isOpen();
        final String scopedLabel = scopedLabel(label, scope);
        try { // we intentionally use READ on multiLabelLock, as put() only concerns one label
            multiLabelLock.lockRead();
            singleLabelLocks.computeIfAbsent(scopedLabel, x -> new ManagedReadWriteLock()).lockWrite();

            final TypeVertex typeVertex = typesByLabel.computeIfAbsent(scopedLabel, i -> new TypeVertexImpl.Buffered(
                    this, VertexIID.Type.generate(keyGenerator, encoding), label, scope
            ));
            typesByIID.put(typeVertex.iid(), typeVertex);
            return typeVertex;
        } catch (InterruptedException e) {
            throw GraknException.of(e);
        } finally {
            singleLabelLocks.get(scopedLabel).unlockWrite();
            multiLabelLock.unlockRead();
        }
    }

    public RuleStructure create(String label, Conjunction<? extends Pattern> when, ThingVariable<?> then) {
        assert storage.isOpen();
        try {
            multiLabelLock.lockRead();
            singleLabelLocks.computeIfAbsent(label, x -> new ManagedReadWriteLock()).lockWrite();

            final RuleStructure rule = rulesByLabel.computeIfAbsent(label, i -> new RuleStructureImpl.Buffered(
                    this, StructureIID.Rule.generate(keyGenerator), label, when, then
            ));
            rulesByIID.put(rule.iid(), rule);
            return rule;
        } catch (InterruptedException e) {
            throw GraknException.of(e);
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
            if (type != null) throw GraknException.of(INVALID_SCHEMA_WRITE, newScopedLabel);
            typesByLabel.remove(oldScopedLabel);
            typesByLabel.put(newScopedLabel, vertex);
            return vertex;
        } catch (InterruptedException e) {
            throw GraknException.of(e);
        } finally {
            multiLabelLock.unlockWrite();
        }
    }

    public RuleStructure update(RuleStructure vertex, String oldLabel, String newLabel) {
        assert storage.isOpen();
        try {
            multiLabelLock.lockWrite();
            final RuleStructure rule = getRule(newLabel);
            if (rule != null) throw GraknException.of(INVALID_SCHEMA_WRITE, newLabel);
            rulesByLabel.remove(oldLabel);
            rulesByLabel.put(newLabel, vertex);
            return vertex;
        } catch (InterruptedException e) {
            throw GraknException.of(e);
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
            throw GraknException.of(e);
        } finally {
            singleLabelLocks.get(vertex.scopedLabel()).unlockWrite();
            multiLabelLock.unlockRead();
        }
    }

    public void delete(RuleStructure vertex) {
        assert storage.isOpen();
        try { // we intentionally use READ on multiLabelLock, as delete() only concerns one label
            // TODO do we need all these locks here? Are they applicable for this method?
            multiLabelLock.lockRead();
            singleLabelLocks.computeIfAbsent(vertex.label(), x -> new ManagedReadWriteLock()).lockWrite();

            rulesByLabel.remove(vertex.label());
            rulesByIID.remove(vertex.iid());
        } catch (InterruptedException e) {
            throw GraknException.of(e);
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
        assert storage.isSchema();
        typesByIID.values().parallelStream().filter(v -> v.status().equals(Encoding.Status.BUFFERED)).forEach(
                typeVertex -> typeVertex.iid(VertexIID.Type.generate(storage.asSchema().schemaKeyGenerator(), typeVertex.encoding()))
        ); // typeByIID no longer contains valid mapping from IID to TypeVertex
        rulesByIID.values().parallelStream().filter(v -> v.status().equals(Encoding.Status.BUFFERED)).forEach(
                ruleStructure -> ruleStructure.iid(StructureIID.Rule.generate(storage.asSchema().schemaKeyGenerator()))
        ); // rulesByIID no longer contains valid mapping from IID to TypeVertex
        typesByIID.values().forEach(TypeVertex::commit);
        rulesByIID.values().forEach(RuleStructure::commit);
        clear(); // we now flush the indexes after commit, and we do not expect this Graph.Type to be used again
    }

    @Override
    public void clear() {
        typesByIID.clear();
        typesByLabel.clear();
        rulesByIID.clear();
        rulesByLabel.clear();
    }

    public class Statistics {

        private static final int UNSET_COUNT = -1;
        private volatile int abstractTypeCount;
        private volatile int thingTypeCount;
        private volatile int attributeTypeCount;
        private volatile int relationTypeCount;
        private volatile int roleTypeCount;
        private final ConcurrentMap<TypeVertex, Long> subTypesDepth;
        private final ConcurrentMap<Pair<TypeVertex, Boolean>, Long> subTypesCount;
        private final ConcurrentMap<Encoding.ValueType, Long> attTypesWithValueType;

        private Statistics() {
            abstractTypeCount = UNSET_COUNT;
            thingTypeCount = UNSET_COUNT;
            attributeTypeCount = UNSET_COUNT;
            relationTypeCount = UNSET_COUNT;
            roleTypeCount = UNSET_COUNT;
            subTypesDepth = new ConcurrentHashMap<>();
            subTypesCount = new ConcurrentHashMap<>();
            attTypesWithValueType = new ConcurrentHashMap<>();
        }

        public long abstractTypeCount() {
            Supplier<Integer> fn = () -> toIntExact(thingTypes().stream().filter(TypeVertex::isAbstract).count());
            if (isReadOnly) {
                if (abstractTypeCount == UNSET_COUNT) abstractTypeCount = fn.get();
                return abstractTypeCount;
            } else {
                return fn.get();
            }
        }

        public long thingTypeCount() {
            Supplier<Integer> fn = () -> toIntExact(thingTypes().stream().count());
            if (isReadOnly) {
                if (thingTypeCount == UNSET_COUNT) thingTypeCount = fn.get();
                return thingTypeCount;
            } else {
                return fn.get();
            }
        }

        public long relationTypeCount() {
            Supplier<Integer> fn = () -> toIntExact(relationTypes().stream().count());
            if (isReadOnly) {
                if (relationTypeCount == UNSET_COUNT) relationTypeCount = fn.get();
                return relationTypeCount;
            } else {
                return fn.get();
            }
        }

        public long roleTypeCount() {
            Supplier<Integer> fn = () -> toIntExact(roleTypes().stream().count());
            if (isReadOnly) {
                if (roleTypeCount == UNSET_COUNT) roleTypeCount = fn.get();
                return roleTypeCount;
            } else {
                return fn.get();
            }
        }

        public long attributeTypeCount() {
            Supplier<Integer> fn = () -> toIntExact(attributeTypes().stream().count());
            if (isReadOnly) {
                if (attributeTypeCount == UNSET_COUNT) attributeTypeCount = fn.get();
                return attributeTypeCount;
            } else {
                return fn.get();
            }
        }

        public long attTypesWithValueType(Encoding.ValueType valueType) {
            Supplier<Long> fn = () -> attributeTypes(valueType).stream().count();
            if (isReadOnly) return attTypesWithValueType.computeIfAbsent(valueType, vt -> fn.get());
            else return fn.get();
        }

        public long attTypesWithValTypeComparableTo(Set<Label> labels) {
            Set<Encoding.ValueType> valueTypes =
                    labels.stream().flatMap(l -> getType(l).valueType().comparables().stream()).collect(toSet());
            return valueTypes.stream().mapToLong(this::attTypesWithValueType).sum();
        }

        public double outOwnsMean(Set<Label> labels, boolean isKey) {
            return outOwnsMean(labels.stream().map(SchemaGraph.this::getType), isKey);
        }

        public double outOwnsMean(Stream<TypeVertex> types, boolean isKey) {
            return types.mapToLong(vertex -> vertex.outOwnsCount(isKey)).average().orElse(0);
        }

        public double inOwnsMean(Set<Label> labels, boolean isKey) {
            return inOwnsMean(labels.stream().map(SchemaGraph.this::getType), isKey);
        }

        public double inOwnsMean(Stream<TypeVertex> types, boolean isKey) {
            return types.mapToLong(vertex -> vertex.inOwnsCount(isKey)).average().orElse(0);
        }

        public double outPlaysMean(Set<Label> labels) {
            return outPlaysMean(labels.stream().map(SchemaGraph.this::getType));
        }

        public double outPlaysMean(Stream<TypeVertex> types) {
            return types.mapToLong(TypeVertex::outPlaysCount).average().orElse(0);
        }

        public double inPlaysMean(Set<Label> labels) {
            return inPlaysMean(labels.stream().map(SchemaGraph.this::getType));
        }

        public double inPlaysMean(Stream<TypeVertex> types) {
            return types.mapToLong(TypeVertex::inPlaysCount).average().orElse(0);
        }

        public double outRelates(Set<Label> labels) {
            return outRelates(labels.stream().map(SchemaGraph.this::getType));
        }

        public double outRelates(Stream<TypeVertex> types) {
            return types.mapToLong(TypeVertex::outRelatesCount).average().orElse(0);
        }

        public double subTypesMean(Set<Label> labels, boolean isTransitive) {
            return subTypesMean(labels.stream().map(SchemaGraph.this::getType), isTransitive);
        }

        public double subTypesMean(Stream<TypeVertex> types, boolean isTransitive) {
            return types.mapToLong(t -> subTypesCount(t, isTransitive)).average().orElse(0);
        }

        public long subTypesSum(Set<Label> labels, boolean isTransitive) {
            return labels.stream().mapToLong(l -> subTypesCount(getType(l), isTransitive)).sum();
        }

        public long subTypesCount(TypeVertex type, boolean isTransitive) {
            Supplier<Long> countFn = () -> {
                if (!isTransitive) return type.ins().edge(SUB).from().stream().count();
                else return tree(type, v -> v.ins().edge(SUB).from()).stream().count() - 1;
            };
            if (isReadOnly) return subTypesCount.computeIfAbsent(pair(type, isTransitive), p -> countFn.get());
            else return countFn.get();
        }

        public long subTypesDepth(Set<Label> labels) {
            return labels.stream().mapToLong(l -> subTypesDepth(getType(l))).max().orElse(0);
        }

        public long subTypesDepth(TypeVertex type) {
            Supplier<Long> maxDepthFn =
                    () -> 1 + type.ins().edge(SUB).from().stream().mapToLong(this::subTypesDepth).max().orElse(0);
            if (isReadOnly) return subTypesDepth.computeIfAbsent(type, t -> maxDepthFn.get());
            else return maxDepthFn.get();
        }
    }
}
