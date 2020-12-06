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

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Label;
import grakn.core.graph.iid.EdgeIID;
import grakn.core.graph.iid.VertexIID;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.util.KeyGenerator;
import grakn.core.graph.util.Storage;
import grakn.core.graph.vertex.AttributeVertex;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.TypeVertex;
import grakn.core.graph.vertex.Vertex;
import grakn.core.graph.vertex.impl.AttributeVertexImpl;
import grakn.core.graph.vertex.impl.ThingVertexImpl;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.list;
import static grakn.core.common.collection.Bytes.join;
import static grakn.core.common.iterator.Iterators.link;
import static grakn.core.common.iterator.Iterators.tree;
import static grakn.core.graph.iid.VertexIID.Thing.generate;
import static grakn.core.graph.util.Encoding.Edge.Type.SUB;
import static grakn.core.graph.util.Encoding.Vertex.Type.ATTRIBUTE_TYPE;
import static java.util.stream.Stream.concat;

public class DataGraph implements Graph {

    private final Storage.Data storage;
    private final SchemaGraph schemaGraph;
    private final KeyGenerator.Data.Buffered keyGenerator;
    private final ConcurrentMap<VertexIID.Thing, ThingVertex> thingsByIID;
    private final ConcurrentMap<VertexIID.Type, Set<ThingVertex>> thingsByTypeIID;
    private final AttributesByIID attributesByIID;
    private final Statistics statistics;
    private boolean isModified;

    public DataGraph(Storage.Data storage, SchemaGraph schemaGraph) {
        this.storage = storage;
        this.schemaGraph = schemaGraph;
        keyGenerator = new KeyGenerator.Data.Buffered();
        thingsByIID = new ConcurrentHashMap<>();
        thingsByTypeIID = new ConcurrentHashMap<>();
        attributesByIID = new AttributesByIID();
        statistics = new Statistics();
    }

    @Override
    public Storage.Data storage() {
        return storage;
    }

    public SchemaGraph schema() {
        return schemaGraph;
    }

    public DataGraph.Statistics stats() {
        return statistics;
    }

    public Stream<ThingVertex> vertices() {
        return concat(thingsByIID.values().stream(), attributesByIID.valueStream());
    }

    public ThingVertex get(VertexIID.Thing iid) {
        assert storage.isOpen();
        if (iid.encoding().equals(Encoding.Vertex.Thing.ATTRIBUTE)) return get(iid.asAttribute());
        else if (!thingsByIID.containsKey(iid) && storage.get(iid.bytes()) == null) return null;
        return convert(iid);
    }

    public AttributeVertex<?> get(VertexIID.Attribute<?> iid) {
        if (!attributesByIID.forValueType(iid.valueType()).containsKey(iid) && storage.get(iid.bytes()) == null) {
            return null;
        }
        return convert(iid);
    }

    public ThingVertex convert(VertexIID.Thing iid) {
        // TODO: benchmark caching persisted edges
        // assert storage.isOpen();
        // enable the the line above
        if (iid.encoding().equals(Encoding.Vertex.Thing.ATTRIBUTE)) return convert(iid.asAttribute());
        else return thingsByIID.computeIfAbsent(iid, i -> ThingVertexImpl.of(this, i));
    }

    public AttributeVertex<?> convert(VertexIID.Attribute<?> attIID) {
        switch (attIID.valueType()) {
            case BOOLEAN:
                return attributesByIID.booleans.computeIfAbsent(
                        attIID.asBoolean(), iid1 -> new AttributeVertexImpl.Boolean(this, iid1)
                );
            case LONG:
                return attributesByIID.longs.computeIfAbsent(
                        attIID.asLong(), iid1 -> new AttributeVertexImpl.Long(this, iid1)
                );
            case DOUBLE:
                return attributesByIID.doubles.computeIfAbsent(
                        attIID.asDouble(), iid1 -> new AttributeVertexImpl.Double(this, iid1)
                );
            case STRING:
                return attributesByIID.strings.computeIfAbsent(
                        attIID.asString(), iid1 -> new AttributeVertexImpl.String(this, iid1)
                );
            case DATETIME:
                return attributesByIID.dateTimes.computeIfAbsent(
                        attIID.asDateTime(), iid1 -> new AttributeVertexImpl.DateTime(this, iid1)
                );
            default:
                assert false;
                return null;
        }
    }

    public ThingVertex create(TypeVertex typeVertex, boolean isInferred) {
        assert storage.isOpen();
        assert !typeVertex.encoding().equals(ATTRIBUTE_TYPE);
        final VertexIID.Thing iid = generate(keyGenerator, typeVertex.iid(), typeVertex.properLabel());
        final ThingVertex vertex = new ThingVertexImpl.Buffered(this, iid, isInferred);
        thingsByIID.put(iid, vertex);
        thingsByTypeIID.computeIfAbsent(typeVertex.iid(), t -> new HashSet<>()).add(vertex);
        return vertex;
    }

    private <VALUE, ATT_IID extends VertexIID.Attribute<VALUE>, ATT_VERTEX extends AttributeVertex<VALUE>>
    ATT_VERTEX getOrReadFromStorage(Map<ATT_IID, ATT_VERTEX> map, ATT_IID attIID, Function<ATT_IID, ATT_VERTEX> vertexConstructor) {
        return map.computeIfAbsent(attIID, iid -> {
            final byte[] val = storage.get(iid.bytes());
            if (val != null) return vertexConstructor.apply(iid);
            else return null;
        });
    }

    public ResourceIterator<ThingVertex> get(TypeVertex typeVertex) {
        final ResourceIterator<ThingVertex> storageIterator = storage.iterate(
                join(typeVertex.iid().bytes(), Encoding.Edge.ISA.in().bytes()),
                (key, value) -> convert(EdgeIID.InwardsISA.of(key).end())
        );
        if (!thingsByTypeIID.containsKey(typeVertex.iid())) return storageIterator;
        else return link(list(thingsByTypeIID.get(typeVertex.iid()).iterator(), storageIterator)).distinct();
    }

    public AttributeVertex<Boolean> get(TypeVertex type, boolean value) {
        assert storage.isOpen();
        assert type.encoding().equals(ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Boolean.class);

        return getOrReadFromStorage(
                attributesByIID.booleans,
                new VertexIID.Attribute.Boolean(type.iid(), value),
                iid -> new AttributeVertexImpl.Boolean(this, iid)
        );
    }

    public AttributeVertex<Long> get(TypeVertex type, long value) {
        assert storage.isOpen();
        assert type.encoding().equals(ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Long.class);

        return getOrReadFromStorage(
                attributesByIID.longs,
                new VertexIID.Attribute.Long(type.iid(), value),
                iid -> new AttributeVertexImpl.Long(this, iid)
        );
    }

    public AttributeVertex<Double> get(TypeVertex type, double value) {
        assert storage.isOpen();
        assert type.encoding().equals(ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Double.class);

        return getOrReadFromStorage(
                attributesByIID.doubles,
                new VertexIID.Attribute.Double(type.iid(), value),
                iid -> new AttributeVertexImpl.Double(this, iid)
        );
    }

    public AttributeVertex<String> get(TypeVertex type, String value) {
        assert storage.isOpen();
        assert type.encoding().equals(ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(String.class);

        return getOrReadFromStorage(
                attributesByIID.strings,
                new VertexIID.Attribute.String(type.iid(), value),
                iid -> new AttributeVertexImpl.String(this, iid)
        );
    }

    public AttributeVertex<LocalDateTime> get(TypeVertex type, LocalDateTime value) {
        assert storage.isOpen();
        assert type.encoding().equals(ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(LocalDateTime.class);

        return getOrReadFromStorage(
                attributesByIID.dateTimes,
                new VertexIID.Attribute.DateTime(type.iid(), value),
                iid -> new AttributeVertexImpl.DateTime(this, iid)
        );
    }

    public AttributeVertex<Boolean> put(TypeVertex type, boolean value, boolean isInferred) {
        assert storage.isOpen();
        assert type.encoding().equals(ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Boolean.class);

        final AttributeVertex<Boolean> vertex = attributesByIID.booleans.computeIfAbsent(
                new VertexIID.Attribute.Boolean(type.iid(), value),
                iid -> {
                    final AttributeVertex<Boolean> v = new AttributeVertexImpl.Boolean(this, iid, isInferred);
                    thingsByTypeIID.computeIfAbsent(type.iid(), t -> new HashSet<>()).add(v);
                    return v;
                }
        );
        if (!isInferred && vertex.isInferred()) vertex.isInferred(false);
        return vertex;
    }

    public AttributeVertex<Long> put(TypeVertex type, long value, boolean isInferred) {
        assert storage.isOpen();
        assert type.encoding().equals(ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Long.class);

        final AttributeVertex<Long> vertex = attributesByIID.longs.computeIfAbsent(
                new VertexIID.Attribute.Long(type.iid(), value),
                iid -> {
                    final AttributeVertex<Long> v = new AttributeVertexImpl.Long(this, iid, isInferred);
                    thingsByTypeIID.computeIfAbsent(type.iid(), t -> new HashSet<>()).add(v);
                    return v;
                }
        );
        if (!isInferred && vertex.isInferred()) vertex.isInferred(false);
        return vertex;
    }

    public AttributeVertex<Double> put(TypeVertex type, double value, boolean isInferred) {
        assert storage.isOpen();
        assert type.encoding().equals(ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Double.class);

        final AttributeVertex<Double> vertex = attributesByIID.doubles.computeIfAbsent(
                new VertexIID.Attribute.Double(type.iid(), value),
                iid -> {
                    final AttributeVertex<Double> v = new AttributeVertexImpl.Double(this, iid, isInferred);
                    thingsByTypeIID.computeIfAbsent(type.iid(), t -> new HashSet<>()).add(v);
                    return v;
                }
        );
        if (!isInferred && vertex.isInferred()) vertex.isInferred(false);
        return vertex;
    }

    public AttributeVertex<String> put(TypeVertex type, String value, boolean isInferred) {
        assert storage.isOpen();
        assert type.encoding().equals(ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(String.class);
        assert value.length() <= Encoding.STRING_MAX_LENGTH;

        final AttributeVertex<String> vertex = attributesByIID.strings.computeIfAbsent(
                new VertexIID.Attribute.String(type.iid(), value),
                iid -> {
                    final AttributeVertex<String> v = new AttributeVertexImpl.String(this, iid, isInferred);
                    thingsByTypeIID.computeIfAbsent(type.iid(), t -> new HashSet<>()).add(v);
                    return v;
                }
        );
        if (!isInferred && vertex.isInferred()) vertex.isInferred(false);
        return vertex;
    }

    public AttributeVertex<LocalDateTime> put(TypeVertex type, LocalDateTime value, boolean isInferred) {
        assert storage.isOpen();
        assert type.encoding().equals(ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(LocalDateTime.class);

        final AttributeVertex<LocalDateTime> vertex = attributesByIID.dateTimes.computeIfAbsent(
                new VertexIID.Attribute.DateTime(type.iid(), value),
                iid -> {
                    final AttributeVertex<LocalDateTime> v = new AttributeVertexImpl.DateTime(this, iid, isInferred);
                    thingsByTypeIID.computeIfAbsent(type.iid(), t -> new HashSet<>()).add(v);
                    return v;
                }
        );
        if (!isInferred && vertex.isInferred()) vertex.isInferred(false);
        return vertex;
    }

    public void delete(AttributeVertex<?> vertex) {
        assert storage.isOpen();
        attributesByIID.remove(vertex.iid());
        if (thingsByTypeIID.containsKey(vertex.type().iid())) {
            thingsByTypeIID.get(vertex.type().iid()).remove(vertex);
        }
    }

    public void delete(ThingVertex vertex) {
        assert storage.isOpen();
        if (!vertex.isAttribute()) {
            thingsByIID.remove(vertex.iid());
            if (thingsByTypeIID.containsKey(vertex.type().iid())) {
                thingsByTypeIID.get(vertex.type().iid()).remove(vertex);
            }
        } else delete(vertex.asAttribute());
    }

    public void setModified() {
        assert storage.isOpen();
        if (!isModified) isModified = true;
    }

    public boolean isModified() {
        return isModified;
    }

    @Override
    public void clear() {
        thingsByIID.clear();
        thingsByTypeIID.clear();
        attributesByIID.clear();
    }

    /**
     * Commits all the writes captured in this graph into storage.
     *
     * We start off by generating new IIDs for every {@code ThingVertex} (which
     * does not actually include {@code AttributeVertex}). We then write the every
     * {@code ThingVertex} onto the storage. Once all commit operations for every
     * {@code ThingVertex} is done, we the write all the {@code AttributeVertex}
     * as the last step. Since the write operations to storage are serialised
     * anyways, we don't need to parallelise the streams to commit the vertices.
     */
    @Override
    public void commit() {
        thingsByIID.values().parallelStream().filter(v -> v.status().equals(Encoding.Status.BUFFERED) && !v.isInferred()).forEach(
                vertex -> vertex.iid(generate(storage.dataKeyGenerator(), vertex.type().iid(), vertex.type().properLabel()))
        ); // thingByIID no longer contains valid mapping from IID to TypeVertex
        thingsByIID.values().stream().filter(v -> !v.isInferred()).forEach(Vertex::commit);
        attributesByIID.valueStream().forEach(Vertex::commit);

        clear(); // we now flush the indexes after commit, and we do not expect this Graph.Thing to be used again
    }

    private static class AttributesByIID {

        private final ConcurrentMap<VertexIID.Attribute.Boolean, AttributeVertex<Boolean>> booleans;
        private final ConcurrentMap<VertexIID.Attribute.Long, AttributeVertex<Long>> longs;
        private final ConcurrentMap<VertexIID.Attribute.Double, AttributeVertex<Double>> doubles;
        private final ConcurrentMap<VertexIID.Attribute.String, AttributeVertex<String>> strings;
        private final ConcurrentMap<VertexIID.Attribute.DateTime, AttributeVertex<LocalDateTime>> dateTimes;

        AttributesByIID() {
            booleans = new ConcurrentHashMap<>();
            longs = new ConcurrentHashMap<>();
            doubles = new ConcurrentHashMap<>();
            strings = new ConcurrentHashMap<>();
            dateTimes = new ConcurrentHashMap<>();
        }

        Stream<AttributeVertex<?>> valueStream() {
            return concat(booleans.values().stream(),
                          concat(longs.values().stream(),
                                 concat(doubles.values().stream(),
                                        concat(strings.values().stream(),
                                               dateTimes.values().stream()))));
        }

        void clear() {
            booleans.clear();
            longs.clear();
            doubles.clear();
            strings.clear();
            dateTimes.clear();
        }

        void remove(VertexIID.Attribute<?> iid) {
            switch (iid.valueType()) {
                // We need to manually cast all 'iid', to avoid warning from .remove()
                case BOOLEAN:
                    booleans.remove((VertexIID.Attribute.Boolean) iid);
                    break;
                case LONG:
                    longs.remove((VertexIID.Attribute.Long) iid);
                    break;
                case DOUBLE:
                    doubles.remove((VertexIID.Attribute.Double) iid);
                    break;
                case STRING:
                    strings.remove((VertexIID.Attribute.String) iid);
                    break;
                case DATETIME:
                    dateTimes.remove((VertexIID.Attribute.DateTime) iid);
                    break;
            }
        }

        ConcurrentMap<? extends VertexIID.Attribute<?>, ? extends AttributeVertex<?>> forValueType(Encoding.ValueType valueType) {
            switch (valueType) {
                case BOOLEAN:
                    return booleans;
                case LONG:
                    return longs;
                case DOUBLE:
                    return doubles;
                case STRING:
                    return strings;
                case DATETIME:
                    return dateTimes;
                default:
                    assert false;
                    return null;
            }
        }
    }

    public class Statistics { // TODO: implement properly

        private volatile long snapshot;

        public Statistics() {
            snapshot = 0; // TODO: initialise properly
        }

        // If you want to call this method concurrently, you need to convert 'snapshot' to AtomicLong
        @SuppressWarnings("NonAtomicOperationOnVolatileField")
        void incrementSnapshot() {
            snapshot++; // TODO: update properly
        }

        // If you want to call this method concurrently, you need to convert 'snapshot' to AtomicLong
        @SuppressWarnings("NonAtomicOperationOnVolatileField")
        public long snapshot() {
            return ++snapshot; // TODO: this is dummy code; properly update field and remove suppression
        }

        public long hasEdgeSum(TypeVertex owner, Set<TypeVertex> attributes) {
            return attributes.stream().map(att -> hasEdgeCount(owner, att)).mapToLong(l -> l).sum();
        }

        public long hasEdgeSum(Set<TypeVertex> owners, TypeVertex attribute) {
            return owners.stream().map(owner -> hasEdgeCount(owner, attribute)).mapToLong(l -> l).sum();
        }

        public long hasEdgeCount(TypeVertex owner, TypeVertex attribute) { // TODO: count properly
            return new Random(owner.hashCode()).nextInt(100);
        }

        public long thingVertexSum(Set<Label> labels) {
            return thingVertexSum(labels.stream().map(schemaGraph::getType));
        }

        public long thingVertexSum(Stream<TypeVertex> types) {
            return types.mapToLong(this::thingVertexCount).sum();
        }

        public long thingVertexMax(Set<Label> labels) {
            return thingVertexMax(labels.stream().map(schemaGraph::getType));
        }

        public long thingVertexMax(Stream<TypeVertex> types) {
            return types.mapToLong(this::thingVertexCount).max().orElse(0);
        }

        public long thingVertexCount(Label label) {
            return thingVertexCount(schemaGraph.getType(label));
        }

        public long thingVertexCount(TypeVertex type) {  // TODO: count properly
            return new Random(type.hashCode()).nextInt(1000);
        }

        public long thingVertexTransitiveCount(TypeVertex type) {  // TODO: count properly
            return new Random(type.hashCode()).nextInt(10_000);
        }

        public long thingVertexTransitiveMax(Set<Label> labels, Set<Label> filter) {
            return thingVertexTransitiveMax(labels.stream().map(schemaGraph::getType), filter);
        }

        public long thingVertexTransitiveMax(Stream<TypeVertex> types, Set<Label> filter) {
            return types.mapToLong(t -> tree(t, v -> v.ins().edge(SUB).from()
                    .filter(tf -> !filter.contains(tf.properLabel())))
                    .stream().mapToLong(this::thingVertexCount).sum()
            ).max().orElse(0);
        }
    }
}
