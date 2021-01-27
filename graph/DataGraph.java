/*
 * Copyright (C) 2021 Grakn Labs
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
import grakn.core.common.exception.GraknCheckedException;
import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Label;
import grakn.core.concurrent.common.ConcurrentSet;
import grakn.core.graph.common.Encoding;
import grakn.core.graph.common.KeyGenerator;
import grakn.core.graph.common.StatisticsBytes;
import grakn.core.graph.common.Storage;
import grakn.core.graph.iid.EdgeIID;
import grakn.core.graph.iid.PrefixIID;
import grakn.core.graph.iid.VertexIID;
import grakn.core.graph.vertex.AttributeVertex;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.TypeVertex;
import grakn.core.graph.vertex.Vertex;
import grakn.core.graph.vertex.impl.AttributeVertexImpl;
import grakn.core.graph.vertex.impl.ThingVertexImpl;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.pair;
import static grakn.common.util.Objects.className;
import static grakn.core.common.collection.Bytes.bytesToLong;
import static grakn.core.common.collection.Bytes.join;
import static grakn.core.common.collection.Bytes.longToBytes;
import static grakn.core.common.collection.Bytes.stripPrefix;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_STRING_SIZE;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.common.iterator.Iterators.link;
import static grakn.core.common.iterator.Iterators.tree;
import static grakn.core.graph.common.Encoding.Edge.Type.SUB;
import static grakn.core.graph.common.Encoding.Prefix.VERTEX_ATTRIBUTE_TYPE;
import static grakn.core.graph.common.Encoding.Prefix.VERTEX_ENTITY_TYPE;
import static grakn.core.graph.common.Encoding.Prefix.VERTEX_RELATION_TYPE;
import static grakn.core.graph.common.Encoding.Statistics.JobOperation.CREATED;
import static grakn.core.graph.common.Encoding.Statistics.JobOperation.DELETED;
import static grakn.core.graph.common.Encoding.Status.BUFFERED;
import static grakn.core.graph.common.Encoding.ValueType.STRING_MAX_SIZE;
import static grakn.core.graph.common.Encoding.Vertex.Thing.ATTRIBUTE;
import static grakn.core.graph.common.StatisticsBytes.attributeCountJobKey;
import static grakn.core.graph.common.StatisticsBytes.attributeCountedKey;
import static grakn.core.graph.common.StatisticsBytes.hasEdgeCountJobKey;
import static grakn.core.graph.common.StatisticsBytes.hasEdgeCountKey;
import static grakn.core.graph.common.StatisticsBytes.hasEdgeTotalCountKey;
import static grakn.core.graph.common.StatisticsBytes.snapshotKey;
import static grakn.core.graph.common.StatisticsBytes.vertexCountKey;
import static grakn.core.graph.common.StatisticsBytes.vertexTransitiveCountKey;
import static grakn.core.graph.iid.VertexIID.Thing.generate;

public class DataGraph implements Graph {

    private final Storage.Data storage;
    private final SchemaGraph schemaGraph;
    private final KeyGenerator.Data.Buffered keyGenerator;
    private final ConcurrentMap<VertexIID.Thing, ThingVertex> thingsByIID;
    private final ConcurrentMap<VertexIID.Type, ConcurrentSet<ThingVertex>> thingsByTypeIID;
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
        statistics = new Statistics(schemaGraph, storage);
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

    public ResourceIterator<ThingVertex> vertices() {
        return link(thingsByIID.values().iterator(), attributesByIID.valuesIterator());
    }

    public ThingVertex get(VertexIID.Thing iid) {
        assert storage.isOpen();
        if (iid.encoding().equals(ATTRIBUTE)) return get(iid.asAttribute());
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
        if (iid.encoding().equals(ATTRIBUTE)) return convert(iid.asAttribute());
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
        assert !typeVertex.isAttributeType();
        VertexIID.Thing iid = generate(keyGenerator, typeVertex.iid(), typeVertex.properLabel());
        ThingVertex vertex = new ThingVertexImpl.Buffered(this, iid, isInferred);
        thingsByIID.put(iid, vertex);
        thingsByTypeIID.computeIfAbsent(typeVertex.iid(), t -> new ConcurrentSet<>()).add(vertex);
        if (!isInferred) statistics.vertexCreated(typeVertex.iid());
        return vertex;
    }

    private <VALUE, ATT_IID extends VertexIID.Attribute<VALUE>, ATT_VERTEX extends AttributeVertex<VALUE>>
    ATT_VERTEX getOrReadFromStorage(Map<ATT_IID, ATT_VERTEX> map, ATT_IID attIID, Function<ATT_IID, ATT_VERTEX> vertexConstructor) {
        return map.computeIfAbsent(attIID, iid -> {
            byte[] val = storage.get(iid.bytes());
            if (val != null) return vertexConstructor.apply(iid);
            else return null;
        });
    }

    public ResourceIterator<ThingVertex> get(TypeVertex typeVertex) {
        ResourceIterator<ThingVertex> storageIterator = storage.iterate(
                join(typeVertex.iid().bytes(), Encoding.Edge.ISA.in().bytes()),
                (key, value) -> convert(EdgeIID.InwardsISA.of(key).end())
        );
        if (!thingsByTypeIID.containsKey(typeVertex.iid())) return storageIterator;
        else return link(thingsByTypeIID.get(typeVertex.iid()).iterator(), storageIterator).distinct();
    }

    public AttributeVertex<Boolean> get(TypeVertex type, boolean value) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(Boolean.class);

        return getOrReadFromStorage(
                attributesByIID.booleans,
                new VertexIID.Attribute.Boolean(type.iid(), value),
                iid -> new AttributeVertexImpl.Boolean(this, iid)
        );
    }

    public AttributeVertex<Long> get(TypeVertex type, long value) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(Long.class);

        return getOrReadFromStorage(
                attributesByIID.longs,
                new VertexIID.Attribute.Long(type.iid(), value),
                iid -> new AttributeVertexImpl.Long(this, iid)
        );
    }

    public AttributeVertex<Double> get(TypeVertex type, double value) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(Double.class);

        return getOrReadFromStorage(
                attributesByIID.doubles,
                new VertexIID.Attribute.Double(type.iid(), value),
                iid -> new AttributeVertexImpl.Double(this, iid)
        );
    }

    public AttributeVertex<String> get(TypeVertex type, String value) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(String.class);

        VertexIID.Attribute.String attIID;
        try {
            attIID = new VertexIID.Attribute.String(type.iid(), value);
        } catch (GraknCheckedException e) {
            if (e.code().isPresent() && e.code().get().equals(ILLEGAL_STRING_SIZE.code())) return null;
            else throw storage().exception(GraknException.of(e));
        }

        return getOrReadFromStorage(
                attributesByIID.strings, attIID,
                iid -> new AttributeVertexImpl.String(this, iid)
        );
    }

    public AttributeVertex<LocalDateTime> get(TypeVertex type, LocalDateTime value) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(LocalDateTime.class);

        return getOrReadFromStorage(
                attributesByIID.dateTimes,
                new VertexIID.Attribute.DateTime(type.iid(), value),
                iid -> new AttributeVertexImpl.DateTime(this, iid)
        );
    }

    public AttributeVertex<Boolean> put(TypeVertex type, boolean value, boolean isInferred) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(Boolean.class);

        AttributeVertex<Boolean> vertex = attributesByIID.booleans.computeIfAbsent(
                new VertexIID.Attribute.Boolean(type.iid(), value),
                iid -> {
                    AttributeVertex<Boolean> v = new AttributeVertexImpl.Boolean(this, iid, isInferred);
                    thingsByTypeIID.computeIfAbsent(type.iid(), t -> new ConcurrentSet<>()).add(v);
                    if (!isInferred) statistics.attributeVertexCreated(v.iid());
                    return v;
                }
        );
        if (!isInferred && vertex.isInferred()) {
            // promote inferred attribute to non-inferred attribute
            vertex.isInferred(false);
            statistics.attributeVertexCreated(vertex.iid());
        }
        return vertex;
    }

    public AttributeVertex<Long> put(TypeVertex type, long value, boolean isInferred) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(Long.class);

        AttributeVertex<Long> vertex = attributesByIID.longs.computeIfAbsent(
                new VertexIID.Attribute.Long(type.iid(), value),
                iid -> {
                    AttributeVertex<Long> v = new AttributeVertexImpl.Long(this, iid, isInferred);
                    thingsByTypeIID.computeIfAbsent(type.iid(), t -> new ConcurrentSet<>()).add(v);
                    if (!isInferred) statistics.attributeVertexCreated(v.iid());
                    return v;
                }
        );
        if (!isInferred && vertex.isInferred()) {
            // promote inferred attribute to non-inferred attribute
            vertex.isInferred(false);
            statistics.attributeVertexCreated(vertex.iid());
        }
        return vertex;
    }

    public AttributeVertex<Double> put(TypeVertex type, double value, boolean isInferred) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(Double.class);

        AttributeVertex<Double> vertex = attributesByIID.doubles.computeIfAbsent(
                new VertexIID.Attribute.Double(type.iid(), value),
                iid -> {
                    AttributeVertex<Double> v = new AttributeVertexImpl.Double(this, iid, isInferred);
                    thingsByTypeIID.computeIfAbsent(type.iid(), t -> new ConcurrentSet<>()).add(v);
                    if (!isInferred) statistics.attributeVertexCreated(v.iid());
                    return v;
                }
        );
        if (!isInferred && vertex.isInferred()) {
            // promote inferred attribute to non-inferred attribute
            vertex.isInferred(false);
            statistics.attributeVertexCreated(vertex.iid());
        }
        return vertex;
    }

    public AttributeVertex<String> put(TypeVertex type, String value, boolean isInferred) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(String.class);
        assert value.length() <= STRING_MAX_SIZE;

        VertexIID.Attribute.String attIID;
        try {
            attIID = new VertexIID.Attribute.String(type.iid(), value);
        } catch (GraknCheckedException e) {
            if (e.code().isPresent() && e.code().get().equals(ILLEGAL_STRING_SIZE.code())) {
                throw storage().exception(GraknException.of(ILLEGAL_STRING_SIZE, STRING_MAX_SIZE));
            } else {
                throw storage().exception(GraknException.of(e));
            }
        }

        AttributeVertex<String> vertex = attributesByIID.strings.computeIfAbsent(
                attIID, iid -> {
                    AttributeVertex<String> v = new AttributeVertexImpl.String(this, iid, isInferred);
                    thingsByTypeIID.computeIfAbsent(type.iid(), t -> new ConcurrentSet<>()).add(v);
                    if (!isInferred) statistics.attributeVertexCreated(v.iid());
                    return v;
                }
        );
        if (!isInferred && vertex.isInferred()) {
            // promote inferred attribute to non-inferred attribute
            vertex.isInferred(false);
            statistics.attributeVertexCreated(vertex.iid());
        }
        return vertex;
    }

    public AttributeVertex<LocalDateTime> put(TypeVertex type, LocalDateTime value, boolean isInferred) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(LocalDateTime.class);

        AttributeVertex<LocalDateTime> vertex = attributesByIID.dateTimes.computeIfAbsent(
                new VertexIID.Attribute.DateTime(type.iid(), value),
                iid -> {
                    AttributeVertex<LocalDateTime> v = new AttributeVertexImpl.DateTime(this, iid, isInferred);
                    thingsByTypeIID.computeIfAbsent(type.iid(), t -> new ConcurrentSet<>()).add(v);
                    if (!isInferred) statistics.attributeVertexCreated(v.iid());
                    return v;
                }
        );
        if (!isInferred && vertex.isInferred()) {
            // promote inferred attribute to non-inferred attribute
            vertex.isInferred(false);
            statistics.attributeVertexCreated(vertex.iid());
        }
        return vertex;
    }

    public void delete(AttributeVertex<?> vertex) {
        assert storage.isOpen();
        attributesByIID.remove(vertex.iid());
        if (thingsByTypeIID.containsKey(vertex.type().iid())) {
            thingsByTypeIID.get(vertex.type().iid()).remove(vertex);
        }
        if (!vertex.isInferred()) statistics.attributeVertexDeleted(vertex.iid());
    }

    public void delete(ThingVertex vertex) {
        assert storage.isOpen();
        if (!vertex.isAttribute()) {
            thingsByIID.remove(vertex.iid());
            if (thingsByTypeIID.containsKey(vertex.type().iid())) {
                thingsByTypeIID.get(vertex.type().iid()).remove(vertex);
            }
            if (!vertex.isInferred()) statistics.vertexDeleted(vertex.type().iid());
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
        statistics.clear();
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
        iterate(thingsByIID.values()).filter(v -> v.status().equals(BUFFERED) && !v.isInferred()).forEachRemaining(
                vertex -> vertex.iid(generate(storage.dataKeyGenerator(), vertex.type().iid(), vertex.type().properLabel()))
        ); // thingByIID no longer contains valid mapping from IID to TypeVertex
        thingsByIID.values().stream().filter(v -> !v.isInferred()).forEach(Vertex::commit);
        attributesByIID.valuesIterator().forEachRemaining(Vertex::commit);
        statistics.commit();

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

        ResourceIterator<AttributeVertex<?>> valuesIterator() {
            return link(list(
                    booleans.values().iterator(),
                    longs.values().iterator(),
                    doubles.values().iterator(),
                    strings.values().iterator(),
                    dateTimes.values().iterator()
            ));
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
                case BOOLEAN:
                    booleans.remove(iid.asBoolean());
                    break;
                case LONG:
                    longs.remove(iid.asLong());
                    break;
                case DOUBLE:
                    doubles.remove(iid.asDouble());
                    break;
                case STRING:
                    strings.remove(iid.asString());
                    break;
                case DATETIME:
                    dateTimes.remove(iid.asDateTime());
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

    public static class Statistics {

        private static int COUNT_JOB_BATCH_SIZE = 10000;
        private final ConcurrentMap<VertexIID.Type, Long> persistedVertexCount;
        private final ConcurrentMap<VertexIID.Type, Long> persistedVertexTransitiveCount;
        private final ConcurrentMap<VertexIID.Type, Long> deltaVertexCount;
        private final ConcurrentMap<Pair<VertexIID.Type, VertexIID.Type>, Long> persistedHasEdgeCount;
        private final ConcurrentMap<VertexIID.Type, Long> persistedHasEdgeTotalCount;
        private final ConcurrentMap<VertexIID.Attribute<?>, Encoding.Statistics.JobOperation> attributeVertexCountJobs;
        private final ConcurrentMap<Pair<VertexIID.Thing, VertexIID.Attribute<?>>, Encoding.Statistics.JobOperation> hasEdgeCountJobs;
        private boolean needsBackgroundCounting;
        private final SchemaGraph schemaGraph;
        private final Storage storage;
        private final long snapshot;

        public Statistics(SchemaGraph schemaGraph, Storage storage) {
            persistedVertexCount = new ConcurrentHashMap<>();
            persistedVertexTransitiveCount = new ConcurrentHashMap<>();
            deltaVertexCount = new ConcurrentHashMap<>();
            persistedHasEdgeCount = new ConcurrentHashMap<>();
            persistedHasEdgeTotalCount = new ConcurrentHashMap<>();
            attributeVertexCountJobs = new ConcurrentHashMap<>();
            hasEdgeCountJobs = new ConcurrentHashMap<>();
            needsBackgroundCounting = false;
            snapshot = bytesToLongOrZero(storage.get(snapshotKey()));
            this.schemaGraph = schemaGraph;
            this.storage = storage;
        }

        public long snapshot() {
            return snapshot;
        }

        public long hasEdgeSum(TypeVertex owner, Set<TypeVertex> attributes) {
            return attributes.stream().map(att -> hasEdgeCount(owner, att)).mapToLong(l -> l).sum();
        }

        public long hasEdgeSum(Set<TypeVertex> owners, TypeVertex attribute) {
            return owners.stream().map(owner -> hasEdgeCount(owner, attribute)).mapToLong(l -> l).sum();
        }

        public long hasEdgeCount(TypeVertex thing, TypeVertex attribute) {
            if (attribute.iid().equals(schemaGraph.rootAttributeType().iid())) {
                return hasEdgeTotalCount(thing.iid());
            } else {
                return hasEdgeCount(thing.iid(), attribute.iid());
            }
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

        public long thingVertexCount(TypeVertex type) {
            return vertexCount(type.iid(), false);
        }

        public long thingVertexTransitiveCount(TypeVertex type) {
            return vertexCount(type.iid(), true);
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

        public boolean needsBackgroundCounting() {
            return needsBackgroundCounting;
        }

        public void vertexCreated(VertexIID.Type typeIID) {
            deltaVertexCount.compute(typeIID, (k, v) -> (v == null ? 0 : v) + 1);
        }

        public void vertexDeleted(VertexIID.Type typeIID) {
            deltaVertexCount.compute(typeIID, (k, v) -> (v == null ? 0 : v) - 1);
        }

        public void attributeVertexCreated(VertexIID.Attribute<?> attIID) {
            attributeVertexCountJobs.put(attIID, CREATED);
            needsBackgroundCounting = true;
        }

        public void attributeVertexDeleted(VertexIID.Attribute<?> attIID) {
            attributeVertexCountJobs.put(attIID, DELETED);
            needsBackgroundCounting = true;
        }

        public void hasEdgeCreated(VertexIID.Thing thingIID, VertexIID.Attribute<?> attIID) {
            hasEdgeCountJobs.put(pair(thingIID, attIID), CREATED);
            needsBackgroundCounting = true;
        }

        public void hasEdgeDeleted(VertexIID.Thing thingIID, VertexIID.Attribute<?> attIID) {
            hasEdgeCountJobs.put(pair(thingIID, attIID), DELETED);
            needsBackgroundCounting = true;
        }

        private long vertexCount(VertexIID.Type typeIID, boolean isTransitive) {
            return persistedVertexCount(typeIID, isTransitive) + deltaVertexCount(typeIID);
        }

        private long deltaVertexCount(VertexIID.Type typeIID) {
            return deltaVertexCount.getOrDefault(typeIID, 0L);
        }

        private long hasEdgeCount(VertexIID.Type fromTypeIID, VertexIID.Type toTypeIID) {
            return persistedHasEdgeCount(fromTypeIID, toTypeIID);
        }

        private long hasEdgeTotalCount(VertexIID.Type rootTypeIID) {
            return persistedHasEdgeTotalCount(rootTypeIID);
        }

        private long persistedVertexCount(VertexIID.Type typeIID, boolean isTransitive) {
            if (isTransitive) {
                return persistedVertexTransitiveCount.computeIfAbsent(typeIID, iid -> {
                    if (isRootTypeIID(typeIID)) {
                        return bytesToLongOrZero(storage.get(vertexTransitiveCountKey(typeIID)));
                    } else {
                        ResourceIterator<TypeVertex> subTypes = schemaGraph.convert(typeIID).ins().edge(SUB).from();
                        long childrenPersistedCount = 0;
                        while (subTypes.hasNext()) {
                            TypeVertex subType = subTypes.next();
                            childrenPersistedCount += persistedVertexCount(subType.iid(), true);
                        }
                        long persistedCount = persistedVertexCount(typeIID, false);
                        return childrenPersistedCount + persistedCount;
                    }
                });
            } else {
                return persistedVertexCount.computeIfAbsent(typeIID, iid ->
                        bytesToLongOrZero(storage.get(vertexCountKey(typeIID))));
            }
        }

        private long persistedHasEdgeCount(VertexIID.Type thingTypeIID, VertexIID.Type attTypeIID) {
            return persistedHasEdgeCount.computeIfAbsent(pair(thingTypeIID, attTypeIID), iid ->
                    bytesToLongOrZero(storage.get(hasEdgeCountKey(thingTypeIID, attTypeIID))));
        }

        private long persistedHasEdgeTotalCount(VertexIID.Type rootTypeIID) {
            if (isRootTypeIID(rootTypeIID)) {
                return persistedHasEdgeTotalCount.computeIfAbsent(rootTypeIID, iid ->
                        bytesToLongOrZero(storage.get(hasEdgeTotalCountKey(rootTypeIID))));
            } else if (rootTypeIID.equals(schemaGraph.rootThingType().iid())) {
                return persistedHasEdgeTotalCount(schemaGraph.rootEntityType().iid()) +
                        persistedHasEdgeTotalCount(schemaGraph.rootRelationType().iid()) +
                        persistedHasEdgeTotalCount(schemaGraph.rootAttributeType().iid());
            } else {
                assert false;
                return 0;
            }
        }

        private boolean isRootTypeIID(VertexIID.Type typeIID) {
            return typeIID.equals(schemaGraph.rootEntityType().iid()) ||
                    typeIID.equals(schemaGraph.rootRelationType().iid()) ||
                    typeIID.equals(schemaGraph.rootAttributeType().iid()) ||
                    typeIID.equals(schemaGraph.rootRoleType().iid());
        }

        private void commit() {
            deltaVertexCount.forEach((typeIID, delta) -> {
                storage.mergeUntracked(vertexCountKey(typeIID), longToBytes(delta));
                if (typeIID.encoding().prefix() == VERTEX_ENTITY_TYPE) {
                    storage.mergeUntracked(vertexTransitiveCountKey(schemaGraph.rootEntityType().iid()), longToBytes(delta));
                } else if (typeIID.encoding().prefix() == VERTEX_RELATION_TYPE) {
                    storage.mergeUntracked(vertexTransitiveCountKey(schemaGraph.rootRelationType().iid()), longToBytes(delta));
                } else if (typeIID.encoding().prefix() == Encoding.Prefix.VERTEX_ROLE_TYPE) {
                    storage.mergeUntracked(vertexTransitiveCountKey(schemaGraph.rootRoleType().iid()), longToBytes(delta));
                }
            });
            attributeVertexCountJobs.forEach((attIID, countWorkValue) -> storage.putUntracked(
                    attributeCountJobKey(attIID), countWorkValue.bytes()
            ));
            hasEdgeCountJobs.forEach((hasEdge, countWorkValue) -> storage.putUntracked(
                    hasEdgeCountJobKey(hasEdge.first(), hasEdge.second()), countWorkValue.bytes()
            ));
            if (!deltaVertexCount.isEmpty()) {
                storage.mergeUntracked(snapshotKey(), longToBytes(1));
            }
        }

        private void clear() {
            persistedVertexCount.clear();
            persistedVertexTransitiveCount.clear();
            deltaVertexCount.clear();
            persistedHasEdgeCount.clear();
            attributeVertexCountJobs.clear();
            hasEdgeCountJobs.clear();
        }

        public boolean processCountJobs() {
            ResourceIterator<CountJob> countJobs = storage.iterate(StatisticsBytes.countJobKey(), CountJob::of);
            for (long processed = 0; processed < COUNT_JOB_BATCH_SIZE && countJobs.hasNext(); processed++) {
                CountJob countJob = countJobs.next();
                if (countJob instanceof CountJob.Attribute) {
                    processAttributeCountJob(countJob);
                } else if (countJob instanceof CountJob.HasEdge) {
                    processHasEdgeCountJob(countJob);
                } else {
                    assert false;
                }
                storage.delete(countJob.key());
            }
            storage.mergeUntracked(snapshotKey(), longToBytes(1));
            return countJobs.hasNext();
        }

        private void processAttributeCountJob(CountJob countJob) {
            VertexIID.Attribute<?> attIID = countJob.asAttribute().attIID();
            if (countJob.value() == CREATED) {
                processAttributeCreatedCountJob(attIID);
            } else if (countJob.value() == DELETED) {
                processAttributeDeletedCountJob(attIID);
            } else {
                assert false;
            }
        }

        private void processAttributeCreatedCountJob(VertexIID.Attribute<?> attIID) {
            byte[] counted = storage.get(attributeCountedKey(attIID));
            if (counted == null) {
                storage.mergeUntracked(vertexCountKey(attIID.type()), longToBytes(1));
                storage.mergeUntracked(vertexTransitiveCountKey(schemaGraph.rootAttributeType().iid()), longToBytes(1));
                storage.put(attributeCountedKey(attIID));
            }
        }

        private void processAttributeDeletedCountJob(VertexIID.Attribute<?> attIID) {
            byte[] counted = storage.get(attributeCountedKey(attIID));
            if (counted != null) {
                storage.mergeUntracked(vertexCountKey(attIID.type()), longToBytes(-1));
                storage.mergeUntracked(vertexTransitiveCountKey(schemaGraph.rootAttributeType().iid()), longToBytes(-1));
                storage.delete(attributeCountedKey(attIID));
            }
        }

        private void processHasEdgeCountJob(CountJob countJob) {
            VertexIID.Thing thingIID = countJob.asHasEdge().thingIID();
            VertexIID.Attribute<?> attIID = countJob.asHasEdge().attIID();
            if (countJob.value() == CREATED) {
                processHasEdgeCreatedCountJob(thingIID, attIID);
            } else if (countJob.value() == DELETED) {
                processHasEdgeDeletedCountJob(thingIID, attIID);
            } else {
                assert false;
            }
        }

        private void processHasEdgeCreatedCountJob(VertexIID.Thing thingIID, VertexIID.Attribute<?> attIID) {
            byte[] counted = storage.get(StatisticsBytes.hasEdgeCountedKey(thingIID, attIID));
            if (counted == null) {
                storage.mergeUntracked(hasEdgeCountKey(thingIID.type(), attIID.type()), longToBytes(1));
                if (thingIID.type().encoding().prefix() == VERTEX_ENTITY_TYPE) {
                    storage.mergeUntracked(hasEdgeTotalCountKey(schemaGraph.rootEntityType().iid()), longToBytes(1));
                } else if (thingIID.type().encoding().prefix() == VERTEX_RELATION_TYPE) {
                    storage.mergeUntracked(hasEdgeTotalCountKey(schemaGraph.rootRelationType().iid()), longToBytes(1));
                } else if (thingIID.type().encoding().prefix() == VERTEX_ATTRIBUTE_TYPE) {
                    storage.mergeUntracked(hasEdgeTotalCountKey(schemaGraph.rootAttributeType().iid()), longToBytes(1));
                }
                storage.put(StatisticsBytes.hasEdgeCountedKey(thingIID, attIID));
            }
        }

        private void processHasEdgeDeletedCountJob(VertexIID.Thing thingIID, VertexIID.Attribute<?> attIID) {
            byte[] counted = storage.get(StatisticsBytes.hasEdgeCountedKey(thingIID, attIID));
            if (counted != null) {
                storage.mergeUntracked(hasEdgeCountKey(thingIID.type(), attIID.type()), longToBytes(-1));
                if (thingIID.type().encoding().prefix() == VERTEX_ENTITY_TYPE) {
                    storage.mergeUntracked(hasEdgeTotalCountKey(schemaGraph.rootEntityType().iid()), longToBytes(-1));
                } else if (thingIID.type().encoding().prefix() == VERTEX_RELATION_TYPE) {
                    storage.mergeUntracked(hasEdgeTotalCountKey(schemaGraph.rootRelationType().iid()), longToBytes(-1));
                } else if (thingIID.type().encoding().prefix() == VERTEX_ATTRIBUTE_TYPE) {
                    storage.mergeUntracked(hasEdgeTotalCountKey(schemaGraph.rootAttributeType().iid()), longToBytes(-1));
                }
                storage.delete(StatisticsBytes.hasEdgeCountedKey(thingIID, attIID));
            }
        }

        private long bytesToLongOrZero(byte[] bytes) {
            return bytes != null ? bytesToLong(bytes) : 0;
        }

        public abstract static class CountJob {
            private final Encoding.Statistics.JobOperation value;
            private final byte[] key;

            private CountJob(byte[] key, Encoding.Statistics.JobOperation value) {
                this.key = key;
                this.value = value;
            }

            public static CountJob of(byte[] key, byte[] value) {
                byte[] countJobKey = stripPrefix(key, PrefixIID.LENGTH);
                Encoding.Statistics.JobType jobType = Encoding.Statistics.JobType.of(new byte[]{countJobKey[0]});
                Encoding.Statistics.JobOperation jobOperation = Encoding.Statistics.JobOperation.of(value);
                byte[] countJobIID = stripPrefix(countJobKey, PrefixIID.LENGTH);
                if (jobType == Encoding.Statistics.JobType.ATTRIBUTE_VERTEX) {
                    VertexIID.Attribute<?> attIID = VertexIID.Attribute.of(countJobIID);
                    return new Attribute(key, attIID, jobOperation);
                } else if (jobType == Encoding.Statistics.JobType.HAS_EDGE) {
                    VertexIID.Thing thingIID = VertexIID.Thing.extract(countJobIID, 0);
                    VertexIID.Attribute<?> attIID = VertexIID.Attribute.extract(countJobIID, thingIID.bytes().length);
                    return new HasEdge(key, thingIID, attIID, jobOperation);
                } else {
                    assert false;
                    return null;
                }
            }

            public byte[] key() {
                return key;
            }

            public Encoding.Statistics.JobOperation value() {
                return value;
            }

            public Attribute asAttribute() {
                throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(Attribute.class));
            }

            public HasEdge asHasEdge() {
                throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(HasEdge.class));
            }

            public static class Attribute extends CountJob {
                private final VertexIID.Attribute<?> attIID;

                private Attribute(byte[] key, VertexIID.Attribute<?> attIID, Encoding.Statistics.JobOperation value) {
                    super(key, value);
                    this.attIID = attIID;
                }

                public VertexIID.Attribute<?> attIID() {
                    return attIID;
                }

                @Override
                public Attribute asAttribute() {
                    return this;
                }
            }

            public static class HasEdge extends CountJob {
                private final VertexIID.Thing thingIID;
                private final VertexIID.Attribute<?> attIID;

                private HasEdge(byte[] key, VertexIID.Thing thingIID, VertexIID.Attribute<?> attIID,
                                Encoding.Statistics.JobOperation value) {
                    super(key, value);
                    this.thingIID = thingIID;
                    this.attIID = attIID;
                }

                public VertexIID.Thing thingIID() {
                    return thingIID;
                }

                public VertexIID.Attribute<?> attIID() {
                    return attIID;
                }

                @Override
                public HasEdge asHasEdge() {
                    return this;
                }
            }
        }
    }
}
