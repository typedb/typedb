/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.graph;

import com.vaticle.typedb.common.collection.ConcurrentSet;
import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBCheckedException;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.common.KeyGenerator;
import com.vaticle.typedb.core.graph.common.StatisticsBytes;
import com.vaticle.typedb.core.graph.common.Storage;
import com.vaticle.typedb.core.graph.iid.EdgeIID;
import com.vaticle.typedb.core.graph.iid.IID;
import com.vaticle.typedb.core.graph.iid.PrefixIID;
import com.vaticle.typedb.core.graph.iid.VertexIID;
import com.vaticle.typedb.core.graph.vertex.AttributeVertex;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.graph.vertex.impl.AttributeVertexImpl;
import com.vaticle.typedb.core.graph.vertex.impl.ThingVertexImpl;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.pair;
import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.collection.ByteArray.encodeLong;
import static com.vaticle.typedb.core.common.collection.ByteArray.join;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_STRING_SIZE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.Iterators.link;
import static com.vaticle.typedb.core.common.iterator.Iterators.tree;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.SUB;
import static com.vaticle.typedb.core.graph.common.Encoding.Prefix.VERTEX_ATTRIBUTE_TYPE;
import static com.vaticle.typedb.core.graph.common.Encoding.Prefix.VERTEX_ENTITY_TYPE;
import static com.vaticle.typedb.core.graph.common.Encoding.Prefix.VERTEX_RELATION_TYPE;
import static com.vaticle.typedb.core.graph.common.Encoding.Statistics.JobOperation.CREATED;
import static com.vaticle.typedb.core.graph.common.Encoding.Statistics.JobOperation.DELETED;
import static com.vaticle.typedb.core.graph.common.Encoding.Status.BUFFERED;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.STRING_MAX_SIZE;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Thing.ATTRIBUTE;
import static com.vaticle.typedb.core.graph.common.StatisticsBytes.attributeCountJobKey;
import static com.vaticle.typedb.core.graph.common.StatisticsBytes.attributeCountedKey;
import static com.vaticle.typedb.core.graph.common.StatisticsBytes.hasEdgeCountJobKey;
import static com.vaticle.typedb.core.graph.common.StatisticsBytes.hasEdgeCountKey;
import static com.vaticle.typedb.core.graph.common.StatisticsBytes.hasEdgeCountedKey;
import static com.vaticle.typedb.core.graph.common.StatisticsBytes.hasEdgeTotalCountKey;
import static com.vaticle.typedb.core.graph.common.StatisticsBytes.snapshotKey;
import static com.vaticle.typedb.core.graph.common.StatisticsBytes.vertexCountKey;
import static com.vaticle.typedb.core.graph.common.StatisticsBytes.vertexTransitiveCountKey;
import static com.vaticle.typedb.core.graph.iid.VertexIID.Thing.generate;

public class ThingGraph {

    private final Storage.Data storage;
    private final TypeGraph typeGraph;
    private final KeyGenerator.Data.Buffered keyGenerator;
    private final ConcurrentMap<VertexIID.Thing, ThingVertex.Write> thingsByIID;
    private final ConcurrentMap<VertexIID.Type, ConcurrentSet<ThingVertex.Write>> thingsByTypeIID;
    private final AttributesByIID attributesByIID;
    private final Statistics statistics;
    private boolean isModified;

    public ThingGraph(Storage.Data storage, TypeGraph typeGraph) {
        this.storage = storage;
        this.typeGraph = typeGraph;
        keyGenerator = new KeyGenerator.Data.Buffered();
        thingsByIID = new ConcurrentHashMap<>();
        thingsByTypeIID = new ConcurrentHashMap<>();
        attributesByIID = new AttributesByIID();
        statistics = new Statistics(typeGraph, storage);
    }

    public Storage.Data storage() {
        return storage;
    }

    public TypeGraph type() {
        return typeGraph;
    }

    public ThingGraph.Statistics stats() {
        return statistics;
    }

    public FunctionalIterator<ThingVertex.Write> vertices() {
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

    public ThingVertex.Write getWritable(VertexIID.Thing iid) {
        assert storage.isOpen();
        if (iid.encoding().equals(ATTRIBUTE)) return getWritable(iid.asAttribute());
        else if (!thingsByIID.containsKey(iid) && storage.get(iid.bytes()) == null) return null;
        return convertWritable(iid);
    }

    public AttributeVertex.Write<?> getWritable(VertexIID.Attribute<?> iid) {
        if (!attributesByIID.forValueType(iid.valueType()).containsKey(iid) && storage.get(iid.bytes()) == null) {
            return null;
        }
        return convertWritable(iid);
    }

    public ThingVertex convert(VertexIID.Thing iid) {
        assert storage.isOpen();
        if (iid.encoding().equals(ATTRIBUTE)) return convert(iid.asAttribute());
        else {
            ThingVertex.Write vertex;
            if ((vertex = thingsByIID.get(iid)) != null) return vertex;
            else return new ThingVertexImpl.Read(this, iid);
        }
    }

    public AttributeVertex<?> convert(VertexIID.Attribute<?> attIID) {
        AttributeVertex<?> vertex;
        switch (attIID.valueType()) {
            case BOOLEAN:
                vertex = attributesByIID.booleans.get(attIID.asBoolean());
                if (vertex == null) return new AttributeVertexImpl.Read.Boolean(this, attIID.asBoolean());
                else return vertex;
            case LONG:
                vertex = attributesByIID.longs.get(attIID.asLong());
                if (vertex == null) return new AttributeVertexImpl.Read.Long(this, attIID.asLong());
                else return vertex;
            case DOUBLE:
                vertex = attributesByIID.doubles.get(attIID.asDouble());
                if (vertex == null) return new AttributeVertexImpl.Read.Double(this, attIID.asDouble());
                else return vertex;
            case STRING:
                vertex = attributesByIID.strings.get(attIID.asString());
                if (vertex == null) return new AttributeVertexImpl.Read.String(this, attIID.asString());
                else return vertex;
            case DATETIME:
                vertex = attributesByIID.dateTimes.get(attIID.asDateTime());
                if (vertex == null) return new AttributeVertexImpl.Read.DateTime(this, attIID.asDateTime());
                else return vertex;
            default:
                assert false;
                return null;
        }
    }

    public ThingVertex.Write convertWritable(VertexIID.Thing iid) {
        assert storage.isOpen();
        if (iid.encoding().equals(ATTRIBUTE)) return convertWritable(iid.asAttribute());
        else return thingsByIID.computeIfAbsent(iid, i -> ThingVertexImpl.Write.of(this, i));
    }

    public AttributeVertex.Write<?> convertWritable(VertexIID.Attribute<?> attIID) {
        switch (attIID.valueType()) {
            case BOOLEAN:
                return attributesByIID.booleans.computeIfAbsent(attIID.asBoolean(),
                        i -> new AttributeVertexImpl.Write.Boolean(this, attIID.asBoolean()));
            case LONG:
                return attributesByIID.longs.computeIfAbsent(attIID.asLong(),
                        i -> new AttributeVertexImpl.Write.Long(this, attIID.asLong()));
            case DOUBLE:
                return attributesByIID.doubles.computeIfAbsent(attIID.asDouble(),
                        i -> new AttributeVertexImpl.Write.Double(this, attIID.asDouble()));
            case STRING:
                return attributesByIID.strings.computeIfAbsent(attIID.asString(),
                        i -> new AttributeVertexImpl.Write.String(this, attIID.asString()));
            case DATETIME:
                return attributesByIID.dateTimes.computeIfAbsent(attIID.asDateTime(),
                        i -> new AttributeVertexImpl.Write.DateTime(this, attIID.asDateTime()));
            default:
                assert false;
                return null;
        }
    }

    public ThingVertex.Write create(TypeVertex typeVertex, boolean isInferred) {
        assert storage.isOpen();
        assert !typeVertex.isAttributeType();
        VertexIID.Thing iid = generate(keyGenerator, typeVertex.iid(), typeVertex.properLabel());
        ThingVertex.Write vertex = new ThingVertexImpl.Write.Buffered(this, iid, isInferred);
        thingsByIID.put(iid, vertex);
        thingsByTypeIID.computeIfAbsent(typeVertex.iid(), t -> new ConcurrentSet<>()).add(vertex);
        if (!isInferred) statistics.vertexCreated(typeVertex.iid());
        return vertex;
    }

    private <VALUE, ATT_IID extends VertexIID.Attribute<VALUE>, ATT_VERTEX extends AttributeVertex<VALUE>>
    ATT_VERTEX getOrReadFromStorage(Map<ATT_IID, ? extends ATT_VERTEX> map, ATT_IID attIID, Function<ATT_IID, ATT_VERTEX> vertexConstructor) {
        ATT_VERTEX vertex = map.get(attIID);
        if (vertex == null && storage.get(attIID.bytes()) != null) {
            return vertexConstructor.apply(attIID);
        } else {
            return vertex;
        }
    }

    public FunctionalIterator<ThingVertex> get(TypeVertex typeVertex) {
        FunctionalIterator<ThingVertex> storageIterator = storage.iterate(
                join(typeVertex.iid().bytes(), Encoding.Edge.ISA.in().bytes()),
                (key, value) -> convert(EdgeIID.InwardsISA.of(key).end())
        );
        if (!thingsByTypeIID.containsKey(typeVertex.iid())) return storageIterator;
        else return link(thingsByTypeIID.get(typeVertex.iid()).iterator(), storageIterator).distinct();
    }

    public FunctionalIterator<ThingVertex.Write> getWritable(TypeVertex typeVertex) {
        FunctionalIterator<ThingVertex.Write> storageIterator = storage.iterate(
                join(typeVertex.iid().bytes(), Encoding.Edge.ISA.in().bytes()),
                (key, value) -> convertWritable(EdgeIID.InwardsISA.of(key).end())
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
                iid -> new AttributeVertexImpl.Read.Boolean(this, iid)
        );
    }

    public AttributeVertex<Long> get(TypeVertex type, long value) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(Long.class);

        return getOrReadFromStorage(
                attributesByIID.longs,
                new VertexIID.Attribute.Long(type.iid(), value),
                iid -> new AttributeVertexImpl.Read.Long(this, iid)
        );
    }

    public AttributeVertex<Double> get(TypeVertex type, double value) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(Double.class);

        return getOrReadFromStorage(
                attributesByIID.doubles,
                new VertexIID.Attribute.Double(type.iid(), value),
                iid -> new AttributeVertexImpl.Read.Double(this, iid)
        );
    }

    public AttributeVertex<String> get(TypeVertex type, String value) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(String.class);

        VertexIID.Attribute.String attIID;
        try {
            attIID = new VertexIID.Attribute.String(type.iid(), value);
        } catch (TypeDBCheckedException e) {
            if (e.code().isPresent() && e.code().get().equals(ILLEGAL_STRING_SIZE.code())) return null;
            else throw storage().exception(TypeDBException.of(e));
        }

        return getOrReadFromStorage(
                attributesByIID.strings, attIID,
                iid -> new AttributeVertexImpl.Read.String(this, iid)
        );
    }

    public AttributeVertex<LocalDateTime> get(TypeVertex type, LocalDateTime value) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(LocalDateTime.class);

        return getOrReadFromStorage(
                attributesByIID.dateTimes,
                new VertexIID.Attribute.DateTime(type.iid(), value),
                iid -> new AttributeVertexImpl.Read.DateTime(this, iid)
        );
    }

    public AttributeVertex.Write<Boolean> put(TypeVertex type, boolean value, boolean isInferred) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(Boolean.class);

        boolean[] isNewVertex = new boolean[]{false};
        AttributeVertex.Write<Boolean> vertex = attributesByIID.booleans.computeIfAbsent(
                new VertexIID.Attribute.Boolean(type.iid(), value),
                iid -> {
                    AttributeVertexImpl.Write.Boolean v = new AttributeVertexImpl.Write.Boolean(this, iid, isInferred);
                    thingsByTypeIID.computeIfAbsent(type.iid(), t -> new ConcurrentSet<>()).add(v);
                    isNewVertex[0] = !v.isPersisted();
                    return v;
                }
        );
        if (!isInferred && (isNewVertex[0] || vertex.isInferred())) {
            vertex.isInferred(false);
            statistics.attributeVertexCreated(vertex.iid());
        }
        return vertex;
    }

    public AttributeVertex.Write<Long> put(TypeVertex type, long value, boolean isInferred) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(Long.class);

        boolean[] isNewVertex = new boolean[]{false};
        AttributeVertex.Write<Long> vertex = attributesByIID.longs.computeIfAbsent(
                new VertexIID.Attribute.Long(type.iid(), value),
                iid -> {
                    AttributeVertexImpl.Write.Long v = new AttributeVertexImpl.Write.Long(this, iid, isInferred);
                    thingsByTypeIID.computeIfAbsent(type.iid(), t -> new ConcurrentSet<>()).add(v);
                    isNewVertex[0] = !v.isPersisted();
                    return v;
                }
        );

        if (!isInferred && (isNewVertex[0] || vertex.isInferred())) {
            vertex.isInferred(false);
            statistics.attributeVertexCreated(vertex.iid());
        }

        return vertex;
    }

    public AttributeVertex.Write<Double> put(TypeVertex type, double value, boolean isInferred) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(Double.class);

        boolean[] isNewVertex = new boolean[]{false};
        AttributeVertex.Write<Double> vertex = attributesByIID.doubles.computeIfAbsent(
                new VertexIID.Attribute.Double(type.iid(), value),
                iid -> {
                    AttributeVertexImpl.Write.Double v = new AttributeVertexImpl.Write.Double(this, iid, isInferred);
                    thingsByTypeIID.computeIfAbsent(type.iid(), t -> new ConcurrentSet<>()).add(v);
                    isNewVertex[0] = !v.isPersisted();
                    return v;
                }
        );
        if (!isInferred && (isNewVertex[0] || vertex.isInferred())) {
            vertex.isInferred(false);
            statistics.attributeVertexCreated(vertex.iid());
        }
        return vertex;
    }

    public AttributeVertex.Write<String> put(TypeVertex type, String value, boolean isInferred) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(String.class);
        assert value.length() <= STRING_MAX_SIZE;

        VertexIID.Attribute.String attIID;
        try {
            attIID = new VertexIID.Attribute.String(type.iid(), value);
        } catch (TypeDBCheckedException e) {
            if (e.code().isPresent() && e.code().get().equals(ILLEGAL_STRING_SIZE.code())) {
                throw storage().exception(TypeDBException.of(ILLEGAL_STRING_SIZE, STRING_MAX_SIZE));
            } else {
                throw storage().exception(TypeDBException.of(e));
            }
        }

        boolean[] isNewVertex = new boolean[]{false};
        AttributeVertex.Write<String> vertex = attributesByIID.strings.computeIfAbsent(
                attIID, iid -> {
                    AttributeVertexImpl.Write.String v = new AttributeVertexImpl.Write.String(this, iid, isInferred);
                    thingsByTypeIID.computeIfAbsent(type.iid(), t -> new ConcurrentSet<>()).add(v);
                    isNewVertex[0] = !v.isPersisted();
                    return v;
                }
        );

        if (!isInferred && (isNewVertex[0] || vertex.isInferred())) {
            vertex.isInferred(false);
            statistics.attributeVertexCreated(vertex.iid());
        }
        return vertex;
    }

    public AttributeVertex.Write<LocalDateTime> put(TypeVertex type, LocalDateTime value, boolean isInferred) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(LocalDateTime.class);

        boolean[] isNewVertex = new boolean[]{false};
        AttributeVertex.Write<LocalDateTime> vertex = attributesByIID.dateTimes.computeIfAbsent(
                new VertexIID.Attribute.DateTime(type.iid(), value),
                iid -> {
                    AttributeVertexImpl.Write.DateTime v = new AttributeVertexImpl.Write.DateTime(this, iid, isInferred);
                    thingsByTypeIID.computeIfAbsent(type.iid(), t -> new ConcurrentSet<>()).add(v);
                    isNewVertex[0] = !v.isPersisted();
                    return v;
                }
        );

        if (!isInferred && (isNewVertex[0] || vertex.isInferred())) {
            vertex.isInferred(false);
            statistics.attributeVertexCreated(vertex.iid());
        }
        return vertex;
    }

    public void delete(AttributeVertex.Write<?> vertex) {
        assert storage.isOpen();
        attributesByIID.remove(vertex.iid());
        if (thingsByTypeIID.containsKey(vertex.type().iid())) {
            thingsByTypeIID.get(vertex.type().iid()).remove(vertex);
        }
        if (!vertex.isInferred()) statistics.attributeVertexDeleted(vertex.iid());
    }

    public void delete(ThingVertex.Write vertex) {
        assert storage.isOpen();
        if (!vertex.isAttribute()) {
            thingsByIID.remove(vertex.iid());
            if (thingsByTypeIID.containsKey(vertex.type().iid())) {
                thingsByTypeIID.get(vertex.type().iid()).remove(vertex);
            }
            if (!vertex.isInferred()) statistics.vertexDeleted(vertex.type().iid());
        } else delete(vertex.asAttribute().asWrite());
    }

    public void exclusiveOwnership(TypeVertex ownerType, AttributeVertex<?> attribute) {
        storage.trackExclusiveCreate(join(ownerType.iid().bytes(), attribute.iid().bytes()));
    }

    public void setModified(IID iid) {
        assert storage.isOpen();
        if (!isModified) isModified = true;
        storage.trackModified(iid.bytes());
    }

    public boolean isModified() {
        return isModified;
    }

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
    public void commit() {
        Map<VertexIID.Thing, VertexIID.Thing> bufferedToPersistedIIDs = new HashMap<>();
        iterate(thingsByIID.values()).filter(v -> v.status().equals(BUFFERED) && !v.isInferred()).forEachRemaining(v -> {
            VertexIID.Thing newIID = generate(storage.dataKeyGenerator(), v.type().iid(), v.type().properLabel());
            bufferedToPersistedIIDs.put(v.iid(), newIID);
            v.iid(newIID);
        }); // thingsByIID no longer contains valid mapping from IID to TypeVertex
        thingsByIID.values().stream().filter(v -> !v.isInferred()).forEach(ThingVertex.Write::commit);
        attributesByIID.valuesIterator().forEachRemaining(AttributeVertex.Write::commit);
        statistics.commit(bufferedToPersistedIIDs);

        clear(); // we now flush the indexes after commit, and we do not expect this Graph.Thing to be used again
    }

    private static class AttributesByIID {

        private final ConcurrentMap<VertexIID.Attribute.Boolean, AttributeVertex.Write<Boolean>> booleans;
        private final ConcurrentMap<VertexIID.Attribute.Long, AttributeVertex.Write<Long>> longs;
        private final ConcurrentMap<VertexIID.Attribute.Double, AttributeVertex.Write<Double>> doubles;
        private final ConcurrentMap<VertexIID.Attribute.String, AttributeVertex.Write<String>> strings;
        private final ConcurrentMap<VertexIID.Attribute.DateTime, AttributeVertex.Write<LocalDateTime>> dateTimes;

        AttributesByIID() {
            booleans = new ConcurrentHashMap<>();
            longs = new ConcurrentHashMap<>();
            doubles = new ConcurrentHashMap<>();
            strings = new ConcurrentHashMap<>();
            dateTimes = new ConcurrentHashMap<>();
        }

        FunctionalIterator<AttributeVertex.Write<?>> valuesIterator() {
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

        private static final int COUNT_JOB_BATCH_SIZE = 10_000;
        private final ConcurrentMap<VertexIID.Type, Long> persistedVertexCount;
        private final ConcurrentMap<VertexIID.Type, Long> persistedVertexTransitiveCount;
        private final ConcurrentMap<VertexIID.Type, Long> deltaVertexCount;
        private final ConcurrentMap<Pair<VertexIID.Type, VertexIID.Type>, Long> persistedHasEdgeCount;
        private final ConcurrentMap<VertexIID.Type, Long> persistedHasEdgeTotalCount;
        private final ConcurrentMap<VertexIID.Attribute<?>, Encoding.Statistics.JobOperation> attributeVertexCountJobs;
        private final ConcurrentMap<Pair<VertexIID.Thing, VertexIID.Attribute<?>>, Encoding.Statistics.JobOperation> hasEdgeCountJobs;
        private boolean needsBackgroundCounting;
        private final TypeGraph typeGraph;
        private final Storage.Data storage;
        private final long snapshot;

        public Statistics(TypeGraph typeGraph, Storage.Data storage) {
            persistedVertexCount = new ConcurrentHashMap<>();
            persistedVertexTransitiveCount = new ConcurrentHashMap<>();
            deltaVertexCount = new ConcurrentHashMap<>();
            persistedHasEdgeCount = new ConcurrentHashMap<>();
            persistedHasEdgeTotalCount = new ConcurrentHashMap<>();
            attributeVertexCountJobs = new ConcurrentHashMap<>();
            hasEdgeCountJobs = new ConcurrentHashMap<>();
            needsBackgroundCounting = false;
            snapshot = bytesToLongOrZero(storage.get(snapshotKey()));
            this.typeGraph = typeGraph;
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
            if (attribute.iid().equals(typeGraph.rootAttributeType().iid())) {
                return hasEdgeTotalCount(thing.iid());
            } else {
                return hasEdgeCount(thing.iid(), attribute.iid());
            }
        }

        public long hasEdgeCount(Label thing, Label attribute) {
            return hasEdgeCount(typeGraph.getType(thing), typeGraph.getType(attribute));
        }

        public long thingVertexSum(Set<Label> labels) {
            return thingVertexSum(labels.stream().map(typeGraph::getType));
        }

        public long thingVertexSum(Stream<TypeVertex> types) {
            return types.mapToLong(this::thingVertexCount).sum();
        }

        public long thingVertexMax(Set<Label> labels) {
            return thingVertexMax(labels.stream().map(typeGraph::getType));
        }

        public long thingVertexMax(Stream<TypeVertex> types) {
            return types.mapToLong(this::thingVertexCount).max().orElse(0);
        }

        public long thingVertexCount(Label label) {
            return thingVertexCount(typeGraph.getType(label));
        }

        public long thingVertexCount(TypeVertex type) {
            return vertexCount(type.iid(), false);
        }

        public long thingVertexTransitiveCount(TypeVertex type) {
            return vertexCount(type.iid(), true);
        }

        public long thingVertexTransitiveMax(Set<Label> labels, Set<Label> filter) {
            return thingVertexTransitiveMax(labels.stream().map(typeGraph::getType), filter);
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
                if (isRootTypeIID(typeIID)) {
                    return persistedVertexCount.computeIfAbsent(typeIID, iid ->
                            bytesToLongOrZero(storage.get(vertexTransitiveCountKey(typeIID))));
                } else {
                    if (persistedVertexTransitiveCount.containsKey(typeIID)) {
                        return persistedVertexTransitiveCount.get(typeIID);
                    } else {
                        FunctionalIterator<TypeVertex> subTypes = typeGraph.convert(typeIID).ins().edge(SUB).from();
                        long childrenPersistedCount = 0;
                        while (subTypes.hasNext()) {
                            TypeVertex subType = subTypes.next();
                            childrenPersistedCount += persistedVertexCount(subType.iid(), true);
                        }
                        long persistedCount = persistedVertexCount(typeIID, false);
                        persistedVertexTransitiveCount.put(typeIID, childrenPersistedCount + persistedCount);
                        return childrenPersistedCount + persistedCount;
                    }
                }
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
            } else if (rootTypeIID.equals(typeGraph.rootThingType().iid())) {
                return persistedHasEdgeTotalCount(typeGraph.rootEntityType().iid()) +
                        persistedHasEdgeTotalCount(typeGraph.rootRelationType().iid()) +
                        persistedHasEdgeTotalCount(typeGraph.rootAttributeType().iid());
            } else {
                assert false;
                return 0;
            }
        }

        private boolean isRootTypeIID(VertexIID.Type typeIID) {
            return typeIID.equals(typeGraph.rootEntityType().iid()) ||
                    typeIID.equals(typeGraph.rootRelationType().iid()) ||
                    typeIID.equals(typeGraph.rootAttributeType().iid()) ||
                    typeIID.equals(typeGraph.rootRoleType().iid());
        }

        private void commit(Map<VertexIID.Thing, VertexIID.Thing> IIDMap) {
            deltaVertexCount.forEach((typeIID, delta) -> {
                storage.mergeUntracked(vertexCountKey(typeIID), encodeLong(delta));
                if (typeIID.encoding().prefix() == VERTEX_ENTITY_TYPE) {
                    storage.mergeUntracked(vertexTransitiveCountKey(typeGraph.rootEntityType().iid()), encodeLong(delta));
                } else if (typeIID.encoding().prefix() == VERTEX_RELATION_TYPE) {
                    storage.mergeUntracked(vertexTransitiveCountKey(typeGraph.rootRelationType().iid()), encodeLong(delta));
                } else if (typeIID.encoding().prefix() == Encoding.Prefix.VERTEX_ROLE_TYPE) {
                    storage.mergeUntracked(vertexTransitiveCountKey(typeGraph.rootRoleType().iid()), encodeLong(delta));
                }
            });
            attributeVertexCountJobs.forEach((attIID, countWorkValue) -> storage.putTracked(
                    attributeCountJobKey(attIID), countWorkValue.bytes(), false
            ));
            hasEdgeCountJobs.forEach((hasEdge, countWorkValue) -> storage.putTracked(
                    hasEdgeCountJobKey(IIDMap.getOrDefault(hasEdge.first(), hasEdge.first()), hasEdge.second()), countWorkValue.bytes(), false
            ));
            if (!deltaVertexCount.isEmpty()) {
                storage.mergeUntracked(snapshotKey(), encodeLong(1));
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
            FunctionalIterator<CountJob> countJobs = storage.iterate(StatisticsBytes.countJobKey(), CountJob::of);
            for (long processed = 0; processed < COUNT_JOB_BATCH_SIZE && countJobs.hasNext(); processed++) {
                CountJob countJob = countJobs.next();
                if (countJob instanceof CountJob.Attribute) {
                    processAttributeCountJob(countJob);
                } else if (countJob instanceof CountJob.HasEdge) {
                    processHasEdgeCountJob(countJob);
                } else {
                    assert false;
                }
                storage.deleteTracked(countJob.key());
            }
            storage.mergeUntracked(snapshotKey(), encodeLong(1));
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
            ByteArray countedKey = attributeCountedKey(attIID);
            ByteArray counted = storage.get(countedKey);
            if (counted == null) {
                storage.mergeUntracked(vertexCountKey(attIID.type()), encodeLong(1));
                storage.mergeUntracked(vertexTransitiveCountKey(typeGraph.rootAttributeType().iid()), encodeLong(1));
                storage.putTracked(countedKey);
            }
        }

        private void processAttributeDeletedCountJob(VertexIID.Attribute<?> attIID) {
            ByteArray countedKey = attributeCountedKey(attIID);
            ByteArray counted = storage.get(countedKey);
            if (counted != null) {
                storage.mergeUntracked(vertexCountKey(attIID.type()), encodeLong(-1));
                storage.mergeUntracked(vertexTransitiveCountKey(typeGraph.rootAttributeType().iid()), encodeLong(-1));
                storage.putTracked(countedKey);
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
            ByteArray countedKey = hasEdgeCountedKey(thingIID, attIID);
            ByteArray counted = storage.get(countedKey);
            if (counted == null) {
                storage.mergeUntracked(hasEdgeCountKey(thingIID.type(), attIID.type()), encodeLong(1));
                if (thingIID.type().encoding().prefix() == VERTEX_ENTITY_TYPE) {
                    storage.mergeUntracked(hasEdgeTotalCountKey(typeGraph.rootEntityType().iid()), encodeLong(1));
                } else if (thingIID.type().encoding().prefix() == VERTEX_RELATION_TYPE) {
                    storage.mergeUntracked(hasEdgeTotalCountKey(typeGraph.rootRelationType().iid()), encodeLong(1));
                } else if (thingIID.type().encoding().prefix() == VERTEX_ATTRIBUTE_TYPE) {
                    storage.mergeUntracked(hasEdgeTotalCountKey(typeGraph.rootAttributeType().iid()), encodeLong(1));
                }
                storage.putTracked(countedKey);
            }
        }

        private void processHasEdgeDeletedCountJob(VertexIID.Thing thingIID, VertexIID.Attribute<?> attIID) {
            ByteArray countedKey = hasEdgeCountedKey(thingIID, attIID);
            ByteArray counted = storage.get(countedKey);
            if (counted != null) {
                storage.mergeUntracked(hasEdgeCountKey(thingIID.type(), attIID.type()), encodeLong(-1));
                if (thingIID.type().encoding().prefix() == VERTEX_ENTITY_TYPE) {
                    storage.mergeUntracked(hasEdgeTotalCountKey(typeGraph.rootEntityType().iid()), encodeLong(-1));
                } else if (thingIID.type().encoding().prefix() == VERTEX_RELATION_TYPE) {
                    storage.mergeUntracked(hasEdgeTotalCountKey(typeGraph.rootRelationType().iid()), encodeLong(-1));
                } else if (thingIID.type().encoding().prefix() == VERTEX_ATTRIBUTE_TYPE) {
                    storage.mergeUntracked(hasEdgeTotalCountKey(typeGraph.rootAttributeType().iid()), encodeLong(-1));
                }
                storage.deleteTracked(countedKey);
            }
        }

        private long bytesToLongOrZero(ByteArray bytes) {
            return bytes != null ? bytes.decodeLong() : 0;
        }

        public abstract static class CountJob {
            private final Encoding.Statistics.JobOperation value;
            private final ByteArray key;

            private CountJob(ByteArray key, Encoding.Statistics.JobOperation value) {
                this.key = key;
                this.value = value;
            }

            public static CountJob of(ByteArray key, ByteArray value) {
                ByteArray countJobKey = key.view(PrefixIID.LENGTH);
                Encoding.Statistics.JobType jobType = Encoding.Statistics.JobType.of(countJobKey.view(0, 1));
                Encoding.Statistics.JobOperation jobOperation = Encoding.Statistics.JobOperation.of(value);
                ByteArray countJobIID = countJobKey.view(PrefixIID.LENGTH);
                if (jobType == Encoding.Statistics.JobType.ATTRIBUTE_VERTEX) {
                    VertexIID.Attribute<?> attIID = VertexIID.Attribute.of(countJobIID);
                    return new Attribute(key, attIID, jobOperation);
                } else if (jobType == Encoding.Statistics.JobType.HAS_EDGE) {
                    VertexIID.Thing thingIID = VertexIID.Thing.extract(countJobIID, 0);
                    VertexIID.Attribute<?> attIID = VertexIID.Attribute.extract(countJobIID, thingIID.bytes().length());
                    return new HasEdge(key, thingIID, attIID, jobOperation);
                } else {
                    assert false;
                    return null;
                }
            }

            public ByteArray key() {
                return key;
            }

            public Encoding.Statistics.JobOperation value() {
                return value;
            }

            public Attribute asAttribute() {
                throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Attribute.class));
            }

            public HasEdge asHasEdge() {
                throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(HasEdge.class));
            }

            public static class Attribute extends CountJob {
                private final VertexIID.Attribute<?> attIID;

                private Attribute(ByteArray key, VertexIID.Attribute<?> attIID, Encoding.Statistics.JobOperation value) {
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

                private HasEdge(ByteArray key, VertexIID.Thing thingIID, VertexIID.Attribute<?> attIID,
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
