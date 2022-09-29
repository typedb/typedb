/*
 * Copyright (C) 2022 Vaticle
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
import com.vaticle.typedb.core.common.collection.KeyValue;
import com.vaticle.typedb.core.common.exception.TypeDBCheckedException;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.key.KeyGenerator;
import com.vaticle.typedb.core.encoding.key.StatisticsKey;
import com.vaticle.typedb.core.encoding.Storage;
import com.vaticle.typedb.core.graph.edge.ThingEdge;
import com.vaticle.typedb.core.encoding.iid.PartitionedIID;
import com.vaticle.typedb.core.encoding.iid.VertexIID;
import com.vaticle.typedb.core.graph.vertex.AttributeVertex;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.graph.vertex.impl.AttributeVertexImpl;
import com.vaticle.typedb.core.graph.vertex.impl.ThingVertexImpl;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.pair;
import static com.vaticle.typedb.core.common.collection.ByteArray.empty;
import static com.vaticle.typedb.core.common.collection.ByteArray.encodeLong;
import static com.vaticle.typedb.core.common.collection.ByteArray.join;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_STRING_SIZE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.Iterators.link;
import static com.vaticle.typedb.core.common.parameters.Order.Asc.ASC;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.iterateSorted;
import static com.vaticle.typedb.core.encoding.Encoding.Status.BUFFERED;
import static com.vaticle.typedb.core.encoding.Encoding.Status.PERSISTED;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.BOOLEAN;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.DATETIME;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.DOUBLE;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.LONG;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.STRING;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.STRING_MAX_SIZE;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Thing.ATTRIBUTE;
import static com.vaticle.typedb.core.encoding.iid.VertexIID.Thing.generate;

public class ThingGraph {

    private final Storage.Data storage;
    private final TypeGraph typeGraph;
    private final KeyGenerator.Data.Buffered keyGenerator;
    private final AttributesByIID attributesByIID;
    private final ConcurrentMap<VertexIID.Thing, ThingVertex.Write> thingsByIID;
    private final ConcurrentMap<VertexIID.Type, ConcurrentSkipListSet<ThingVertex.Write>> thingsByTypeIID;
    private final Map<VertexIID.Thing, VertexIID.Thing> committedIIDs;
    private final Statistics statistics;
    private final ConcurrentSet<AttributeVertex.Write<?>> attributesCreated;
    private final ConcurrentSet<AttributeVertex<?>> attributesDeleted;
    private final ConcurrentSet<ThingEdge> hasEdgeCreated;
    private final ConcurrentSet<ThingEdge> hasEdgeDeleted;
    private boolean isModified;

    public ThingGraph(Storage.Data storage, TypeGraph typeGraph) {
        this.storage = storage;
        this.typeGraph = typeGraph;
        keyGenerator = new KeyGenerator.Data.Buffered();
        thingsByIID = new ConcurrentHashMap<>();
        attributesByIID = new AttributesByIID();
        thingsByTypeIID = new ConcurrentHashMap<>();
        statistics = new Statistics(typeGraph, storage);
        committedIIDs = new HashMap<>();
        attributesCreated = new ConcurrentSet<>();
        attributesDeleted = new ConcurrentSet<>();
        hasEdgeCreated = new ConcurrentSet<>();
        hasEdgeDeleted = new ConcurrentSet<>();
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

    public ThingVertex getReadable(VertexIID.Thing iid) {
        assert storage.isOpen();
        if (iid.encoding().equals(ATTRIBUTE)) return getReadable(iid.asAttribute());
        else if (!thingsByIID.containsKey(iid) && storage.get(iid) == null) return null;
        return convertToReadable(iid);
    }

    public AttributeVertex<?> getReadable(VertexIID.Attribute<?> iid) {
        if (!attributesByIID.forValueType(iid.valueType()).containsKey(iid) && storage.get(iid) == null) {
            return null;
        }
        return convertToReadable(iid);
    }

    public ThingVertex convertToReadable(VertexIID.Thing iid) {
        assert storage.isOpen();
        if (iid.encoding().equals(ATTRIBUTE)) return convertToReadable(iid.asAttribute());
        else {
            ThingVertex.Write vertex;
            if ((vertex = thingsByIID.get(iid)) != null) return vertex;
            else return new ThingVertexImpl.Read(this, iid);
        }
    }

    public AttributeVertex<?> convertToReadable(VertexIID.Attribute<?> attIID) {
        AttributeVertex<?> vertex;
        Encoding.ValueType<?> valueType = attIID.valueType();
        if (valueType == BOOLEAN) {
            vertex = attributesByIID.booleans.get(attIID.asBoolean());
            if (vertex == null) return new AttributeVertexImpl.Read.Boolean(this, attIID.asBoolean());
            else return vertex;
        } else if (valueType == LONG) {
            vertex = attributesByIID.longs.get(attIID.asLong());
            if (vertex == null) return new AttributeVertexImpl.Read.Long(this, attIID.asLong());
            else return vertex;
        } else if (valueType == DOUBLE) {
            vertex = attributesByIID.doubles.get(attIID.asDouble());
            if (vertex == null) return new AttributeVertexImpl.Read.Double(this, attIID.asDouble());
            else return vertex;
        } else if (valueType == STRING) {
            vertex = attributesByIID.strings.get(attIID.asString());
            if (vertex == null) return new AttributeVertexImpl.Read.String(this, attIID.asString());
            else return vertex;
        } else if (valueType == DATETIME) {
            vertex = attributesByIID.dateTimes.get(attIID.asDateTime());
            if (vertex == null) return new AttributeVertexImpl.Read.DateTime(this, attIID.asDateTime());
            else return vertex;
        }
        assert false;
        return null;
    }

    public ThingVertex.Write convertToWritable(VertexIID.Thing iid) {
        assert storage.isOpen();
        if (iid.encoding().equals(ATTRIBUTE)) return convertToWritable(iid.asAttribute());
        else return thingsByIID.computeIfAbsent(iid, i -> ThingVertexImpl.Write.of(this, i));
    }

    public AttributeVertex.Write<?> convertToWritable(VertexIID.Attribute<?> attIID) {
        Encoding.ValueType<?> valueType = attIID.valueType();
        if (valueType == BOOLEAN) {
            return attributesByIID.booleans.computeIfAbsent(attIID.asBoolean(),
                    i -> new AttributeVertexImpl.Write.Boolean(this, attIID.asBoolean()));
        } else if (valueType == LONG) {
            return attributesByIID.longs.computeIfAbsent(attIID.asLong(),
                    i -> new AttributeVertexImpl.Write.Long(this, attIID.asLong()));
        } else if (valueType == DOUBLE) {
            return attributesByIID.doubles.computeIfAbsent(attIID.asDouble(),
                    i -> new AttributeVertexImpl.Write.Double(this, attIID.asDouble()));
        } else if (valueType == STRING) {
            return attributesByIID.strings.computeIfAbsent(attIID.asString(),
                    i -> new AttributeVertexImpl.Write.String(this, attIID.asString()));
        } else if (valueType == DATETIME) {
            return attributesByIID.dateTimes.computeIfAbsent(attIID.asDateTime(),
                    i -> new AttributeVertexImpl.Write.DateTime(this, attIID.asDateTime()));
        }
        assert false;
        return null;
    }

    public ThingVertex.Write create(TypeVertex typeVertex, boolean isInferred) {
        assert storage.isOpen();
        assert !typeVertex.isAttributeType();
        VertexIID.Thing iid = generate(keyGenerator, typeVertex.iid(), typeVertex.properLabel());
        ThingVertexImpl.Write vertex = new ThingVertexImpl.Write.Buffered(this, iid, isInferred);
        thingsByIID.put(iid, vertex);
        thingsByTypeIID.computeIfAbsent(typeVertex.iid(), t -> new ConcurrentSkipListSet<>()).add(vertex);
        vertexCreated(vertex);
        return vertex;
    }

    private <VAL, IID extends VertexIID.Attribute<VAL>, VERTEX extends AttributeVertex<VAL>> VERTEX getOrReadFromStorage(
            Map<IID, ? extends VERTEX> map, IID attIID, Function<IID, VERTEX> vertexConstructor) {
        VERTEX vertex = map.get(attIID);
        if (vertex == null && storage.get(attIID) != null) return vertexConstructor.apply(attIID);
        else return vertex;
    }

    public FunctionalIterator<ThingVertex> getReadable(TypeVertex typeVertex) {
        return getReadable(typeVertex, ASC);
    }

    public <ORDER extends Order> Forwardable<ThingVertex, ORDER> getReadable(TypeVertex typeVertex, ORDER order) {
        Forwardable<ThingVertex, ORDER> vertices = storage.iterate(
                VertexIID.Thing.prefix(typeVertex.iid()),
                order
        ).mapSorted(kv -> convertToReadable(kv.key()), vertex -> KeyValue.of(vertex.iid(), empty()), order);
        if (!thingsByTypeIID.containsKey(typeVertex.iid())) return vertices;
        else {
            Forwardable<ThingVertex, ORDER> buffered = iterateSorted(thingsByTypeIID.get(typeVertex.iid()), order)
                    .mapSorted(e -> e, ThingVertex::toWrite, order);
            return vertices.merge(buffered).distinct();
        }
    }

    public AttributeVertex<Boolean> getReadable(TypeVertex type, boolean value) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(Boolean.class);

        return getOrReadFromStorage(
                attributesByIID.booleans,
                new VertexIID.Attribute.Boolean(type.iid(), value),
                iid -> new AttributeVertexImpl.Read.Boolean(this, iid)
        );
    }

    public AttributeVertex<Long> getReadable(TypeVertex type, long value) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(Long.class);

        return getOrReadFromStorage(
                attributesByIID.longs,
                new VertexIID.Attribute.Long(type.iid(), value),
                iid -> new AttributeVertexImpl.Read.Long(this, iid)
        );
    }

    public AttributeVertex<Double> getReadable(TypeVertex type, double value) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(Double.class);

        return getOrReadFromStorage(
                attributesByIID.doubles,
                new VertexIID.Attribute.Double(type.iid(), value),
                iid -> new AttributeVertexImpl.Read.Double(this, iid)
        );
    }

    public AttributeVertex<String> getReadable(TypeVertex type, String value) {
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

    public AttributeVertex<LocalDateTime> getReadable(TypeVertex type, LocalDateTime value) {
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

        AttributeVertex.Write<Boolean> vertex = attributesByIID.booleans.computeIfAbsent(
                new VertexIID.Attribute.Boolean(type.iid(), value),
                iid -> {
                    AttributeVertexImpl.Write.Boolean v = new AttributeVertexImpl.Write.Boolean(this, iid, isInferred);
                    thingsByTypeIID.computeIfAbsent(type.iid(), t -> new ConcurrentSkipListSet<>()).add(v);
                    vertexCreated(v);
                    return v;
                }
        );
        assert isInferred == vertex.isInferred();
        return vertex;
    }

    public AttributeVertex.Write<Long> put(TypeVertex type, long value, boolean isInferred) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(Long.class);

        AttributeVertex.Write<Long> vertex = attributesByIID.longs.computeIfAbsent(
                new VertexIID.Attribute.Long(type.iid(), value),
                iid -> {
                    AttributeVertexImpl.Write.Long v = new AttributeVertexImpl.Write.Long(this, iid, isInferred);
                    thingsByTypeIID.computeIfAbsent(type.iid(), t -> new ConcurrentSkipListSet<>()).add(v);
                    vertexCreated(v);
                    return v;
                }
        );
        assert isInferred == vertex.isInferred();
        return vertex;
    }

    public AttributeVertex.Write<Double> put(TypeVertex type, double value, boolean isInferred) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(Double.class);

        AttributeVertex.Write<Double> vertex = attributesByIID.doubles.computeIfAbsent(
                new VertexIID.Attribute.Double(type.iid(), value),
                iid -> {
                    AttributeVertexImpl.Write.Double v = new AttributeVertexImpl.Write.Double(this, iid, isInferred);
                    thingsByTypeIID.computeIfAbsent(type.iid(), t -> new ConcurrentSkipListSet<>()).add(v);
                    vertexCreated(v);
                    return v;
                }
        );
        assert isInferred == vertex.isInferred();
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

        AttributeVertex.Write<String> vertex = attributesByIID.strings.computeIfAbsent(
                attIID, iid -> {
                    AttributeVertexImpl.Write.String v = new AttributeVertexImpl.Write.String(this, iid, isInferred);
                    thingsByTypeIID.computeIfAbsent(type.iid(), t -> new ConcurrentSkipListSet<>()).add(v);
                    vertexCreated(v);
                    return v;
                }
        );
        assert isInferred == vertex.isInferred();
        return vertex;
    }

    public AttributeVertex.Write<LocalDateTime> put(TypeVertex type, LocalDateTime value, boolean isInferred) {
        assert storage.isOpen();
        assert type.isAttributeType();
        assert type.valueType().valueClass().equals(LocalDateTime.class);

        AttributeVertex.Write<LocalDateTime> vertex = attributesByIID.dateTimes.computeIfAbsent(
                new VertexIID.Attribute.DateTime(type.iid(), value),
                iid -> {
                    AttributeVertexImpl.Write.DateTime v = new AttributeVertexImpl.Write.DateTime(this, iid, isInferred);
                    thingsByTypeIID.computeIfAbsent(type.iid(), t -> new ConcurrentSkipListSet<>()).add(v);
                    vertexCreated(v);
                    return v;
                }
        );
        assert isInferred == vertex.isInferred();
        return vertex;
    }

    public void delete(ThingVertexImpl.Write vertex) {
        assert storage.isOpen();
        if (!vertex.isAttribute()) {
            thingsByIID.remove(vertex.iid());
            if (thingsByTypeIID.containsKey(vertex.type().iid())) {
                thingsByTypeIID.get(vertex.type().iid()).remove(vertex);
            }
        } else {
            assert storage.isOpen();
            attributesByIID.remove(vertex.asAttribute().iid());
            if (thingsByTypeIID.containsKey(vertex.iid().type())) {
                thingsByTypeIID.get(vertex.iid().type()).remove(vertex);
            }
        }
        vertexDeleted(vertex);
    }

    private void vertexCreated(ThingVertexImpl.Write vertex) {
        if (vertex.status() != BUFFERED) return;
        statistics.vertexCreated(vertex.iid().type(), vertex.isInferred());
        if (vertex.isAttribute() && !vertex.isInferred()) {
            if (attributesDeleted.contains(vertex.asAttribute())) {
                // if the vertex has already been deleted, and we are re-creating it, we should just reverse the deletion
                attributesDeleted.remove(vertex.asAttribute());
            } else {
                // if creating a brand new attribute, we should record it
                attributesCreated.add(vertex.asAttribute());
            }
        }
    }

    private void vertexDeleted(ThingVertexImpl.Write vertex) {
        statistics.vertexDeleted(vertex.iid().type(), vertex.isInferred());
        if (vertex.isAttribute() && !vertex.isInferred()) {
            if (attributesCreated.contains(vertex.asAttribute())) {
                // if the vertex has already been created, and we are deleting it, we just reverse the creation
                // the attribute that was created must have been a brand-new attribute that was not persisted
                attributesCreated.remove(vertex.asAttribute());
            } else {
                // if deleting a not brand-new attribute, we should record it
                attributesDeleted.add(vertex.asAttribute());
            }
        }
    }

    public void edgeCreated(ThingEdge edge) {
        if (edge.encoding() != Encoding.Edge.Thing.Base.HAS || isPersisted(edge)) return;
        statistics.hasEdgeCreated(edge.from().asWrite(), edge.to().asAttribute().asWrite(), edge.isInferred());

        if (hasEdgeDeleted.contains(edge)) {
            // if the edge was already deleted, and we are re-creating it, we should just reverse the deletion
            hasEdgeDeleted.remove(edge);
        } else if (edge.from().status() == PERSISTED) {
            // if creating a brand new edge, we should record it
            hasEdgeCreated.add(edge);
        }
    }

    private boolean isPersisted(ThingEdge edge) {
        if (edge.from().status() != PERSISTED || edge.to().status() != PERSISTED) return false;
        else return storage.get(edge.forwardView().iid()) != null;
    }

    public void edgeDeleted(ThingEdge edge) {
        if (edge.encoding() == Encoding.Edge.Thing.Base.HAS) {
            statistics.hasEdgeDeleted(edge.from().asWrite(), edge.to().asAttribute().asWrite(), edge.isInferred());
            if (hasEdgeCreated.contains(edge)) {
                // if the edge has already been created, and we are deleting it, we just reverse the creation
                hasEdgeCreated.remove(edge);
            } else if (edge.from().status() == PERSISTED) {
                hasEdgeDeleted.add(edge);
            }
        }
    }

    public void exclusiveOwnership(TypeVertex ownerType, AttributeVertex<?> attribute) {
        storage.trackExclusiveBytes(join(ownerType.iid().bytes(), attribute.iid().bytes()));
    }

    public void setModified(PartitionedIID iid) {
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

    public FunctionalIterator<Pair<ByteArray, ByteArray>> committedIIDs() {
        return iterate(committedIIDs.entrySet())
                .filter(committed -> !committed.getKey().encoding().equals(Encoding.Vertex.Thing.ROLE))
                .map(entry -> new Pair<>(entry.getKey().bytes(), entry.getValue().bytes()));
    }

    public Set<AttributeVertex.Write<?>> attributesCreated() {
        return attributesCreated;
    }

    public Set<AttributeVertex<?>> attributesDeleted() {
        return attributesDeleted;
    }

    public Set<ThingEdge> hasEdgeCreated() {
        return hasEdgeCreated;
    }

    public Set<ThingEdge> hasEdgeDeleted() {
        return hasEdgeDeleted;
    }


    /**
     * Commits all the writes captured in
     *
     * We start off by generating new IIDs for every {@code ThingVertex} (which
     * does not actually include {@code AttributeVertex}). We then write the every
     * {@code ThingVertex} onto the storage. Once all commit operations for every
     * {@code ThingVertex} is done, we the write all the {@code AttributeVertex}
     * as the last step. Since the write operations to storage are serialised
     * anyways, we don't need to parallelise the streams to commit the vertices.
     */
    public void commit() {
        iterate(thingsByIID.values()).filter(v -> v.status().equals(BUFFERED) && !v.isInferred()).forEachRemaining(v -> {
            VertexIID.Thing newIID = generate(storage.dataKeyGenerator(), v.type().iid(), v.type().properLabel());
            committedIIDs.put(v.iid(), newIID);
            v.iid(newIID);
        }); // thingsByIID no longer contains valid mapping from IID to TypeVertex
        thingsByIID.values().stream().filter(v -> !v.isInferred()).forEach(ThingVertex.Write::commit);
        attributesByIID.valuesIterator().forEachRemaining(AttributeVertex.Write::commit);
        statistics.commit();
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
            Encoding.ValueType<?> valueType = iid.valueType();
            if (valueType == BOOLEAN) booleans.remove(iid.asBoolean());
            else if (valueType == LONG) longs.remove(iid.asLong());
            else if (valueType == DOUBLE) doubles.remove(iid.asDouble());
            else if (valueType == STRING) strings.remove(iid.asString());
            else if (valueType == DATETIME) dateTimes.remove(iid.asDateTime());
        }

        ConcurrentMap<? extends VertexIID.Attribute<?>, ? extends AttributeVertex<?>> forValueType(Encoding.ValueType<?> valueType) {
            if (valueType == BOOLEAN) return booleans;
            else if (valueType == LONG) return longs;
            else if (valueType == DOUBLE) return doubles;
            else if (valueType == STRING) return strings;
            else if (valueType == DATETIME) return dateTimes;
            assert false;
            return null;
        }
    }

    public static class Statistics {

        private final ConcurrentMap<VertexIID.Type, Long> persistedVertexCount;
        private final ConcurrentMap<VertexIID.Type, Long> deltaVertexCount;
        private final ConcurrentMap<VertexIID.Type, Long> inferredVertexCount;
        private final ConcurrentMap<Pair<VertexIID.Type, VertexIID.Type>, Long> persistedHasEdgeCount;
        private final ConcurrentMap<Pair<VertexIID.Type, VertexIID.Type>, Long> deltaHasEdgeCount;
        private final ConcurrentMap<Pair<VertexIID.Type, VertexIID.Type>, Long> inferredHasEdgeCount;

        private final TypeGraph typeGraph;
        private final Storage.Data storage;
        private final long snapshot;

        Statistics(TypeGraph typeGraph, Storage.Data storage) {
            persistedVertexCount = new ConcurrentHashMap<>();
            deltaVertexCount = new ConcurrentHashMap<>();
            inferredVertexCount = new ConcurrentHashMap<>();
            persistedHasEdgeCount = new ConcurrentHashMap<>();
            deltaHasEdgeCount = new ConcurrentHashMap<>();
            inferredHasEdgeCount = new ConcurrentHashMap<>();

            snapshot = bytesToLongOrZero(storage.get(StatisticsKey.snapshot()));
            this.typeGraph = typeGraph;
            this.storage = storage;
        }

        public long snapshot() {
            return snapshot;
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

        public long thingVertexTransitiveCount(Label label) {
            return thingVertexTransitiveCount(typeGraph.getType(label));
        }

        public long thingVertexTransitiveCount(TypeVertex type) {
            return iterate(typeGraph.getSubtypes(type)).map(this::thingVertexCount).reduce(0L, Long::sum);
        }

        public long thingVertexTransitiveMax(Set<Label> labels) {
            return thingVertexTransitiveMax(iterate(labels).map(typeGraph::getType));
        }

        public long thingVertexTransitiveMax(FunctionalIterator<TypeVertex> types) {
            return types.map(this::thingVertexTransitiveCount).stream().max(Comparator.naturalOrder()).orElse(0L);
        }

        public long thingVertexCount(Label label) {
            return thingVertexCount(typeGraph.getType(label));
        }

        public long thingVertexCount(TypeVertex type) {
            return persistedVertexCount(type.iid()) + deltaVertexCount(type.iid()) + inferredVertexCount(type.iid());
        }

        public long hasEdgeSum(TypeVertex owner, Set<TypeVertex> attributes) {
            return attributes.stream().map(att -> hasEdgeCount(owner, att)).mapToLong(l -> l).sum();
        }

        public long hasEdgeSum(Set<TypeVertex> owners, TypeVertex attribute) {
            return owners.stream().map(owner -> hasEdgeCount(owner, attribute)).mapToLong(l -> l).sum();
        }

        public long hasEdgeCount(Label thing, Label attribute) {
            return hasEdgeCount(typeGraph.getType(thing), typeGraph.getType(attribute));
        }

        private long hasEdgeCount(TypeVertex thing, TypeVertex attribute) {
            return hasEdgeCount(thing.iid(), attribute.iid());
        }

        private long hasEdgeCount(VertexIID.Type fromTypeIID, VertexIID.Type toTypeIID) {
            return persistedHasEdgeCount(fromTypeIID, toTypeIID) + deltaHasEdgeCount(fromTypeIID, toTypeIID) +
                    inferredHasEdgeCount(fromTypeIID, toTypeIID);
        }

        private void vertexCreated(VertexIID.Type type, boolean inferred) {
            if (inferred) inferredVertexCount.compute(type, (k, v) -> (v == null ? 0 : v) + 1);
            else deltaVertexCount.compute(type, (k, v) -> (v == null ? 0 : v) + 1);
        }

        private void vertexDeleted(VertexIID.Type type, boolean inferred) {
            if (inferred) inferredVertexCount.compute(type, (k, v) -> (v == null ? 0 : v) - 1);
            else deltaVertexCount.compute(type, (k, v) -> (v == null ? 0 : v) - 1);
        }

        private void hasEdgeCreated(ThingVertex.Write thing, AttributeVertex.Write<?> attribute, boolean inferred) {
            if (inferred) {
                inferredHasEdgeCount.compute(new Pair<>(thing.type().iid(), attribute.type().iid()), (k, v) -> (v == null ? 0 : v) + 1);
            } else {
                deltaHasEdgeCount.compute(new Pair<>(thing.type().iid(), attribute.type().iid()), (k, v) -> (v == null ? 0 : v) + 1);
            }
        }

        private void hasEdgeDeleted(ThingVertex.Write thing, AttributeVertex.Write<?> attribute, boolean inferred) {
            if (inferred) {
                inferredHasEdgeCount.compute(new Pair<>(thing.type().iid(), attribute.type().iid()), (k, v) -> (v == null ? 0 : v) - 1);
            } else {
                deltaHasEdgeCount.compute(new Pair<>(thing.type().iid(), attribute.type().iid()), (k, v) -> (v == null ? 0 : v) - 1);
            }
        }

        private long deltaVertexCount(VertexIID.Type typeIID) {
            return deltaVertexCount.getOrDefault(typeIID, 0L);
        }

        private long inferredVertexCount(VertexIID.Type typeIID) {
            return inferredVertexCount.getOrDefault(typeIID, 0L);
        }

        private long persistedVertexCount(VertexIID.Type typeIID) {
            return persistedVertexCount.computeIfAbsent(typeIID, iid ->
                    bytesToLongOrZero(storage.get(StatisticsKey.vertexCount(typeIID))));
        }

        private long persistedHasEdgeCount(VertexIID.Type thingTypeIID, VertexIID.Type attTypeIID) {
            return persistedHasEdgeCount.computeIfAbsent(pair(thingTypeIID, attTypeIID), iid ->
                    bytesToLongOrZero(storage.get(StatisticsKey.hasEdgeCount(thingTypeIID, attTypeIID))));
        }

        private long deltaHasEdgeCount(VertexIID.Type thingTypeIID, VertexIID.Type attTypeIID) {
            return deltaHasEdgeCount.getOrDefault(pair(thingTypeIID, attTypeIID), 0L);
        }

        private long inferredHasEdgeCount(VertexIID.Type thingTypeIID, VertexIID.Type attTypeIID) {
            return inferredHasEdgeCount.getOrDefault(pair(thingTypeIID, attTypeIID), 0L);
        }

        private long bytesToLongOrZero(ByteArray bytes) {
            return bytes != null ? bytes.decodeLong() : 0;
        }

        private void commit() {
            deltaVertexCount.forEach((typeIID, delta) ->
                    storage.mergeUntracked(StatisticsKey.vertexCount(typeIID), encodeLong(delta))
            );
            deltaHasEdgeCount.forEach((ownership, delta) ->
                    storage.mergeUntracked(StatisticsKey.hasEdgeCount(ownership.first(), ownership.second()), encodeLong(delta))
            );
            if (!deltaVertexCount.isEmpty() || !deltaHasEdgeCount.isEmpty()) {
                storage.mergeUntracked(StatisticsKey.snapshot(), encodeLong(1));
            }
        }

        private void clear() {
            persistedVertexCount.clear();
            deltaVertexCount.clear();
            inferredVertexCount.clear();
            persistedHasEdgeCount.clear();
            deltaHasEdgeCount.clear();
            inferredHasEdgeCount.clear();
        }
    }
}
