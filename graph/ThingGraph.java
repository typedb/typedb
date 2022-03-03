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

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.collection.KeyValue;
import com.vaticle.typedb.core.common.exception.TypeDBCheckedException;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.common.KeyGenerator;
import com.vaticle.typedb.core.graph.common.StatisticsKey;
import com.vaticle.typedb.core.graph.common.Storage;
import com.vaticle.typedb.core.graph.edge.ThingEdge;
import com.vaticle.typedb.core.graph.iid.PartitionedIID;
import com.vaticle.typedb.core.graph.iid.VertexIID;
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
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.ASC;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.iterateSorted;
import static com.vaticle.typedb.core.graph.common.Encoding.Status.BUFFERED;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.STRING_MAX_SIZE;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Thing.ATTRIBUTE;
import static com.vaticle.typedb.core.graph.iid.VertexIID.Thing.generate;

public class ThingGraph {

    private final Storage.Data storage;
    private final TypeGraph typeGraph;
    private final KeyGenerator.Data.Buffered keyGenerator;
    private final AttributesByIID attributesByIID;
    private final ConcurrentMap<VertexIID.Thing, ThingVertex.Write> thingsByIID;
    private final ConcurrentMap<VertexIID.Type, ConcurrentSkipListSet<ThingVertex.Write>> thingsByTypeIID;
    private final Map<VertexIID.Thing, VertexIID.Thing> committedIIDs;
    private final Statistics statistics;
    private final ConcurrentSkipListSet<AttributeVertex.Write<?>> attributesCreated;
    private final ConcurrentSkipListSet<AttributeVertex.Write<?>> attributesDeleted;
    private final ConcurrentSkipListSet<ThingEdge.View.Forward> hasEdgeCreated;
    private final ConcurrentSkipListSet<ThingEdge.View.Forward> hasEdgeDeleted;
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
        attributesCreated = new ConcurrentSkipListSet<>();
        attributesDeleted = new ConcurrentSkipListSet<>();
        hasEdgeCreated = new ConcurrentSkipListSet<>();
        hasEdgeDeleted = new ConcurrentSkipListSet<>();
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

    public ThingVertex.Write convertToWritable(VertexIID.Thing iid) {
        assert storage.isOpen();
        if (iid.encoding().equals(ATTRIBUTE)) return convertToWritable(iid.asAttribute());
        else return thingsByIID.computeIfAbsent(iid, i -> ThingVertexImpl.Write.of(this, i));
    }

    public AttributeVertex.Write<?> convertToWritable(VertexIID.Attribute<?> attIID) {
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

    public Forwardable<ThingVertex, Order.Asc> getReadable(TypeVertex typeVertex) {
        Forwardable<ThingVertex, Order.Asc> vertices = storage.iterate(
                VertexIID.Thing.prefix(typeVertex.iid()),
                ASC
        ).mapSorted(kv -> convertToReadable(kv.key()), vertex -> KeyValue.of(vertex.iid(), empty()), ASC);
        if (!thingsByTypeIID.containsKey(typeVertex.iid())) return vertices;
        else {
            Forwardable<ThingVertex, Order.Asc> buffered = iterateSorted(thingsByTypeIID.get(typeVertex.iid()), ASC)
                    .mapSorted(e -> e, ThingVertex::toWrite, ASC);
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
        if (!isInferred && vertex.isInferred()) vertex.isInferred(false);
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
        if (!isInferred && vertex.isInferred()) vertex.isInferred(false);
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
        if (!isInferred && vertex.isInferred()) vertex.isInferred(false);
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
        if (!isInferred && vertex.isInferred()) vertex.isInferred(false);
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
        if (!isInferred && vertex.isInferred()) vertex.isInferred(false);
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
        statistics.vertexCreated(vertex.iid().type(), vertex.isInferred());
        if (vertex.isAttribute() && !vertex.isInferred()) {
            if (attributesDeleted.contains(vertex.asAttribute())) {
                assert vertex.asAttribute().status() == BUFFERED;
                // if the vertex has already been deleted, and we are re-creating it, we should just reverse the deletion
                attributesDeleted.remove(vertex.asAttribute());
            } else if (vertex.asAttribute().status() == BUFFERED) {
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
        if (edge.encoding() == Encoding.Edge.Thing.Base.HAS) {
            statistics.hasEdgeCreated(edge.from().asWrite(), edge.to().asAttribute().asWrite(), edge.isInferred());
            if (hasEdgeDeleted.contains(edge.forwardView())) {
                assert isUnpersistedEdge(edge);
                // if the edge was already deleted, and we are re-creating it, we should just reverse the deletion
                hasEdgeDeleted.remove(edge.forwardView());
            } else if (isUnpersistedEdge(edge)) {
                // if creating a brand new edge, we should record it
                hasEdgeCreated.add(edge.forwardView());
            }
        }
    }

    public void edgeDeleted(ThingEdge edge) {
        if (edge.encoding() == Encoding.Edge.Thing.Base.HAS) {
            statistics.hasEdgeDeleted(edge.from().asWrite(), edge.to().asAttribute().asWrite(), edge.isInferred());
            if (hasEdgeCreated.contains(edge.forwardView())) {
                // if the edge has already been created, and we are deleting it, we just reverse the creation
                hasEdgeCreated.remove(edge.forwardView());
            } else {
                hasEdgeDeleted.add(edge.forwardView());
            }
        }
    }

    private boolean isUnpersistedEdge(ThingEdge edge) {
        if (edge.from().status() == BUFFERED || edge.to().status() == BUFFERED) return true;
        else return storage.get(edge.forwardView().iid()) == null;
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
        committedIIDs.clear();
        statistics.clear();
    }

    public FunctionalIterator<Pair<ByteArray, ByteArray>> committedIIDs() {
        return iterate(committedIIDs.entrySet())
                .filter(committed -> !committed.getKey().encoding().equals(Encoding.Vertex.Thing.ROLE))
                .map(entry -> new Pair<>(entry.getKey().bytes(), entry.getValue().bytes()));
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
//
//        public static class Miscountable {
//
//            public FunctionalIterator<AttributeVertex.Write<?>> attrCreatedIntersection(Miscountable other) {
//                return iterateSorted(attributesCreated, ASC)
//                        .intersect(iterateSorted(other.attributesCreated, ASC));
//            }
//
//            public FunctionalIterator<AttributeVertex.Write<?>> attrDeletedIntersection(Miscountable other) {
//                return iterateSorted(attributesDeleted, ASC)
//                        .intersect(iterateSorted(other.attributesDeleted, ASC));
//            }
//
//            // TODO these could be optimised using navigable sets and a forwardable intersection algorithm?
//            public FunctionalIterator<Pair<ThingVertex.Write, AttributeVertex.Write<?>>> hasCreatedIntersection(Miscountable other) {
//                return iterate(hasEdgeCreated).filter(other.hasEdgeCreated::contains);
//            }
//
//            public FunctionalIterator<Pair<ThingVertex.Write, AttributeVertex.Write<?>>> hasDeletedIntersection(Miscountable other) {
//                return iterate(hasEdgeDeleted).filter(other.hasEdgeDeleted::contains);
//            }
//        }

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

        public long thingVertexTransitiveCount(TypeVertex type) {
            return iterate(typeGraph.getSubtypes(type)).map(this::thingVertexCount).sum(e -> e).orElse(0L);
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
        void vertexDeleted(VertexIID.Type type, boolean inferred) {
            if (inferred) inferredVertexCount.compute(type, (k, v) -> (v == null ? 0 : v) - 1);
            else deltaVertexCount.compute(type, (k, v) -> (v == null ? 0 : v) - 1);
        }
//
//        void attributeVertexCreated(AttributeVertex.Write<?> attribute) {
//            vertexCreated(attribute.type().iid());
//            attributesCreated.add(attribute);
//        }
//
//        void attributeVertexDeleted(AttributeVertex.Write<?> attribute) {
//            vertexDeleted(attribute.type().iid());
//            miscountable.attributesDeleted.add(attribute);

//        }

        public void hasEdgeCreated(ThingVertex.Write thing, AttributeVertex.Write<?> attribute, boolean inferred) {
            if (inferred)
                inferredHasEdgeCount.compute(new Pair<>(thing.type().iid(), attribute.type().iid()), (k, v) -> (v == null ? 0 : v) + 1);
            else
                deltaHasEdgeCount.compute(new Pair<>(thing.type().iid(), attribute.type().iid()), (k, v) -> (v == null ? 0 : v) + 1);
        }

        public void hasEdgeDeleted(ThingVertex.Write thing, AttributeVertex.Write<?> attribute, boolean inferred) {
            if (inferred)
                inferredHasEdgeCount.compute(new Pair<>(thing.type().iid(), attribute.type().iid()), (k, v) -> (v == null ? 0 : v) - 1);
            else
                deltaHasEdgeCount.compute(new Pair<>(thing.type().iid(), attribute.type().iid()), (k, v) -> (v == null ? 0 : v) - 1);
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
            persistedHasEdgeCount.clear();
            deltaHasEdgeCount.clear();
        }

        private long bytesToLongOrZero(ByteArray bytes) {
            return bytes != null ? bytes.decodeLong() : 0;
        }

//        public boolean processCountJobs() {
//            FunctionalIterator<CountJob> countJobs = storage.iterate(CountJobKey.prefix())
//                    .map(kv -> CountJob.of(kv.key(), kv.value()));
//            for (long processed = 0; processed < COUNT_JOB_BATCH_SIZE && countJobs.hasNext(); processed++) {
//                CountJob countJob = countJobs.next();
//                if (countJob instanceof CountJob.Attribute) {
//                    processAttributeCountJob(countJob);
//                } else if (countJob instanceof CountJob.HasEdge) {
//                    processHasEdgeCountJob(countJob);
//                } else {
//                    assert false;
//                }
//                storage.deleteTracked(countJob.getKey());
//            }
//            storage.mergeUntracked(StatisticsKey.snapshot(), encodeLong(1));
//            return countJobs.hasNext();
//        }
//
//        private void processAttributeCountJob(CountJob countJob) {
//            VertexIID.Attribute<?> attIID = countJob.asAttribute().attIID();
//            if (countJob.value() == CREATED) {
//                processAttributeCreatedCountJob(attIID);
//            } else if (countJob.value() == DELETED) {
//                processAttributeDeletedCountJob(attIID);
//            } else {
//                assert false;
//            }
//        }
//
//        private void processAttributeCreatedCountJob(VertexIID.Attribute<?> attIID) {
//            StatisticsKey countedKey = StatisticsKey.attributeCounted(attIID);
//            ByteArray counted = storage.get(countedKey);
//            if (counted == null) {
//                storage.mergeUntracked(StatisticsKey.vertexCount(attIID.type()), encodeLong(1));
//                storage.mergeUntracked(vertexTransitiveCount(typeGraph.rootAttributeType().iid()), encodeLong(1));
//                storage.putTracked(countedKey);
//            }
//        }
//
//        private void processAttributeDeletedCountJob(VertexIID.Attribute<?> attIID) {
//            StatisticsKey countedKey = StatisticsKey.attributeCounted(attIID);
//            ByteArray counted = storage.get(countedKey);
//            if (counted != null) {
//                storage.mergeUntracked(StatisticsKey.vertexCount(attIID.type()), encodeLong(-1));
//                storage.mergeUntracked(vertexTransitiveCount(typeGraph.rootAttributeType().iid()), encodeLong(-1));
//                storage.putTracked(countedKey);
//            }
//        }
//
//        private void processHasEdgeCountJob(CountJob countJob) {
//            VertexIID.Thing thingIID = countJob.asHasEdge().thingIID();
//            VertexIID.Attribute<?> attIID = countJob.asHasEdge().attIID();
//            if (countJob.value() == CREATED) {
//                processHasEdgeCreatedCountJob(thingIID, attIID);
//            } else if (countJob.value() == DELETED) {
//                processHasEdgeDeletedCountJob(thingIID, attIID);
//            } else {
//                assert false;
//            }
//        }
//
//        private void processHasEdgeCreatedCountJob(VertexIID.Thing thingIID, VertexIID.Attribute<?> attIID) {
//            StatisticsKey countedKey = StatisticsKey.hasEdgeCounted(thingIID, attIID);
//            ByteArray counted = storage.get(countedKey);
//            if (counted == null) {
//                storage.mergeUntracked(StatisticsKey.hasEdgeCount(thingIID.type(), attIID.type()), encodeLong(1));
//                if (thingIID.type().encoding().prefix() == VERTEX_ENTITY_TYPE) {
//                    storage.mergeUntracked(StatisticsKey.hasEdgeTotalCount(typeGraph.rootEntityType().iid()), encodeLong(1));
//                } else if (thingIID.type().encoding().prefix() == VERTEX_RELATION_TYPE) {
//                    storage.mergeUntracked(StatisticsKey.hasEdgeTotalCount(typeGraph.rootRelationType().iid()), encodeLong(1));
//                } else if (thingIID.type().encoding().prefix() == VERTEX_ATTRIBUTE_TYPE) {
//                    storage.mergeUntracked(StatisticsKey.hasEdgeTotalCount(typeGraph.rootAttributeType().iid()), encodeLong(1));
//                }
//                storage.putTracked(countedKey);
//            }
//        }
//
//        private void processHasEdgeDeletedCountJob(VertexIID.Thing thingIID, VertexIID.Attribute<?> attIID) {
//            StatisticsKey countedKey = StatisticsKey.hasEdgeCounted(thingIID, attIID);
//            ByteArray counted = storage.get(countedKey);
//            if (counted != null) {
//                storage.mergeUntracked(StatisticsKey.hasEdgeCount(thingIID.type(), attIID.type()), encodeLong(-1));
//                if (thingIID.type().encoding().prefix() == VERTEX_ENTITY_TYPE) {
//                    storage.mergeUntracked(StatisticsKey.hasEdgeTotalCount(typeGraph.rootEntityType().iid()), encodeLong(-1));
//                } else if (thingIID.type().encoding().prefix() == VERTEX_RELATION_TYPE) {
//                    storage.mergeUntracked(StatisticsKey.hasEdgeTotalCount(typeGraph.rootRelationType().iid()), encodeLong(-1));
//                } else if (thingIID.type().encoding().prefix() == VERTEX_ATTRIBUTE_TYPE) {
//                    storage.mergeUntracked(StatisticsKey.hasEdgeTotalCount(typeGraph.rootAttributeType().iid()), encodeLong(-1));
//                }
//                storage.deleteTracked(countedKey);
//            }
//        }

//        public abstract static class CountJob {
//
//            private final Encoding.Statistics.JobOperation value;
//            private final CountJobKey key;
//
//            private CountJob(CountJobKey key, Encoding.Statistics.JobOperation value) {
//                this.key = key;
//                this.value = value;
//            }
//
//            public static CountJob of(Key key, ByteArray value) {
//                ByteArray countJobKey = key.bytes().view(PrefixIID.LENGTH);
//                Encoding.Statistics.JobType jobType = Encoding.Statistics.JobType.of(countJobKey.view(0, 1));
//                Encoding.Statistics.JobOperation jobOperation = Encoding.Statistics.JobOperation.of(value);
//                ByteArray countJobIID = countJobKey.view(PrefixIID.LENGTH);
//                if (jobType == Encoding.Statistics.JobType.ATTRIBUTE_VERTEX) {
//                    VertexIID.Attribute<?> attIID = VertexIID.Attribute.of(countJobIID);
//                    return new Attribute(CountJobKey.of(key.bytes()), attIID, jobOperation);
//                } else if (jobType == Encoding.Statistics.JobType.HAS_EDGE) {
//                    VertexIID.Thing thingIID = VertexIID.Thing.extract(countJobIID, 0);
//                    VertexIID.Attribute<?> attIID = VertexIID.Attribute.extract(countJobIID, thingIID.bytes().length());
//                    return new HasEdge(CountJobKey.of(key.bytes()), thingIID, attIID, jobOperation);
//                } else {
//                    assert false;
//                    return null;
//                }
//            }
//
//            CountJobKey getKey() {
//                return key;
//            }
//
//            Encoding.Statistics.JobOperation value() {
//                return value;
//            }
//
//            Attribute asAttribute() {
//                throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Attribute.class));
//            }
//
//            HasEdge asHasEdge() {
//                throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(HasEdge.class));
//            }
//
//            static class Attribute extends CountJob {
//
//                private final VertexIID.Attribute<?> attIID;
//
//                private Attribute(CountJobKey key, VertexIID.Attribute<?> attIID, Encoding.Statistics.JobOperation value) {
//                    super(key, value);
//                    this.attIID = attIID;
//                }
//
//                VertexIID.Attribute<?> attIID() {
//                    return attIID;
//                }
//
//                @Override
//                public Attribute asAttribute() {
//                    return this;
//                }
//            }
//
//            static class HasEdge extends CountJob {
//
//                private final VertexIID.Thing thingIID;
//                private final VertexIID.Attribute<?> attIID;
//
//                private HasEdge(CountJobKey key, VertexIID.Thing thingIID, VertexIID.Attribute<?> attIID,
//                                Encoding.Statistics.JobOperation value) {
//                    super(key, value);
//                    this.thingIID = thingIID;
//                    this.attIID = attIID;
//                }
//
//                VertexIID.Thing thingIID() {
//                    return thingIID;
//                }
//
//                VertexIID.Attribute<?> attIID() {
//                    return attIID;
//                }
//
//                @Override
//                public HasEdge asHasEdge() {
//                    return this;
//                }
//            }
//        }
//    }
    }
}
