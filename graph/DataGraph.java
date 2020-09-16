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

import grakn.core.common.exception.GraknException;
import grakn.core.graph.iid.VertexIID;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.util.Storage;
import grakn.core.graph.vertex.AttributeVertex;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.TypeVertex;
import grakn.core.graph.vertex.Vertex;
import grakn.core.graph.vertex.impl.AttributeVertexImpl;
import grakn.core.graph.vertex.impl.ThingVertexImpl;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Stream;

import static grakn.core.graph.iid.VertexIID.Thing.generate;
import static java.util.stream.Stream.concat;

public class DataGraph implements Graph {

    private final Graphs graphManager;
    private final ConcurrentMap<VertexIID.Thing, ThingVertex> thingsByIID;
    private final AttributesByIID attributesByIID;
    private boolean isModified;

    DataGraph(Graphs graphManager) {
        this.graphManager = graphManager;
        thingsByIID = new ConcurrentHashMap<>();
        attributesByIID = new AttributesByIID();
    }

    public SchemaGraph schema() {
        return graphManager.schema();
    }

    @Override
    public Storage storage() {
        return graphManager.storage();
    }

    public Stream<ThingVertex> vertices() {
        return concat(thingsByIID.values().stream(), attributesByIID.valueStream());
    }

    public ThingVertex get(VertexIID.Thing iid) {
        assert storage().isOpen();
        if (iid.encoding().equals(Encoding.Vertex.Thing.ATTRIBUTE)) {
            return get(iid.asAttribute());
        }
        if (!thingsByIID.containsKey(iid) && storage().get(iid.bytes()) == null) {
            return null;
        }
        return convert(iid);
    }

    public AttributeVertex<?> get(VertexIID.Attribute<?> iid) {
        if (!attributesByIID.forValueType(iid.valueType()).containsKey(iid) && storage().get(iid.bytes()) == null) {
            return null;
        }
        return convert(iid);
    }

    public ThingVertex convert(VertexIID.Thing iid) {
        // TODO: benchmark caching persisted edges
        // assert storage().isOpen();
        // enable the the line above
        if (iid.encoding().equals(Encoding.Vertex.Thing.ATTRIBUTE)) {
            return convert(iid.asAttribute());
        } else {
            return thingsByIID.computeIfAbsent(iid, i -> ThingVertexImpl.of(this, i));
        }
    }

    public AttributeVertex<?> convert(VertexIID.Attribute<?> attIID) {
        switch (attIID.valueType()) {
            case BOOLEAN:
                return attributesByIID.booleans.computeIfAbsent(attIID.asBoolean(), iid1 ->
                        new AttributeVertexImpl.Boolean(this, iid1));
            case LONG:
                return attributesByIID.longs.computeIfAbsent(attIID.asLong(), iid1 ->
                        new AttributeVertexImpl.Long(this, iid1));
            case DOUBLE:
                return attributesByIID.doubles.computeIfAbsent(attIID.asDouble(), iid1 ->
                        new AttributeVertexImpl.Double(this, iid1));
            case STRING:
                return attributesByIID.strings.computeIfAbsent(attIID.asString(), iid1 ->
                        new AttributeVertexImpl.String(this, iid1));
            case DATETIME:
                return attributesByIID.dateTimes.computeIfAbsent(attIID.asDateTime(), iid1 ->
                        new AttributeVertexImpl.DateTime(this, iid1));
            default:
                assert false;
                return null;
        }
    }

    public ThingVertex create(VertexIID.Type typeIID, boolean isInferred) {
        assert storage().isOpen();
        assert !typeIID.encoding().equals(Encoding.Vertex.Type.ATTRIBUTE_TYPE);
        VertexIID.Thing iid = generate(graphManager.keyGenerator(), typeIID);
        ThingVertex vertex = new ThingVertexImpl.Buffered(this, iid, isInferred);
        thingsByIID.put(iid, vertex);
        return vertex;
    }

    private <VALUE, ATT_IID extends VertexIID.Attribute<VALUE>, ATT_VERTEX extends AttributeVertex<VALUE>>
    ATT_VERTEX getOrReadFromStorage(Map<ATT_IID, ATT_VERTEX> map, ATT_IID attIID, Function<ATT_IID, ATT_VERTEX> vertexConstructor) {
        return map.computeIfAbsent(attIID, iid -> {
            byte[] val = storage().get(iid.bytes());
            if (val != null) return vertexConstructor.apply(iid);
            else return null;
        });
    }

    public AttributeVertex<Boolean> get(TypeVertex type, boolean value) {
        assert storage().isOpen();
        assert type.encoding().equals(Encoding.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Boolean.class);

        return getOrReadFromStorage(
                attributesByIID.booleans,
                new VertexIID.Attribute.Boolean(type.iid(), value),
                iid -> new AttributeVertexImpl.Boolean(DataGraph.this, iid)
        );
    }

    public AttributeVertex<Long> get(TypeVertex type, long value) {
        assert storage().isOpen();
        assert type.encoding().equals(Encoding.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Long.class);

        return getOrReadFromStorage(
                attributesByIID.longs,
                new VertexIID.Attribute.Long(type.iid(), value),
                iid -> new AttributeVertexImpl.Long(DataGraph.this, iid)
        );
    }

    public AttributeVertex<Double> get(TypeVertex type, double value) {
        assert storage().isOpen();
        assert type.encoding().equals(Encoding.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Double.class);

        return getOrReadFromStorage(
                attributesByIID.doubles,
                new VertexIID.Attribute.Double(type.iid(), value),
                iid -> new AttributeVertexImpl.Double(DataGraph.this, iid)
        );
    }

    public AttributeVertex<String> get(TypeVertex type, String value) {
        assert storage().isOpen();
        assert type.encoding().equals(Encoding.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(String.class);

        return getOrReadFromStorage(
                attributesByIID.strings,
                new VertexIID.Attribute.String(type.iid(), value),
                iid -> new AttributeVertexImpl.String(DataGraph.this, iid)
        );
    }

    public AttributeVertex<LocalDateTime> get(TypeVertex type, LocalDateTime value) {
        assert storage().isOpen();
        assert type.encoding().equals(Encoding.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(LocalDateTime.class);

        return getOrReadFromStorage(
                attributesByIID.dateTimes,
                new VertexIID.Attribute.DateTime(type.iid(), value),
                iid -> new AttributeVertexImpl.DateTime(DataGraph.this, iid)
        );
    }

    public AttributeVertex<Boolean> put(TypeVertex type, boolean value, boolean isInferred) {
        assert storage().isOpen();
        assert type.encoding().equals(Encoding.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Boolean.class);

        return attributesByIID.booleans.computeIfAbsent(
                new VertexIID.Attribute.Boolean(type.iid(), value),
                iid -> new AttributeVertexImpl.Boolean(DataGraph.this, iid, isInferred, true)
        );
    }

    public AttributeVertex<Long> put(TypeVertex type, long value, boolean isInferred) {
        assert storage().isOpen();
        assert type.encoding().equals(Encoding.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Long.class);

        return attributesByIID.longs.computeIfAbsent(
                new VertexIID.Attribute.Long(type.iid(), value),
                iid -> new AttributeVertexImpl.Long(DataGraph.this, iid, isInferred, true)
        );
    }

    public AttributeVertex<Double> put(TypeVertex type, double value, boolean isInferred) {
        assert storage().isOpen();
        assert type.encoding().equals(Encoding.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Double.class);

        return attributesByIID.doubles.computeIfAbsent(
                new VertexIID.Attribute.Double(type.iid(), value),
                iid -> new AttributeVertexImpl.Double(DataGraph.this, iid, isInferred, true)
        );
    }

    public AttributeVertex<String> put(TypeVertex type, String value, boolean isInferred) {
        assert storage().isOpen();
        assert type.encoding().equals(Encoding.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(String.class);
        assert value.length() <= Encoding.STRING_MAX_LENGTH;

        return attributesByIID.strings.computeIfAbsent(
                new VertexIID.Attribute.String(type.iid(), value),
                iid -> new AttributeVertexImpl.String(DataGraph.this, iid, isInferred, true)
        );
    }

    public AttributeVertex<LocalDateTime> put(TypeVertex type, LocalDateTime value, boolean isInferred) {
        assert storage().isOpen();
        assert type.encoding().equals(Encoding.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(LocalDateTime.class);

        return attributesByIID.dateTimes.computeIfAbsent(
                new VertexIID.Attribute.DateTime(type.iid(), value),
                iid -> new AttributeVertexImpl.DateTime(DataGraph.this, iid, isInferred, true)
        );
    }

    public void delete(ThingVertex vertex) {
        assert storage().isOpen();
        thingsByIID.remove(vertex.iid());
    }

    public void setModified() {
        assert storage().isOpen();
        if (!isModified) isModified = true;
    }

    public boolean isModified() {
        return isModified;
    }

    public void deleteAttribute(AttributeVertex<?> vertex) {
        assert storage().isOpen();
        attributesByIID.remove(vertex.iid());
    }

    @Override
    public void clear() {
        thingsByIID.clear();
        attributesByIID.clear();
    }

    @Override
    public GraknException exception(String errorMessage) {
        return graphManager.exception(errorMessage);
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
                vertex -> vertex.iid(generate(graphManager.storage().keyGenerator(), vertex.type().iid()))
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
}
