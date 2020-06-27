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

package hypergraph.graph;

import hypergraph.graph.iid.VertexIID;
import hypergraph.graph.util.Schema;
import hypergraph.graph.util.Storage;
import hypergraph.graph.vertex.AttributeVertex;
import hypergraph.graph.vertex.ThingVertex;
import hypergraph.graph.vertex.TypeVertex;
import hypergraph.graph.vertex.Vertex;
import hypergraph.graph.vertex.impl.AttributeVertexImpl;
import hypergraph.graph.vertex.impl.ThingVertexImpl;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Stream;

import static hypergraph.graph.iid.VertexIID.Thing.generate;
import static java.util.stream.Stream.concat;

public class ThingGraph implements Graph<VertexIID.Thing, ThingVertex> {

    private final Graphs graphManager;
    private final ConcurrentMap<VertexIID.Thing, ThingVertex> thingsByIID;
    private final AttributesByIID attributesByIID;
    private boolean isModified;

    ThingGraph(Graphs graphManager) {
        this.graphManager = graphManager;
        thingsByIID = new ConcurrentHashMap<>();
        attributesByIID = new AttributesByIID();
    }

    public TypeGraph type() {
        return graphManager.type();
    }

    @Override
    public Storage storage() {
        return graphManager.storage();
    }

    @Override
    public Stream<ThingVertex> vertices() {
        return concat(thingsByIID.values().stream(), attributesByIID.valueStream());
    }

    @Override
    public ThingVertex convert(VertexIID.Thing iid) {
//        return thingsByIID.computeIfAbsent(iid, i -> ThingVertexImpl.of(this, i));
        if (iid.schema().equals(Schema.Vertex.Thing.ATTRIBUTE)) {
            VertexIID.Attribute attIID = iid.asAttribute();
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
        else {
            return thingsByIID.computeIfAbsent(iid, i -> ThingVertexImpl.of(this, i));
        }
    }

    public ThingVertex create(VertexIID.Type typeIID, boolean isInferred) {
        assert !typeIID.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
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
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Boolean.class);

        return getOrReadFromStorage(
                attributesByIID.booleans,
                new VertexIID.Attribute.Boolean(type.iid(), value),
                iid -> new AttributeVertexImpl.Boolean(ThingGraph.this, iid)
        );
    }

    public AttributeVertex<Long> get(TypeVertex type, long value) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Long.class);

        return getOrReadFromStorage(
                attributesByIID.longs,
                new VertexIID.Attribute.Long(type.iid(), value),
                iid -> new AttributeVertexImpl.Long(ThingGraph.this, iid)
        );
    }

    public AttributeVertex<Double> get(TypeVertex type, double value) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Double.class);

        return getOrReadFromStorage(
                attributesByIID.doubles,
                new VertexIID.Attribute.Double(type.iid(), value),
                iid -> new AttributeVertexImpl.Double(ThingGraph.this, iid)
        );
    }

    public AttributeVertex<String> get(TypeVertex type, String value) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(String.class);

        return getOrReadFromStorage(
                attributesByIID.strings,
                new VertexIID.Attribute.String(type.iid(), value),
                iid -> new AttributeVertexImpl.String(ThingGraph.this, iid)
        );
    }

    public AttributeVertex<LocalDateTime> get(TypeVertex type, LocalDateTime value) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(LocalDateTime.class);

        return getOrReadFromStorage(
                attributesByIID.dateTimes,
                new VertexIID.Attribute.DateTime(type.iid(), value),
                iid -> new AttributeVertexImpl.DateTime(ThingGraph.this, iid)
        );
    }

    public AttributeVertex<Boolean> put(TypeVertex type, boolean value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Boolean.class);

        return attributesByIID.booleans.computeIfAbsent(
                new VertexIID.Attribute.Boolean(type.iid(), value),
                iid -> new AttributeVertexImpl.Boolean(ThingGraph.this, iid, isInferred, true)
        );
    }

    public AttributeVertex<Long> put(TypeVertex type, long value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Long.class);

        return attributesByIID.longs.computeIfAbsent(
                new VertexIID.Attribute.Long(type.iid(), value),
                iid -> new AttributeVertexImpl.Long(ThingGraph.this, iid, isInferred, true)
        );
    }

    public AttributeVertex<Double> put(TypeVertex type, double value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Double.class);

        return attributesByIID.doubles.computeIfAbsent(
                new VertexIID.Attribute.Double(type.iid(), value),
                iid -> new AttributeVertexImpl.Double(ThingGraph.this, iid, isInferred, true)
        );
    }

    public AttributeVertex<String> put(TypeVertex type, String value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(String.class);
        assert value.length() <= Schema.STRING_MAX_LENGTH;

        return attributesByIID.strings.computeIfAbsent(
                new VertexIID.Attribute.String(type.iid(), value),
                iid -> new AttributeVertexImpl.String(ThingGraph.this, iid, isInferred, true)
        );
    }

    public AttributeVertex<LocalDateTime> put(TypeVertex type, LocalDateTime value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(LocalDateTime.class);

        return attributesByIID.dateTimes.computeIfAbsent(
                new VertexIID.Attribute.DateTime(type.iid(), value),
                iid -> new AttributeVertexImpl.DateTime(ThingGraph.this, iid, isInferred, true)
        );
    }

    @Override
    public void delete(ThingVertex vertex) {
        thingsByIID.remove(vertex.iid());
    }

    @Override
    public void setModified() {
        if (!isModified) isModified = true;
    }

    @Override
    public boolean isModified() {
        return isModified;
    }

    public void delete(AttributeVertex<?> vertex) {
        attributesByIID.remove(vertex.iid());
    }

    @Override
    public void clear() {
        thingsByIID.clear();
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
        thingsByIID.values().parallelStream().filter(v -> v.status().equals(Schema.Status.BUFFERED) && !v.isInferred()).forEach(
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

        void remove(VertexIID.Attribute iid) {
            switch (iid.valueType()) {
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
    }
}
