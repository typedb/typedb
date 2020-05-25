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

import hypergraph.graph.util.IID;
import hypergraph.graph.util.Schema;
import hypergraph.graph.util.Storage;
import hypergraph.graph.vertex.AttributeVertex;
import hypergraph.graph.vertex.ThingVertex;
import hypergraph.graph.vertex.TypeVertex;
import hypergraph.graph.vertex.impl.AttributeVertexImpl;
import hypergraph.graph.vertex.impl.ThingVertexImpl;

import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import static hypergraph.graph.util.AttributeSync.CommitSync.Status.NONE;
import static hypergraph.graph.util.IID.Vertex.Thing.generate;
import static java.util.stream.Stream.concat;

public class ThingGraph implements Graph<IID.Vertex.Thing, ThingVertex> {

    private final Graphs graphManager;
    private final ConcurrentMap<IID.Vertex.Thing, ThingVertex> thingsByIID;
    private final AttributeIIDMap attributesByIID;
    private ConcurrentLinkedQueue<AttributeVertex<?>> attributesWritten;
    private boolean attributeSyncIsLocked;

    ThingGraph(Graphs graphManager) {
        this.graphManager = graphManager;
        thingsByIID = new ConcurrentHashMap<>();
        attributesByIID = new AttributeIIDMap();
        attributesWritten = new ConcurrentLinkedQueue<>();
        attributeSyncIsLocked = false;
    }

    public TypeGraph typeGraph() {
        return graphManager.type();
    }

    @Override
    public Storage storage() {
        return graphManager.storage();
    }

    @Override
    public ThingVertex convert(IID.Vertex.Thing iid) {
        return null; // TODO
    }

    public <VALUE> AttributeVertex<VALUE> get(IID.Vertex.Attribute<VALUE> attributeIID) {
        return null; // TODO
    }

    public ThingVertex insert(IID.Vertex.Type typeIID, boolean isInferred) {
        assert !typeIID.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        IID.Vertex.Thing iid = generate(graphManager.keyGenerator(), typeIID);
        ThingVertex vertex = new ThingVertexImpl.Buffered(this, iid, isInferred);
        thingsByIID.put(iid, vertex);
        return vertex;
    }

    public AttributeVertex<Boolean> put(TypeVertex type, boolean value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Boolean.class);

        IID.Vertex.Attribute.Boolean attIID = new IID.Vertex.Attribute.Boolean(type.iid(), value);
        return attributesByIID.getOrCreate(attIID, isInferred);
    }

    public AttributeVertex<Long> put(TypeVertex type, long value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Long.class);

        IID.Vertex.Attribute.Long attIID = new IID.Vertex.Attribute.Long(type.iid(), value);
        return attributesByIID.getOrCreate(attIID, isInferred);
    }

    public AttributeVertex<Double> put(TypeVertex type, double value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Double.class);

        IID.Vertex.Attribute.Double attIID = new IID.Vertex.Attribute.Double(type.iid(), value);
        return attributesByIID.getOrCreate(attIID, isInferred);
    }

    public AttributeVertex<String> put(TypeVertex type, String value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(String.class);
        assert value.length() <= Schema.STRING_MAX_LENGTH;

        IID.Vertex.Attribute.String attIID = new IID.Vertex.Attribute.String(type.iid(), value);
        return attributesByIID.getOrCreate(attIID, isInferred);
    }

    public AttributeVertex<LocalDateTime> put(TypeVertex type, LocalDateTime value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(LocalDateTime.class);

        IID.Vertex.Attribute.DateTime attIID = new IID.Vertex.Attribute.DateTime(type.iid(), value);
        return attributesByIID.getOrCreate(attIID, isInferred);
    }

    @Override
    public void delete(ThingVertex vertex) {
        // TODO
    }

    @Override
    public void clear() {
        thingsByIID.clear();
        attributesByIID.clear();
    }

    /**
     * Commits all the writes captured in this graph into storage.
     *
     * TODO: describe the logic
     *
     * @return TODO
     */
    @Override
    public boolean commit() {
        thingsByIID.values().parallelStream().filter(v -> !v.isInferred()).forEach(
                vertex -> vertex.iid(generate(graphManager.storage().keyGenerator(), vertex.typeVertex().iid()))
        ); // thingByIID no longer contains valid mapping from IID to TypeVertex
        thingsByIID.values().parallelStream().filter(v -> !v.isInferred()).forEach(v -> v.commit(false));

        storage().attributeSync().lock();
        attributesByIID.valueStream().parallel().forEach(v -> v.commit(true));

        if (attributesWritten.isEmpty()) {
            attributeSyncIsLocked = false;
            storage().attributeSync().unlock();
        } else {
            attributeSyncIsLocked = true;
        }

        clear(); // we now flush the indexes after commit, and we do not expect this Graph.Thing to be used again
        return attributeSyncIsLocked;
    }

    public void confirm(boolean committed, long snapshot) {
        assert attributeSyncIsLocked;
        if (!committed) {
            attributesWritten.parallelStream().forEach(v -> storage().attributeSync().get(v.iid()).status(NONE));
        } else {
            assert snapshot > 0;
            attributesWritten.parallelStream().forEach(v -> storage().attributeSync().get(v.iid()).snapshot(snapshot));
        }
        storage().attributeSync().unlock();
        attributeSyncIsLocked = false;
    }

    public ConcurrentLinkedQueue<AttributeVertex<?>> attributesWritten() {
        return attributesWritten;
    }

    private class AttributeIIDMap {

        private final ConcurrentMap<IID.Vertex.Attribute.Boolean, AttributeVertex<Boolean>> booleans;
        private final ConcurrentMap<IID.Vertex.Attribute.Long, AttributeVertex<Long>> longs;
        private final ConcurrentMap<IID.Vertex.Attribute.Double, AttributeVertex<Double>> doubles;
        private final ConcurrentMap<IID.Vertex.Attribute.String, AttributeVertex<String>> strings;
        private final ConcurrentMap<IID.Vertex.Attribute.DateTime, AttributeVertex<LocalDateTime>> dateTimes;

        AttributeIIDMap() {
            booleans = new ConcurrentHashMap<>();
            longs = new ConcurrentHashMap<>();
            doubles = new ConcurrentHashMap<>();
            strings = new ConcurrentHashMap<>();
            dateTimes = new ConcurrentHashMap<>();
        }

        AttributeVertex<Boolean> getOrCreate(IID.Vertex.Attribute.Boolean attributeIID, boolean isInferred) {
            return booleans.computeIfAbsent(attributeIID, iid -> new AttributeVertexImpl.Boolean(ThingGraph.this, iid, isInferred));
        }

        AttributeVertex<Long> getOrCreate(IID.Vertex.Attribute.Long attributeIID, boolean isInferred) {
            return longs.computeIfAbsent(attributeIID, iid -> new AttributeVertexImpl.Long(ThingGraph.this, iid, isInferred));
        }

        AttributeVertex<Double> getOrCreate(IID.Vertex.Attribute.Double attributeIID, boolean isInferred) {
            return doubles.computeIfAbsent(attributeIID, iid -> new AttributeVertexImpl.Double(ThingGraph.this, iid, isInferred));
        }

        AttributeVertex<String> getOrCreate(IID.Vertex.Attribute.String attributeIID, boolean isInferred) {
            return strings.computeIfAbsent(attributeIID, iid -> new AttributeVertexImpl.String(ThingGraph.this, iid, isInferred));
        }

        AttributeVertex<LocalDateTime> getOrCreate(IID.Vertex.Attribute.DateTime attributeIID, boolean isInferred) {
            return dateTimes.computeIfAbsent(attributeIID, iid -> new AttributeVertexImpl.DateTime(ThingGraph.this, iid, isInferred));
        }

        Stream<AttributeVertex<?>> valueStream() {
            return concat(booleans.values().stream(),
                          concat(longs.values().stream(),
                                 concat(doubles.values().stream(),
                                        concat(strings.values().stream(),
                                               dateTimes.values().stream()))));
        }

        public void clear() {
            booleans.clear();
            longs.clear();
            doubles.clear();
            strings.clear();
            dateTimes.clear();
        }
    }
}
