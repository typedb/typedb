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

import hypergraph.common.exception.HypergraphException;
import hypergraph.graph.util.IID;
import hypergraph.graph.util.Schema;
import hypergraph.graph.util.Storage;
import hypergraph.graph.vertex.AttributeVertex;
import hypergraph.graph.vertex.ThingVertex;
import hypergraph.graph.vertex.TypeVertex;
import hypergraph.graph.vertex.impl.AttributeVertexImpl;
import hypergraph.graph.vertex.impl.ThingVertexImpl;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Stream;

import static hypergraph.graph.util.AttributeSync.CommitSync.Status.NONE;
import static hypergraph.graph.util.IID.Vertex.Thing.generate;
import static java.util.stream.Stream.concat;

public class ThingGraph implements Graph<IID.Vertex.Thing, ThingVertex> {

    private final Graphs graphManager;
    private final ConcurrentMap<IID.Vertex.Thing, ThingVertex> thingsByIID;
    private final AttributesByIID attributesByIID;
    private ConcurrentLinkedQueue<AttributeVertex<?>> attributeVerticesWritten;
    private boolean attributeSyncIsLocked;

    ThingGraph(Graphs graphManager) {
        this.graphManager = graphManager;
        thingsByIID = new ConcurrentHashMap<>();
        attributesByIID = new AttributesByIID();
        attributeVerticesWritten = new ConcurrentLinkedQueue<>();
        attributeSyncIsLocked = false;
    }

    public TypeGraph typeGraph() {
        return graphManager.type();
    }

    public ConcurrentLinkedQueue<AttributeVertex<?>> attributesWritten() {
        return attributeVerticesWritten;
    }

    @Override
    public Storage storage() {
        return graphManager.storage();
    }

    @Override
    public ThingVertex convert(IID.Vertex.Thing iid) {
        return null; // TODO
    }

    public ThingVertex insert(IID.Vertex.Type typeIID, boolean isInferred) {
        assert !typeIID.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        IID.Vertex.Thing iid = generate(graphManager.keyGenerator(), typeIID);
        ThingVertex vertex = new ThingVertexImpl.Buffered(this, iid, isInferred);
        thingsByIID.put(iid, vertex);
        return vertex;
    }

    private <VALUE, ATT_IID extends IID.Vertex.Attribute<VALUE>, ATT_VERTEX extends AttributeVertex<VALUE>>
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
                new IID.Vertex.Attribute.Boolean(type.iid(), value),
                iid -> new AttributeVertexImpl.Boolean(ThingGraph.this, iid, false)
        );
    }

    public AttributeVertex<Long> get(TypeVertex type, long value) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Long.class);

        return getOrReadFromStorage(
                attributesByIID.longs,
                new IID.Vertex.Attribute.Long(type.iid(), value),
                iid -> new AttributeVertexImpl.Long(ThingGraph.this, iid, false)
        );
    }

    public AttributeVertex<Double> get(TypeVertex type, double value) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Double.class);

        return getOrReadFromStorage(
                attributesByIID.doubles,
                new IID.Vertex.Attribute.Double(type.iid(), value),
                iid -> new AttributeVertexImpl.Double(ThingGraph.this, iid, false)
        );
    }

    public AttributeVertex<String> get(TypeVertex type, String value) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(String.class);

        return getOrReadFromStorage(
                attributesByIID.strings,
                new IID.Vertex.Attribute.String(type.iid(), value),
                iid -> new AttributeVertexImpl.String(ThingGraph.this, iid, false)
        );
    }

    public AttributeVertex<LocalDateTime> get(TypeVertex type, LocalDateTime value) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(LocalDateTime.class);

        return getOrReadFromStorage(
                attributesByIID.dateTimes,
                new IID.Vertex.Attribute.DateTime(type.iid(), value),
                iid -> new AttributeVertexImpl.DateTime(ThingGraph.this, iid, false)
        );
    }

    public AttributeVertex<Boolean> put(TypeVertex type, boolean value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Boolean.class);

        return attributesByIID.booleans.computeIfAbsent(
                new IID.Vertex.Attribute.Boolean(type.iid(), value),
                iid -> new AttributeVertexImpl.Boolean(ThingGraph.this, iid, isInferred)
        );
    }

    public AttributeVertex<Long> put(TypeVertex type, long value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Long.class);

        return attributesByIID.longs.computeIfAbsent(
                new IID.Vertex.Attribute.Long(type.iid(), value),
                iid -> new AttributeVertexImpl.Long(ThingGraph.this, iid, isInferred)
        );
    }

    public AttributeVertex<Double> put(TypeVertex type, double value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Double.class);

        return attributesByIID.doubles.computeIfAbsent(
                new IID.Vertex.Attribute.Double(type.iid(), value),
                iid -> new AttributeVertexImpl.Double(ThingGraph.this, iid, isInferred)
        );
    }

    public AttributeVertex<String> put(TypeVertex type, String value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(String.class);
        assert value.length() <= Schema.STRING_MAX_LENGTH;

        return attributesByIID.strings.computeIfAbsent(
                new IID.Vertex.Attribute.String(type.iid(), value),
                iid -> new AttributeVertexImpl.String(ThingGraph.this, iid, isInferred)
        );
    }

    public AttributeVertex<LocalDateTime> put(TypeVertex type, LocalDateTime value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(LocalDateTime.class);

        return attributesByIID.dateTimes.computeIfAbsent(
                new IID.Vertex.Attribute.DateTime(type.iid(), value),
                iid -> new AttributeVertexImpl.DateTime(ThingGraph.this, iid, isInferred)
        );
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
     * We start off by generating new IIDs for every {@code ThingVertex} (which
     * does not actually include {@code AttributeVertex}). We then write the every
     * {@code ThingVertex} onto the storage. Once all commit operations for every
     * {@code ThingVertex} is done, we the write all the {@code AttributeVertex}
     * as the last step. To do so, we acquire a lock from {@code AttributeSync}
     * so that we have exclusive permission to access and manipulate it from
     * within every vertex as we write them onto storage. If an attribute vertex
     * was actually written to storage, it will be saved in
     * {@code attributeVerticesWritten}, and we need to hold onto the lock we
     * acquired from {@code AttributeSync} until we can {@code #confirm()} that
     * they were actually committed at storage. If there were no attributes that
     * needed to be written onto storage (because {@code AttributeSync} says they're
     * already written in storage), then we can let go of the lock we had acquired.
     *
     * @return true if the operation results in locking the storage
     */
    @Override
    public boolean commit() {
        thingsByIID.values().parallelStream().filter(v -> !v.isInferred()).forEach(
                vertex -> vertex.iid(generate(graphManager.storage().keyGenerator(), vertex.typeVertex().iid()))
        ); // thingByIID no longer contains valid mapping from IID to TypeVertex
        thingsByIID.values().stream().filter(v -> !v.isInferred()).forEach(v -> v.commit(false));

        try {
            attributeSyncIsLocked = false;
            storage().attributeSync().lock();
            attributesByIID.valueStream().forEach(v -> v.commit(true));
            if (!attributeVerticesWritten.isEmpty()) attributeSyncIsLocked = true;
        } catch (InterruptedException e) {
            throw new HypergraphException(e);
        } finally {
            if (!attributeSyncIsLocked) storage().attributeSync().unlock();
        }

        clear(); // we now flush the indexes after commit, and we do not expect this Graph.Thing to be used again
        return attributeSyncIsLocked;
    }

    void confirm(boolean committed, long snapshot) {
        assert attributeSyncIsLocked;
        if (!committed) {
            attributeVerticesWritten.parallelStream().forEach(v -> storage().attributeSync().get(v.iid()).status(NONE));
        } else {
            assert snapshot > 0;
            attributeVerticesWritten.parallelStream().forEach(v -> storage().attributeSync().get(v.iid()).snapshot(snapshot));
        }
        storage().attributeSync().unlock();
        attributeSyncIsLocked = false;
    }

    private static class AttributesByIID {

        private final ConcurrentMap<IID.Vertex.Attribute.Boolean, AttributeVertex<Boolean>> booleans;
        private final ConcurrentMap<IID.Vertex.Attribute.Long, AttributeVertex<Long>> longs;
        private final ConcurrentMap<IID.Vertex.Attribute.Double, AttributeVertex<Double>> doubles;
        private final ConcurrentMap<IID.Vertex.Attribute.String, AttributeVertex<String>> strings;
        private final ConcurrentMap<IID.Vertex.Attribute.DateTime, AttributeVertex<LocalDateTime>> dateTimes;

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

        public void clear() {
            booleans.clear();
            longs.clear();
            doubles.clear();
            strings.clear();
            dateTimes.clear();
        }
    }
}
