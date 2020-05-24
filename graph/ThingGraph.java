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
import hypergraph.graph.vertex.Vertex;
import hypergraph.graph.vertex.impl.AttributeVertexImpl;
import hypergraph.graph.vertex.impl.ThingVertexImpl;

import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static hypergraph.graph.util.IID.Vertex.Thing.generate;

public class ThingGraph implements Graph<IID.Vertex.Thing, ThingVertex> {

    private final Graphs graphManager;
    private final ConcurrentMap<IID.Vertex.Thing, ThingVertex> thingByIID;
    private final ConcurrentMap<IID.Vertex.Attribute.Boolean, AttributeVertex<Boolean>> attributeBooleanByIID;
    private final ConcurrentMap<IID.Vertex.Attribute.Long, AttributeVertex<Long>> attributeLongByIID;
    private final ConcurrentMap<IID.Vertex.Attribute.Double, AttributeVertex<Double>> attributeDoubleByIID;
    private final ConcurrentMap<IID.Vertex.Attribute.String, AttributeVertex<String>> attributeStringByIID;
    private final ConcurrentMap<IID.Vertex.Attribute.DateTime, AttributeVertex<LocalDateTime>> attributeDateTimeByIID;

    ThingGraph(Graphs graphManager) {
        this.graphManager = graphManager;
        thingByIID = new ConcurrentHashMap<>();
        attributeBooleanByIID = new ConcurrentHashMap<>();
        attributeLongByIID = new ConcurrentHashMap<>();
        attributeDoubleByIID = new ConcurrentHashMap<>();
        attributeStringByIID = new ConcurrentHashMap<>();
        attributeDateTimeByIID = new ConcurrentHashMap<>();
    }

    public TypeGraph typeGraph() {
        return graphManager.type();
    }

    @Override
    public Storage storage() {
        return null;
    }

    @Override
    public ThingVertex convert(IID.Vertex.Thing iid) {
        return null; // TODO
    }

    public <VALUE> AttributeVertex<VALUE> get(IID.Vertex.Attribute<VALUE> attributeIID) {
        return null; // TODO
    }

    public ThingVertex insert(Schema.Vertex.Thing schema, IID.Vertex.Type type, boolean isInferred) {
        IID.Vertex.Thing iid = generate(graphManager.keyGenerator(), schema, type);
        ThingVertex vertex = new ThingVertexImpl.Buffered(this, iid, isInferred);
        thingByIID.put(iid, vertex);
        return vertex;
    }

    public AttributeVertex<Boolean> put(TypeVertex type, boolean value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Boolean.class);

        IID.Vertex.Attribute.Boolean attIID = new IID.Vertex.Attribute.Boolean(type.iid(), value);
        return attributeBooleanByIID.computeIfAbsent(attIID, iid -> new AttributeVertexImpl<>(this, attIID, isInferred));
    }

    public AttributeVertex<Long> put(TypeVertex type, long value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Long.class);

        IID.Vertex.Attribute.Long attIID = new IID.Vertex.Attribute.Long(type.iid(), value);
        return attributeLongByIID.computeIfAbsent(attIID, iid -> new AttributeVertexImpl<>(this, attIID, isInferred));
    }

    public AttributeVertex<Double> put(TypeVertex type, double value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Double.class);

        IID.Vertex.Attribute.Double attIID = new IID.Vertex.Attribute.Double(type.iid(), value);
        return attributeDoubleByIID.computeIfAbsent(attIID, iid -> new AttributeVertexImpl<>(this, attIID, isInferred));
    }

    public AttributeVertex<String> put(TypeVertex type, String value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(String.class);
        assert value.length() <= Schema.STRING_MAX_LENGTH;

        IID.Vertex.Attribute.String attIID = new IID.Vertex.Attribute.String(type.iid(), value);
        return attributeStringByIID.computeIfAbsent(attIID, iid -> new AttributeVertexImpl<>(this, attIID, isInferred));
    }

    public AttributeVertex<LocalDateTime> put(TypeVertex type, LocalDateTime value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(LocalDateTime.class);

        IID.Vertex.Attribute.DateTime attIID = new IID.Vertex.Attribute.DateTime(type.iid(), value);
        return attributeDateTimeByIID.computeIfAbsent(attIID, iid -> new AttributeVertexImpl<>(this, attIID, isInferred));
    }

    @Override
    public void delete(ThingVertex vertex) {
        // TODO
    }

    @Override
    public void commit() {
        thingByIID.values().parallelStream().filter(v -> !v.isInferred() && !v.schema().equals(Schema.Vertex.Thing.ATTRIBUTE)).forEach(
                vertex -> vertex.iid(generate(graphManager.storage().keyGenerator(), vertex.schema(), vertex.typeVertex().iid()))
        ); // thingByIID no longer contains valid mapping from IID to TypeVertex
        thingByIID.values().parallelStream().filter(v -> !v.isInferred()).forEach(Vertex::commit);
        clear(); // we now flush the indexes after commit, and we do not expect this Graph.Thing to be used again
    }

    @Override
    public void clear() {
        thingByIID.clear();
    }
}
