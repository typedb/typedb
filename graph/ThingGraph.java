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
import hypergraph.graph.vertex.ThingVertex;
import hypergraph.graph.vertex.TypeVertex;
import hypergraph.graph.vertex.Vertex;
import hypergraph.graph.vertex.impl.ThingVertexImpl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static hypergraph.graph.vertex.impl.ThingVertexImpl.generateIID;

public class ThingGraph implements Graph<IID.Vertex.Thing, ThingVertex> {

    private final Graphs graphManager;
    private final ConcurrentMap<IID.Vertex.Thing, ThingVertex> thingByIID;

    ThingGraph(Graphs graphManager) {
        this.graphManager = graphManager;
        thingByIID = new ConcurrentHashMap<>();
    }

    @Override
    public Storage storage() {
        return null;
    }

    @Override
    public ThingVertex get(IID.Vertex.Thing iid) {
        return null; // TODO
    }

    @Override
    public void delete(ThingVertex vertex) {
        // TODO
    }

    public void commit() {
        thingByIID.values().parallelStream().filter(v -> !v.isInferred()).forEach(
                vertex -> vertex.iid(generateIID(graphManager.storage().keyGenerator(), vertex.schema(), vertex.typeVertex().iid()))
        ); // thingByIID no longer contains valid mapping from IID to TypeVertex
        thingByIID.values().parallelStream().filter(v -> !v.isInferred()).forEach(Vertex::commit);
        clear(); // we now flush the indexes after commit, and we do not expect this Graph.Thing to be used again
    }

    @Override
    public void clear() {
        thingByIID.clear();
    }

    public ThingVertex create(Schema.Vertex.Thing schema, IID.Vertex.Type type, boolean isInferred) {
        IID.Vertex.Thing iid = generateIID(graphManager.keyGenerator(), schema, type);
        ThingVertex vertex = new ThingVertexImpl.Buffered(this, schema, iid, isInferred);
        thingByIID.put(iid, vertex);
        return vertex;
    }

    public ThingVertex putAttribute(TypeVertex type, Object value) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().isInstance(value);



        return null;
    }

    public TypeGraph typeGraph() {
        return graphManager.type();
    }
}
