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

import hypergraph.common.collection.ByteArray;
import hypergraph.graph.util.Schema;
import hypergraph.graph.util.Storage;
import hypergraph.graph.vertex.impl.ThingVertexImpl;
import hypergraph.graph.vertex.impl.VertexImpl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ThingGraph implements Graph<ThingVertexImpl> {

    private final Graphs graphManager;
    private final ConcurrentMap<ByteArray, ThingVertexImpl> thingByIID;

    ThingGraph(Graphs graphManager) {
        this.graphManager = graphManager;
        thingByIID = new ConcurrentHashMap<>();
    }

    @Override
    public Storage storage() {
        return null;
    }

    @Override
    public ThingVertexImpl get(byte[] iid) {
        return null; // TODO
    }

    @Override
    public void delete(ThingVertexImpl vertex) {
        // TODO
    }

    public void commit() {
        thingByIID.values().parallelStream().forEach(
                vertex -> vertex.iid(ThingVertexImpl.generateIID(graphManager.storage().keyGenerator(), vertex.schema(), vertex.typeVertex()))
        ); // thingByIID no longer contains valid mapping from IID to TypeVertex
        thingByIID.values().parallelStream().forEach(VertexImpl::commit);
        clear(); // we now flush the indexes after commit, and we do not expect this Graph.Thing to be used again
    }

    @Override
    public void clear() {
        thingByIID.clear();
    }

    public ThingVertexImpl create(Schema.Vertex.Thing schema, byte[] iid) {
        return null; // TODO
    }

    public TypeGraph typeGraph() {
        return graphManager.type();
    }
}
