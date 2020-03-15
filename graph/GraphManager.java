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

import hypergraph.graph.edge.Edge;
import hypergraph.graph.vertex.ThingVertex;
import hypergraph.graph.vertex.TypeVertex;
import hypergraph.graph.vertex.Vertex;

public class GraphManager {

    private final Storage storage;
    private Buffer buffer;

    public GraphManager(Storage storage) {
        this.storage = storage;
        buffer = new Buffer();
    }

    public void reset() {
        buffer = new Buffer();
    }

    public void persist() {
        buffer.typeVertices().parallelStream().forEach(
                vertex -> vertex.iid(TypeVertex.generateIID(storage.keyGenerator(), vertex.schema()))
        );
        buffer.thingVertices().parallelStream().forEach(
                vertex -> vertex.iid(ThingVertex.generateIID(storage.keyGenerator(), vertex.schema()))
        );
        buffer.typeVertices().parallelStream().forEach(Vertex::persist);
        buffer.thingVertices().parallelStream().forEach(Vertex::persist);
    }

    public boolean hasRootType() {
        return false; // TODO
//        return getTypeVertex(Schema.Vertex.Type.Root.THING.label()) != null;
    }

    public void creatRootTypes() {
        TypeVertex rootType = createTypeVertex(
                Schema.Vertex.Type.TYPE,
                Schema.Vertex.Type.Root.THING.label()
        ).setAbstract(true);

        TypeVertex rootEntityType = createTypeVertex(
                Schema.Vertex.Type.ENTITY_TYPE,
                Schema.Vertex.Type.Root.ENTITY.label()
        ).setAbstract(true);

        TypeVertex rootRelationType = createTypeVertex(
                Schema.Vertex.Type.RELATION_TYPE,
                Schema.Vertex.Type.Root.RELATION.label()
        ).setAbstract(true);

        TypeVertex rootRoleType = createTypeVertex(
                Schema.Vertex.Type.ROLE_TYPE,
                Schema.Vertex.Type.Root.ROLE.label()
        ).setAbstract(true);

        TypeVertex rootAttributeType = createTypeVertex(
                Schema.Vertex.Type.ATTRIBUTE_TYPE,
                Schema.Vertex.Type.Root.ATTRIBUTE.label()
        ).setAbstract(true);

        createEdge(Schema.Edge.SUB, rootEntityType, rootType);
        createEdge(Schema.Edge.SUB, rootRelationType, rootType);
        createEdge(Schema.Edge.SUB, rootRoleType, rootType);
        createEdge(Schema.Edge.SUB, rootAttributeType, rootType);
    }

    public TypeVertex createTypeVertex(Schema.Vertex.Type type, String label) {
        byte[] bufferedIID = TypeVertex.generateIID(buffer.keyGenerator(), type);
        TypeVertex typeVertex = new TypeVertex.Buffered(this.storage, type, bufferedIID, label);
        buffer.add(typeVertex);
        return typeVertex;
    }

    public Edge createEdge(Schema.Edge type, Vertex from, Vertex to) {
        Edge edge = new Edge(type, from, to);
        from.out(edge);
        to.in(edge);
        return edge;
    }

    public TypeVertex getTypeVertex(String label) {
        return null;
    }

    public ThingVertex createThingVertex(Schema.Vertex.Thing thing, TypeVertex type) {
        return null;
    }
}
