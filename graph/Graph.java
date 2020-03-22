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

import hypergraph.graph.edge.TypeEdge;
import hypergraph.graph.vertex.ThingVertex;
import hypergraph.graph.vertex.TypeVertex;
import hypergraph.graph.vertex.Vertex;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Graph {

    private final Storage storage;
    private final KeyGenerator keyGenerator;
    private final Map<String, TypeVertex> typeByLabel;
    private final Map<byte[], TypeVertex> typeByIID;
    private final Map<byte[], ThingVertex> thingByIID;

    public Graph(Storage storage) {
        this.storage = storage;
        keyGenerator = new KeyGenerator(Schema.Key.BUFFERED);
        typeByLabel = new ConcurrentHashMap<>();
        typeByIID = new ConcurrentHashMap<>();
        thingByIID = new ConcurrentHashMap<>();
    }

    public Storage storage() {
        return storage;
    }

    public void reset() {
        // TODO
    }

    public void commit() {
        typeByIID.values().parallelStream().filter(v -> v.status().equals(Schema.Status.BUFFERED)).forEach(
                vertex -> vertex.iid(TypeVertex.generateIID(storage.keyGenerator(), vertex.schema()))
        );
        thingByIID.values().parallelStream().forEach(
                vertex -> vertex.iid(ThingVertex.generateIID(storage.keyGenerator(), vertex.schema()))
        );

        typeByIID.values().parallelStream().forEach(Vertex::commit);
        thingByIID.values().parallelStream().forEach(Vertex::commit);
    }

    public boolean isInitialised() {
        return getTypeVertex(Schema.Vertex.Type.Root.THING.label()) != null;
    }

    public void initialise() {
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

        createTypeEdge(Schema.Edge.Type.SUB, rootEntityType, rootType);
        createTypeEdge(Schema.Edge.Type.SUB, rootRelationType, rootType);
        createTypeEdge(Schema.Edge.Type.SUB, rootRoleType, rootType);
        createTypeEdge(Schema.Edge.Type.SUB, rootAttributeType, rootType);
    }

    public TypeVertex createTypeVertex(Schema.Vertex.Type type, String label) {
        byte[] bufferedIID = TypeVertex.generateIID(keyGenerator, type);
        TypeVertex typeVertex = new TypeVertex.Buffered(this, type, bufferedIID, label);
        typeByIID.put(typeVertex.iid(), typeVertex);
        typeByLabel.put(typeVertex.label(), typeVertex);
        return typeVertex;
    }

    public TypeEdge createTypeEdge(Schema.Edge.Type type, TypeVertex from, TypeVertex to) {
        TypeEdge edge = new TypeEdge.Buffered(this, type, from, to);
        from.out(edge);
        to.in(edge);
        return edge;
    }

    public TypeVertex getTypeVertex(String label) {
        TypeVertex vertex = typeByLabel.get(label);
        if (vertex != null) return vertex;

        byte[] iid = storage.get(TypeVertex.generateIndex(label));
        if (iid != null) {
            vertex = typeByIID.computeIfAbsent(iid, x -> new TypeVertex.Persisted(this, x, label));
            typeByLabel.putIfAbsent(label, vertex);
        }

        return vertex;
    }

    public TypeVertex getTypeVertex(byte[] iid) {
        TypeVertex vertex = typeByIID.get(iid);
        if (vertex != null) return vertex;

        vertex = typeByIID.computeIfAbsent(iid, x -> new TypeVertex.Persisted(this, x));
        typeByLabel.putIfAbsent(vertex.label(), vertex);

        return vertex;
    }
}
