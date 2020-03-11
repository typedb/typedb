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

import hypergraph.storage.Storage;

public class GraphManager {

    private final Storage storage;

    public GraphManager(Storage storage) {
        this.storage = storage;
    }

    public void initialise() {
        Vertex.Type rootType = new Vertex.Type(
                storage, Schema.Vertex.Type.TYPE, Schema.Vertex.Type.Root.THING.label()).setAbstract(true);
        Vertex.Type rootEntityType = new Vertex.Type(
                storage, Schema.Vertex.Type.ENTITY_TYPE, Schema.Vertex.Type.Root.ENTITY.label()).setAbstract(true);
        Vertex.Type rootRelationType = new Vertex.Type(
                storage, Schema.Vertex.Type.RELATION_TYPE, Schema.Vertex.Type.Root.RELATION.label()).setAbstract(true);
        Vertex.Type rootRoleType = new Vertex.Type(
                storage, Schema.Vertex.Type.ROLE_TYPE, Schema.Vertex.Type.Root.ROLE.label()).setAbstract(true);
        Vertex.Type rootAttributeType = new Vertex.Type(
                storage, Schema.Vertex.Type.ATTRIBUTE_TYPE, Schema.Vertex.Type.Root.ATTRIBUTE.label()).setAbstract(true);

        putEdge(Schema.Edge.SUB, rootEntityType, rootType);
        putEdge(Schema.Edge.SUB, rootRelationType, rootType);
        putEdge(Schema.Edge.SUB, rootRoleType, rootType);
        putEdge(Schema.Edge.SUB, rootAttributeType, rootType);
    }

    public Vertex.Type createVertexType(Schema.Vertex.Type type, String label) {
        return new Vertex.Type(storage, type, label);
    }

    public Vertex.Thing createVertexThing(Schema.Vertex.Thing thing, Vertex.Type type) {
        return new Vertex.Thing(storage, thing, type);
    }

    public Vertex.Type getVertexType(String label) {
        return null;
    }

    public Edge putEdge(Schema.Edge type, Vertex from, Vertex to) {
        return new Edge();
    }
}
