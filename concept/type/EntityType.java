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

package hypergraph.concept.type;

import hypergraph.graph.GraphManager;
import hypergraph.graph.Schema;
import hypergraph.graph.edge.TypeEdge;
import hypergraph.graph.vertex.TypeVertex;

import java.util.Set;

public class EntityType extends Type {

    private EntityType parent;

    public EntityType(TypeVertex vertex) {
        super(vertex);
    }

    public EntityType(GraphManager graph, String label) {
        super(graph.createTypeVertex(Schema.Vertex.Type.ENTITY_TYPE, label));
        TypeVertex parentVertex = graph.getTypeVertex(Schema.Vertex.Type.Root.ENTITY.label());
        graph.createTypeEdge(Schema.Edge.Type.SUB, vertex, parentVertex);
        parent = new EntityType(parentVertex);
    }

    public EntityType sup() {
        if (parent != null) return parent;

        Set<TypeEdge> sub = vertex.outs(Schema.Edge.Type.SUB);
        if (sub != null && sub.size()==1) parent = new EntityType(sub.iterator().next().to());

        return parent;
    }

}
