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

import hypergraph.common.HypergraphException;
import hypergraph.graph.Graph;
import hypergraph.graph.Schema;
import hypergraph.graph.edge.TypeEdge;
import hypergraph.graph.vertex.TypeVertex;

import java.util.Iterator;

public class EntityType extends Type {

    private EntityType parent;

    public EntityType(TypeVertex vertex) {
        super(vertex);
        if (vertex.schema() != Schema.Vertex.Type.ENTITY_TYPE) {
            throw new HypergraphException("Invalid TypeVertex for EntityType");
        }
    }

    public EntityType(Graph graph, String label) {
        super(graph.type().createVertex(Schema.Vertex.Type.ENTITY_TYPE, label));
        TypeVertex parentVertex = graph.type().getVertex(Schema.Vertex.Type.Root.ENTITY.label());
        vertex.out(Schema.Edge.Type.SUB, parentVertex);
        parent = new EntityType(parentVertex);
    }

    public EntityType sup(EntityType parent) {
        vertex.out(Schema.Edge.Type.SUB, parent.vertex);
        this.parent = parent;
        return this;
    }

    public EntityType sup() {
        if (parent != null) return parent;

        Iterator<TypeEdge> parentEdge = vertex.outs(Schema.Edge.Type.SUB);
        if (parentEdge != null && parentEdge.hasNext()) {
            TypeVertex typeVertex = parentEdge.next().to();
            if (typeVertex.schema().equals(Schema.Vertex.Type.ENTITY_TYPE)) {
                parent = new EntityType(typeVertex);
            }
        }

        return parent;
    }

}
