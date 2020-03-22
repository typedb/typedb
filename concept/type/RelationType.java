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

public class RelationType extends Type {

    private RelationType parent;

    public RelationType(TypeVertex vertex) {
        super(vertex);
        if (vertex.schema() != Schema.Vertex.Type.RELATION_TYPE) {
            throw new HypergraphException("Invalid TypeVertex for EntityType");
        }
    }

    public RelationType(Graph graph, String label) {
        super(graph.type().createVertex(Schema.Vertex.Type.RELATION_TYPE, label));
        TypeVertex parent = graph.type().getVertex(Schema.Vertex.Type.Root.RELATION.label());
        graph.type().createEdge(Schema.Edge.Type.SUB, vertex, parent);
    }

    public RelationType sup() {
        if (parent != null) return parent;

        Iterator<TypeEdge> parentEdge = vertex.outs(Schema.Edge.Type.SUB);
        if (parentEdge != null && parentEdge.hasNext()) {
            TypeVertex typeVertex = parentEdge.next().to();
            if (typeVertex.schema().equals(Schema.Vertex.Type.RELATION_TYPE)) {
                parent = new RelationType(typeVertex);
            }
        }

        return parent;
    }
}
