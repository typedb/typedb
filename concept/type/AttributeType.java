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

import hypergraph.graph.Graph;
import hypergraph.graph.Schema;
import hypergraph.graph.vertex.TypeVertex;

public class AttributeType extends Type.Tree<AttributeType> {

    public AttributeType(TypeVertex vertex) {
        super(vertex);
        assert(vertex.schema() == Schema.Vertex.Type.ATTRIBUTE_TYPE);
    }

    public AttributeType(Graph graph, String label) {
        super(graph, label, Schema.Vertex.Type.ATTRIBUTE_TYPE);
    }

    @Override
    AttributeType newInstance(TypeVertex vertex) {
        return new AttributeType(vertex);
    }

    @Override
    AttributeType getThis() {
        return this;
    }


}
