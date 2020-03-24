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
import hypergraph.graph.vertex.TypeVertex;

public class EntityType extends Type.Real<EntityType> {

    public EntityType(TypeVertex vertex) {
        super(vertex);
        if (vertex.schema() != Schema.Vertex.Type.ENTITY_TYPE) {
            // TODO: REMOVE THIS ONCE TESTED
            throw new HypergraphException("Invalid TypeVertex for EntityType");
        }
    }

    public EntityType(Graph graph, String label) {
        super(graph, label, Schema.Vertex.Type.ENTITY_TYPE);
    }

    @Override
    EntityType newInstance(TypeVertex vertex) {
        return new EntityType(vertex);
    }

    @Override
    EntityType getThis() {
        return this;
    }
}
