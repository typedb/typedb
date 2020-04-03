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

import hypergraph.common.exception.HypergraphException;
import hypergraph.graph.Graph;
import hypergraph.graph.Schema;
import hypergraph.graph.vertex.TypeVertex;

public abstract class ThingType<TYPE extends ThingType> extends Type<TYPE> {

    ThingType(TypeVertex vertex) {
        super(vertex);
    }

    ThingType(Graph graph, String label, Schema.Vertex.Type schema) {
        super(graph, label, schema);
    }

    public static class Root extends ThingType<ThingType> {

        public Root(TypeVertex vertex) {
            super(vertex);
            assert(vertex.label().equals(Schema.Vertex.Type.Root.THING.label()));
        }

        @Override
        ThingType newInstance(TypeVertex vertex) {
            if (vertex.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE)) return AttributeType.of(vertex);
            if (vertex.schema().equals(Schema.Vertex.Type.ENTITY_TYPE)) return EntityType.of(vertex);
            if (vertex.schema().equals(Schema.Vertex.Type.RELATION_TYPE)) return RelationType.of(vertex);
            return null;
        }

        @Override
        ThingType getThis() {
            return this;
        }

        @Override
        public ThingType label(String label) {
            throw new HypergraphException("Invalid Operation Exception: root types are immutable");
        }

        @Override
        public ThingType setAbstract(boolean isAbstract) {
            throw new HypergraphException("Invalid Operation Exception: root types are immutable");
        }

        @Override
        public ThingType sup() {
            return null;
        }

        @Override
        public ThingType sup(ThingType superType) {
            throw new HypergraphException("Invalid Operation Exception: root types are immutable");
        }
    }
}
