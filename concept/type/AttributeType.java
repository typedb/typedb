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

public class AttributeType extends ThingType<AttributeType> {

    public static AttributeType of(TypeVertex vertex) {
        if (vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label())) return new AttributeType.Root(vertex);
        else return new AttributeType(vertex);
    }

    public static AttributeType of(Graph.Type graph, String label) {
        return new AttributeType(graph, label);
    }

    private AttributeType(TypeVertex vertex) {
        super(vertex);
        if (vertex.schema() != Schema.Vertex.Type.ATTRIBUTE_TYPE) {
            throw new HypergraphException("Invalid Attribute Type: " + vertex.label() +
                                                  " subtypes " + vertex.schema().root().label());
        }
    }

    private AttributeType(Graph.Type graph, String label) {
        super(graph, label, Schema.Vertex.Type.ATTRIBUTE_TYPE);
    }

    @Override
    AttributeType newInstance(TypeVertex vertex) {
        return of(vertex);
    }

    public static class Root extends AttributeType {

        Root(TypeVertex vertex) {
            super(vertex);
            assert vertex.label().equals(Schema.Vertex.Type.Root.ATTRIBUTE.label());
        }

        @Override
        boolean isRoot() { return true; }

        @Override
        public void label(String label) {
            throw new HypergraphException("Invalid Operation Exception: root types are immutable");
        }

        @Override
        public void isAbstract(boolean isAbstract) {
            throw new HypergraphException("Invalid Operation Exception: root types are immutable");
        }

        @Override
        public AttributeType sup() {
            return null;
        }

        @Override
        public void sup(AttributeType superType) {
            throw new HypergraphException("Invalid Operation Exception: root types are immutable");
        }
    }
}
