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

import java.util.Iterator;
import java.util.Objects;

public abstract class Type {

    protected final TypeVertex vertex;

    public Type(TypeVertex vertex) {
        this.vertex = Objects.requireNonNull(vertex);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " {" + vertex.toString() + "}";
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Type that = (Type) object;
        return this.vertex.equals(that.vertex);
    }

    @Override
    public final int hashCode() {
        return vertex.hashCode();
    }

    public static class Root extends Type {

        public Root(TypeVertex vertex) {
            super(vertex);
        }
    }

    public static abstract class Tree<TYPE extends Tree> extends Type {

        protected TYPE parent;

        Tree(TypeVertex vertex) {
            super(vertex);
        }

        Tree(Graph graph, String label, Schema.Vertex.Type schema) {
            super(graph.type().createVertex(schema, label));
            TypeVertex parentVertex = graph.type().getVertex(schema.root().label());
            vertex.outs().add(Schema.Edge.Type.SUB, parentVertex);
            parent = newInstance(parentVertex);
        }

        abstract TYPE newInstance(TypeVertex vertex);

        abstract TYPE getThis();

        public TYPE sup(TYPE parent) {
            vertex.outs().add(Schema.Edge.Type.SUB, parent.vertex);
            this.parent = parent;
            return getThis();
        }

        public TYPE sup() {
            if (parent != null) return parent;

            Iterator<TypeVertex> iterator = vertex.outs().get(Schema.Edge.Type.SUB);
            if (iterator != null && iterator.hasNext()) {
                TypeVertex parentVertex = iterator.next();
                if (parentVertex.schema().equals(vertex.schema())) {
                    parent = newInstance(parentVertex);
                }
            }

            return parent;
        }

    }
}
