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
import hypergraph.common.iterator.Iterators;
import hypergraph.graph.Graph;
import hypergraph.graph.Schema;
import hypergraph.graph.vertex.TypeVertex;

import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Stream;

import static hypergraph.common.exception.Error.TypeDefinition.INVALID_ROOT_TYPE_MUTATION;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;

public abstract class Type<TYPE extends Type<TYPE>> {

    protected final TypeVertex vertex;
    protected TYPE superType;

    protected Type(TypeVertex vertex) {
        this.vertex = Objects.requireNonNull(vertex);
    }

    Type(Graph.Type graph, String label, Schema.Vertex.Type schema) {
        this(graph, label, schema, null);
    }

    Type(Graph.Type graph, String label, Schema.Vertex.Type schema, String scope) {
        this.vertex = graph.put(schema, label, scope);
        TypeVertex superTypeVertex = graph.get(schema.root().label(), schema.root().scope());
        vertex.outs().put(Schema.Edge.Type.SUB, superTypeVertex);
        superType = newInstance(superTypeVertex);
    }

    abstract TYPE getThis();

    abstract TYPE newInstance(TypeVertex vertex);

    public boolean isRoot() { return false; }

    public void label(String label) {
        vertex.label(label);
    }

    public String label() {
        return vertex.label();
    }

    protected void isAbstract(boolean isAbstract) {
        vertex.isAbstract(isAbstract);
    }

    public boolean isAbstract() {
        return vertex.isAbstract();
    }

    protected void sup(TYPE superType) {
        vertex.outs().delete(Schema.Edge.Type.SUB, sup().vertex);
        vertex.outs().put(Schema.Edge.Type.SUB, superType.vertex);
        this.superType = superType;
    }

    public TYPE sup() {
        if (superType != null) return superType;

        Iterator<TypeVertex> iterator = Iterators.filter(vertex.outs().edge(Schema.Edge.Type.SUB).to(),
                                                         v -> v.schema().equals(vertex.schema()));
        if (iterator.hasNext()) superType = newInstance(iterator.next());
        return superType;
    }

    public Stream<TYPE> sups() {
        Iterator<TYPE> sups = Iterators.loop(
                vertex,
                v -> v != null && v.schema().equals(this.vertex.schema()),
                v -> {
                    Iterator<TypeVertex> p = v.outs().edge(Schema.Edge.Type.SUB).to();
                    if (p.hasNext()) return p.next();
                    else return null;
                }).apply(this::newInstance);

        return stream(spliteratorUnknownSize(sups, ORDERED | IMMUTABLE), false);
    }

    public Stream<TYPE> subs() {
        Iterator<TYPE> sups = Iterators.tree(vertex, v -> v.ins().edge(Schema.Edge.Type.SUB).from()).apply(this::newInstance);
        return stream(spliteratorUnknownSize(sups, ORDERED), false);
    }

    public void delete() {
        // TODO: Check if a type has any intances too
        if (subs().findAny().isPresent()) {
            vertex.delete();
        } else {
            throw new HypergraphException("Invalid RoleType Removal: " + label() + " has subtypes");
        }
    }

    @Override
    public String toString() {
        return this.getClass().getCanonicalName() + " {" + vertex.toString() + "}";
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Type<?> that = (Type<?>) object;
        return this.vertex.equals(that.vertex);
    }

    @Override
    public final int hashCode() {
        return vertex.hashCode();
    }
}
