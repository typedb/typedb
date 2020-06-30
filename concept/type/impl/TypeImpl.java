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

package hypergraph.concept.type.impl;

import hypergraph.common.exception.Error;
import hypergraph.common.exception.HypergraphException;
import hypergraph.common.iterator.Iterators;
import hypergraph.concept.type.Type;
import hypergraph.graph.TypeGraph;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.ThingVertex;
import hypergraph.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import static hypergraph.common.exception.Error.ThingWrite.ILLEGAL_ABSTRACT_WRITE;
import static hypergraph.common.iterator.Iterators.apply;
import static hypergraph.common.iterator.Iterators.loop;
import static hypergraph.common.iterator.Iterators.stream;
import static hypergraph.common.iterator.Iterators.tree;

public abstract class TypeImpl implements Type {

    public final TypeVertex vertex;

    TypeImpl(TypeVertex vertex) {
        this.vertex = Objects.requireNonNull(vertex);
    }

    TypeImpl(TypeGraph graph, String label, Schema.Vertex.Type schema) {
        this(graph, label, schema, null);
    }

    TypeImpl(TypeGraph graph, String label, Schema.Vertex.Type schema, String scope) {
        this.vertex = graph.create(schema, label, scope);
        TypeVertex superTypeVertex = graph.get(schema.root().label(), schema.root().scope());
        vertex.outs().put(Schema.Edge.Type.SUB, superTypeVertex);
    }

    public static TypeImpl of(TypeVertex vertex) {
        switch (vertex.schema()) {
            case ENTITY_TYPE:
                return EntityTypeImpl.of(vertex);
            case ATTRIBUTE_TYPE:
                return AttributeTypeImpl.of(vertex);
            case RELATION_TYPE:
                return RelationTypeImpl.of(vertex);
            case ROLE_TYPE:
                return RoleTypeImpl.of(vertex);
            case THING_TYPE:
                return new ThingTypeImpl.Root(vertex);
            default:
                throw new HypergraphException(Error.Internal.UNRECOGNISED_VALUE);
        }
    }

    @Override
    public String iid() {
        return vertex.iid().toHexString();
    }

    @Override
    public boolean isDeleted() {
        return vertex.isDeleted();
    }

    @Override
    public boolean isRoot() { return false; }

    @Override
    public Long count() {
        return 0L; // TODO: return total number of type instances
    }

    @Override
    public void label(String label) {
        vertex.label(label);
    }

    @Override
    public String label() {
        return vertex.label();
    }

    @Override
    public boolean isAbstract() {
        return vertex.isAbstract();
    }

    void superTypeVertex(TypeVertex superTypeVertex) {
        if (vertex.equals(superTypeVertex)) {
            throw new HypergraphException(Error.TypeWrite.ILLEGAL_SUBTYPING_SELF.format(vertex.label()));
        }
        vertex.outs().edge(Schema.Edge.Type.SUB, ((TypeImpl) sup()).vertex).delete();
        vertex.outs().put(Schema.Edge.Type.SUB, superTypeVertex);
    }

    @Nullable
    <TYPE> TYPE sup(Function<TypeVertex, TYPE> typeConstructor) {
        Iterator<TypeVertex> iterator = Iterators.filter(vertex.outs().edge(Schema.Edge.Type.SUB).to(),
                                                         v -> v.schema().equals(vertex.schema()));
        if (iterator.hasNext()) return typeConstructor.apply(iterator.next());
        else return null;
    }

    <TYPE> Stream<TYPE> sups(Function<TypeVertex, TYPE> typeConstructor) {
        return stream(apply(loop(
                vertex,
                v -> v != null && v.schema().equals(this.vertex.schema()),
                v -> {
                    Iterator<TypeVertex> p = v.outs().edge(Schema.Edge.Type.SUB).to();
                    if (p.hasNext()) return p.next();
                    else return null;
                }), typeConstructor));
    }

    <TYPE> Stream<TYPE> subs(Function<TypeVertex, TYPE> typeConstructor) {
        return stream(apply(tree(vertex, v -> v.ins().edge(Schema.Edge.Type.SUB).from()), typeConstructor));
    }

    <THING> Stream<THING> instances(Function<ThingVertex, THING> thingConstructor) {
        return subs().flatMap(t -> stream(((TypeImpl) t).vertex.instances())).map(thingConstructor);
    }

    @Override
    public void validate() {
        // TODO: Add any validation that would apply to all Types here
    }

    void validateIsCommitedAndNotAbstract(Class<?> instanceClass) {
        if (vertex.status().equals(Schema.Status.BUFFERED)) {
            throw new HypergraphException(Error.Transaction.DIRTY_DATA_WRITES);
        } else if (isAbstract()) {
            throw new HypergraphException(ILLEGAL_ABSTRACT_WRITE.format(instanceClass.getSimpleName(), label()));
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
        TypeImpl that = (TypeImpl) object;
        return this.vertex.equals(that.vertex);
    }

    @Override
    public final int hashCode() {
        return vertex.hashCode(); // does not need caching
    }
}
