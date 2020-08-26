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

package grakn.core.concept.type.impl;

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.Iterators;
import grakn.core.concept.type.Type;
import grakn.core.graph.TypeGraph;
import grakn.core.graph.util.Schema;
import grakn.core.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import static grakn.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_ABSTRACT_WRITE;
import static grakn.core.common.exception.ErrorMessage.Transaction.DIRTY_DATA_WRITES;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.SUPERTYPE_SELF;
import static grakn.core.common.iterator.Iterators.apply;
import static grakn.core.common.iterator.Iterators.loop;
import static grakn.core.common.iterator.Iterators.stream;
import static grakn.core.common.iterator.Iterators.tree;

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
            case ROLE_TYPE:
                return RoleTypeImpl.of(vertex);
            default:
                return ThingTypeImpl.of(vertex);
        }
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
    public void setLabel(String label) {
        vertex.label(label);
    }

    @Override
    public String getLabel() {
        return vertex.label();
    }

    @Override
    public boolean isAbstract() {
        return vertex.isAbstract();
    }

    void superTypeVertex(TypeVertex superTypeVertex) {
        if (vertex.equals(superTypeVertex)) throw new GraknException(SUPERTYPE_SELF.message(vertex.label()));
        vertex.outs().edge(Schema.Edge.Type.SUB, ((TypeImpl) getSupertype()).vertex).delete();
        vertex.outs().put(Schema.Edge.Type.SUB, superTypeVertex);
    }

    @Nullable
    <TYPE extends Type> TYPE sup(final Function<TypeVertex, TYPE> typeConstructor) {
        final Iterator<TypeVertex> iterator = Iterators.filter(vertex.outs().edge(Schema.Edge.Type.SUB).to(),
                                                         v -> v.schema().equals(vertex.schema()));
        if (iterator.hasNext()) return typeConstructor.apply(iterator.next());
        else return null;
    }

    <TYPE extends Type> Stream<TYPE> sups(final Function<TypeVertex, TYPE> typeConstructor) {
        return stream(apply(loop(
                vertex,
                v -> v != null && v.schema().equals(this.vertex.schema()),
                v -> {
                    Iterator<TypeVertex> p = v.outs().edge(Schema.Edge.Type.SUB).to();
                    if (p.hasNext()) return p.next();
                    else return null;
                }), typeConstructor));
    }

    <TYPE extends Type> Stream<TYPE> subs(final Function<TypeVertex, TYPE> typeConstructor) {
        return stream(apply(tree(vertex, v -> v.ins().edge(Schema.Edge.Type.SUB).from()), typeConstructor));
    }

    @Override
    public List<GraknException> validate() {
        return new ArrayList<>();
    }

    void validateIsCommitedAndNotAbstract(Class<?> instanceClass) {
        if (vertex.status().equals(Schema.Status.BUFFERED)) {
            throw new GraknException(DIRTY_DATA_WRITES);
        } else if (isAbstract()) {
            throw new GraknException(ILLEGAL_ABSTRACT_WRITE.message(instanceClass.getSimpleName(), getLabel()));
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
