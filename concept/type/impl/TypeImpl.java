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
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Label;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.ThingType;
import grakn.core.graph.GraphManager;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_ABSTRACT_WRITE;
import static grakn.core.common.exception.ErrorMessage.Transaction.SESSION_SCHEMA_VIOLATION;
import static grakn.core.common.exception.ErrorMessage.TypeRead.INVALID_TYPE_CASTING;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.CYCLIC_TYPE_HIERARCHY;
import static grakn.core.common.iterator.Iterators.loop;
import static grakn.core.common.iterator.Iterators.tree;
import static grakn.core.graph.util.Encoding.Edge.Type.SUB;

public abstract class TypeImpl implements grakn.core.concept.type.Type {

    protected final GraphManager graphMgr;
    public final TypeVertex vertex;

    TypeImpl(GraphManager graphMgr, TypeVertex vertex) {
        this.graphMgr = graphMgr;
        this.vertex = Objects.requireNonNull(vertex);
    }

    TypeImpl(GraphManager graphMgr, String label, Encoding.Vertex.Type encoding) {
        this(graphMgr, label, encoding, null);
    }

    TypeImpl(GraphManager graphMgr, String label, Encoding.Vertex.Type encoding, String scope) {
        this.graphMgr = graphMgr;
        this.vertex = graphMgr.schema().create(encoding, label, scope);
        final TypeVertex superTypeVertex = graphMgr.schema().getType(encoding.root().label(), encoding.root().scope());
        vertex.outs().put(SUB, superTypeVertex);
    }

    public static TypeImpl of(GraphManager graphMgr, TypeVertex vertex) {
        switch (vertex.encoding()) {
            case ROLE_TYPE:
                return RoleTypeImpl.of(graphMgr, vertex);
            default:
                return ThingTypeImpl.of(graphMgr, vertex);
        }
    }

    @Override
    public boolean isDeleted() {
        return vertex.isDeleted();
    }

    @Override
    public boolean isRoot() {
        return false;
    }

    @Override
    public Long count() {
        return 0L; // TODO: return total number of type instances
    }

    @Override
    public void setLabel(String label) {
        vertex.label(label);
    }

    @Override
    public Label getLabel() {
        return vertex.properLabel();
    }

    @Override
    public boolean isAbstract() {
        return vertex.isAbstract();
    }

    @Override
    public abstract Stream<? extends TypeImpl> getSubtypes();

    <THING> Stream<THING> instances(Function<ThingVertex, THING> thingConstructor) {
        return getSubtypes().flatMap(t -> graphMgr.data().get(t.vertex).stream()).map(thingConstructor);
    }

    void setSuperTypeVertex(TypeVertex superTypeVertex) {
        vertex.outs().edge(SUB, ((TypeImpl) getSupertype()).vertex).delete();
        vertex.outs().put(SUB, superTypeVertex);
        validateTypeHierarchyIsNotCyclic();
    }

    private void validateTypeHierarchyIsNotCyclic() {
        TypeImpl type = this;
        final LinkedHashSet<String> hierarchy = new LinkedHashSet<>();
        hierarchy.add(vertex.scopedLabel());
        while (!type.isRoot()) {
            assert type.getSupertype() != null;
            type = (TypeImpl) type.getSupertype();
            if (!hierarchy.add(type.vertex.scopedLabel())) {
                throw GraknException.of(CYCLIC_TYPE_HIERARCHY, hierarchy);
            }
        }
    }

    @Nullable
    <TYPE extends grakn.core.concept.type.Type> TYPE getSupertype(Function<TypeVertex, TYPE> typeConstructor) {
        final ResourceIterator<TypeVertex> iterator = vertex.outs().edge(SUB).to().filter(v -> v.encoding().equals(vertex.encoding()));
        if (iterator.hasNext()) return typeConstructor.apply(iterator.next());
        else return null;
    }

    <TYPE extends grakn.core.concept.type.Type> Stream<TYPE> getSupertypes(Function<TypeVertex, TYPE> typeConstructor) {
        return loop(
                vertex,
                Objects::nonNull,
                v -> v.outs().edge(SUB).to().filter(s -> s.encoding().equals(vertex.encoding())).firstOrNull()
        ).map(typeConstructor).stream();
    }

    <TYPE extends grakn.core.concept.type.Type> Stream<TYPE> getSubtypes(Function<TypeVertex, TYPE> typeConstructor) {
        return tree(vertex, v -> v.ins().edge(SUB).from()).map(typeConstructor).stream();
    }

    @Override
    public List<GraknException> validate() {
        return new ArrayList<>();
    }

    @Override
    public TypeImpl asType() { return this; }

    @Override
    public boolean isType() { return true; }

    @Override
    public ThingTypeImpl asThingType() {
        throw exception(GraknException.of(INVALID_TYPE_CASTING, className(this.getClass()), className(ThingType.class)));
    }

    @Override
    public EntityTypeImpl asEntityType() {
        throw exception(GraknException.of(INVALID_TYPE_CASTING, className(this.getClass()), className(EntityType.class)));
    }

    @Override
    public AttributeTypeImpl asAttributeType() {
        throw exception(GraknException.of(INVALID_TYPE_CASTING, className(this.getClass()), className(AttributeType.class)));
    }

    @Override
    public RelationTypeImpl asRelationType() {
        throw exception(GraknException.of(INVALID_TYPE_CASTING, className(this.getClass()), className(RelationType.class)));
    }

    @Override
    public RoleTypeImpl asRoleType() {
        throw exception(GraknException.of(INVALID_TYPE_CASTING, className(this.getClass()), className(RoleType.class)));
    }

    void validateIsCommittedAndNotAbstract(Class<?> instanceClass) {
        if (vertex.status().equals(Encoding.Status.BUFFERED)) {
            throw exception(GraknException.of(SESSION_SCHEMA_VIOLATION));
        } else if (isAbstract()) {
            throw exception(GraknException.of(ILLEGAL_ABSTRACT_WRITE, instanceClass.getSimpleName(), getLabel()));
        }
    }

    @Override
    public GraknException exception(GraknException exception) {
        return graphMgr.exception(exception);
    }

    @Override
    public String toString() {
        return className(this.getClass()) + " {" + vertex.toString() + "}";
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        final TypeImpl that = (TypeImpl) object;
        return this.vertex.equals(that.vertex);
    }

    @Override
    public final int hashCode() {
        return vertex.hashCode(); // does not need caching
    }
}
