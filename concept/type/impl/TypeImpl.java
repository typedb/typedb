/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.concept.type.impl;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concept.ConceptImpl;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.Type;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.structure.RuleStructure;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typeql.lang.TypeQL;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_ABSTRACT_WRITE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.SESSION_SCHEMA_VIOLATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.INVALID_TYPE_CASTING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.CYCLIC_TYPE_HIERARCHY;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.TYPE_HAS_BEEN_DELETED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.TYPE_REFERENCED_IN_RULES;
import static com.vaticle.typedb.core.common.iterator.Iterators.tree;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.SUB;

public abstract class TypeImpl extends ConceptImpl implements Type {

    protected final GraphManager graphMgr;
    private final TypeVertex vertex;

    TypeImpl(GraphManager graphMgr, TypeVertex vertex) {
        this.graphMgr = graphMgr;
        this.vertex = Objects.requireNonNull(vertex);
    }

    TypeImpl(GraphManager graphMgr, String label, Encoding.Vertex.Type encoding) {
        this(graphMgr, label, encoding, null);
    }

    TypeImpl(GraphManager graphMgr, String label, Encoding.Vertex.Type encoding, String scope) {
        label = TypeQL.parseLabel(label);
        this.graphMgr = graphMgr;
        this.vertex = graphMgr.schema().create(encoding, label, scope);
        TypeVertex superTypeVertex = graphMgr.schema().getType(encoding.root().label(), encoding.root().scope());
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

    public TypeVertex vertex() {
        return vertex;
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
    public long getInstancesCount() {
        return graphMgr.data().stats().thingVertexTransitiveCount(vertex);
    }

    @Override
    public void setLabel(String label) {
        validateIsNotDeleted();
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
    public abstract FunctionalIterator<? extends TypeImpl> getSubtypes();

    @Override
    public abstract FunctionalIterator<? extends Type> getSubtypesExplicit();

    <THING> FunctionalIterator<THING> instances(Function<ThingVertex.Write, THING> thingConstructor) {
        return getSubtypes().flatMap(t -> graphMgr.data().getWritable(t.vertex())).map(thingConstructor);
    }

    void setSuperTypeVertex(TypeVertex superTypeVertex) {
        vertex.outs().edge(SUB, ((TypeImpl) getSupertype()).vertex).delete();
        vertex.outs().put(SUB, superTypeVertex);
        validateTypeHierarchyIsNotCyclic();
    }

    private void validateTypeHierarchyIsNotCyclic() {
        TypeImpl type = this;
        LinkedHashSet<String> hierarchy = new LinkedHashSet<>();
        hierarchy.add(vertex.scopedLabel());
        while (!type.isRoot()) {
            assert type.getSupertype() != null;
            type = (TypeImpl) type.getSupertype();
            if (!hierarchy.add(type.vertex.scopedLabel())) {
                throw exception(TypeDBException.of(CYCLIC_TYPE_HIERARCHY, hierarchy));
            }
        }
    }

    <TYPE extends Type> FunctionalIterator<TYPE> getSubtypes(Function<TypeVertex, TYPE> typeConstructor) {
        return tree(vertex, v -> v.ins().edge(SUB).from()).map(typeConstructor);
    }

    <TYPE extends Type> FunctionalIterator<TYPE> getSubtypesExplicit(Function<TypeVertex, TYPE> typeConstructor) {
        return vertex.ins().edge(SUB).from().map(typeConstructor);
    }

    void validateDelete() {
        TypeVertex type = graphMgr.schema().getType(getLabel());
        FunctionalIterator<RuleStructure> rules = graphMgr.schema().rules().references().get(type);
        if (rules.hasNext()) {
            throw exception(TypeDBException.of(TYPE_REFERENCED_IN_RULES, getLabel(), rules.toList()));
        }
    }

    @Override
    public List<TypeDBException> validate() {
        return new ArrayList<>();
    }

    @Override
    public boolean isType() { return true; }

    @Override
    public TypeImpl asType() { return this; }

    @Override
    public EntityTypeImpl asEntityType() {
        throw exception(TypeDBException.of(INVALID_TYPE_CASTING, getLabel(), className(EntityType.class)));
    }

    @Override
    public RelationTypeImpl asRelationType() {
        throw exception(TypeDBException.of(INVALID_TYPE_CASTING, getLabel(), className(RelationType.class)));
    }

    @Override
    public AttributeTypeImpl asAttributeType() {
        throw exception(TypeDBException.of(INVALID_TYPE_CASTING, getLabel(), className(AttributeType.class)));
    }

    @Override
    public RoleTypeImpl asRoleType() {
        throw exception(TypeDBException.of(INVALID_TYPE_CASTING, getLabel(), className(RoleType.class)));
    }

    void validateCanHaveInstances(Class<?> instanceClass) {
        validateIsNotDeleted();
        if (vertex.status().equals(Encoding.Status.BUFFERED)) {
            throw exception(TypeDBException.of(SESSION_SCHEMA_VIOLATION));
        } else if (isAbstract()) {
            throw exception(TypeDBException.of(ILLEGAL_ABSTRACT_WRITE, instanceClass.getSimpleName(), getLabel()));
        }
    }

    void validateIsNotDeleted() {
        if (vertex.isDeleted()) throw exception(TypeDBException.of(TYPE_HAS_BEEN_DELETED, getLabel()));
    }

    @Override
    public TypeDBException exception(TypeDBException exception) {
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
        TypeImpl that = (TypeImpl) object;
        return this.vertex.equals(that.vertex);
    }

    @Override
    public final int hashCode() {
        return vertex.hashCode(); // does not need caching
    }
}
