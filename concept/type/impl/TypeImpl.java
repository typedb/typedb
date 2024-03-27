/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.concept.type.impl;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typedb.core.common.parameters.Concept.Transitivity;
import com.vaticle.typedb.core.concept.ConceptImpl;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.Type;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.structure.RuleStructure;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.common.exception.TypeQLException;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.function.Function;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.TYPEQL_ERROR;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_ABSTRACT_WRITE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.SESSION_SCHEMA_VIOLATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.INVALID_TYPE_CASTING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.CYCLIC_TYPE_HIERARCHY;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.ROOT_TYPE_MUTATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.TYPE_HAS_BEEN_DELETED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.TYPE_REFERENCED_IN_RULES;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.iterateSorted;
import static com.vaticle.typedb.core.common.parameters.Order.Asc.ASC;
import static com.vaticle.typedb.core.common.parameters.Concept.Transitivity.EXPLICIT;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Type.SUB;

public abstract class TypeImpl extends ConceptImpl implements Type {

    public final TypeVertex vertex;

    TypeImpl(ConceptManager conceptMgr, TypeVertex vertex) {
        super(conceptMgr);
        this.vertex = Objects.requireNonNull(vertex);
    }

    TypeImpl(ConceptManager conceptMgr, String label, Encoding.Vertex.Type encoding) {
        this(conceptMgr, label, encoding, null);
    }

    TypeImpl(ConceptManager conceptMgr, String label, Encoding.Vertex.Type encoding, String scope) {
        super(conceptMgr);
        label = TypeQL.parseLabel(label);
        this.vertex = graphMgr().schema().create(encoding, label, scope);
        TypeVertex superTypeVertex = graphMgr().schema().getType(encoding.root().label(), encoding.root().scope());
        vertex.outs().put(SUB, superTypeVertex);
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
        return graphMgr().data().stats().thingVertexTransitiveCount(vertex);
    }

    @Override
    public void setLabel(String label) {
        try {
            TypeQL.parseLabel(label);
        } catch (TypeQLException e) {
            throw TypeDBException.of(TYPEQL_ERROR, e);
        }
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
    public abstract Forwardable<? extends TypeImpl, Order.Asc> getSubtypes();

    @Override
    public abstract Forwardable<? extends TypeImpl, Order.Asc> getSubtypes(Transitivity transitivity);

    <TYPE extends TypeImpl> Forwardable<TYPE, Order.Asc> getSubtypes(Transitivity transitivity, Function<TypeVertex, TYPE> typeConstructor) {
        if (transitivity == EXPLICIT) return vertex.ins().edge(SUB).from().mapSorted(typeConstructor, type -> type.vertex, ASC);
        else return iterateSorted(graphMgr().schema().getSubtypes(vertex), ASC).mapSorted(typeConstructor, type -> type.vertex, ASC);
    }

    GraphManager graphMgr() {
        return conceptMgr.graph();
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

    void validateDelete() {
        if (isRoot()) throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        FunctionalIterator<RuleStructure> rules = graphMgr().schema().rules().references().get(vertex);
        if (rules.hasNext()) {
            throw exception(TypeDBException.of(TYPE_REFERENCED_IN_RULES, getLabel(), rules.map(RuleStructure::label).toList()));
        }
    }

    @Override
    public boolean isType() {
        return true;
    }

    @Override
    public TypeImpl asType() {
        return this;
    }

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
        return conceptMgr.exception(exception);
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

    @Override
    public int compareTo(Type other) {
        return vertex.compareTo(((TypeImpl) other).vertex);
    }
}
