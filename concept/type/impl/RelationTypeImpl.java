/*
 * Copyright (C) 2022 Vaticle
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
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typedb.core.common.util.StringBuilders;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.thing.impl.RelationImpl;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.Type;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.edge.TypeEdge;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typeql.lang.common.TypeQLToken;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.TYPE_ROOT_MISMATCH;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.RELATION_ABSTRACT_ROLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.RELATION_NO_ROLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.RELATION_RELATES_ROLE_FROM_SUPERTYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.RELATION_RELATES_ROLE_NOT_AVAILABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.ROOT_TYPE_MUTATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.TYPE_HAS_INSTANCES_SET_ABSTRACT;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.iterateSorted;
import static com.vaticle.typedb.core.common.parameters.Order.Asc.ASC;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Type.RELATES;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Type.RELATION_TYPE;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Type.Root.RELATION;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Type.Root.ROLE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.SPACE;
import static java.util.Comparator.comparing;

public class RelationTypeImpl extends ThingTypeImpl implements RelationType {

    private RelationTypeImpl(GraphManager graphMgr, TypeVertex vertex) {
        super(graphMgr, vertex);
        if (vertex.encoding() != RELATION_TYPE) {
            throw exception(TypeDBException.of(TYPE_ROOT_MISMATCH, vertex.label(),
                    RELATION_TYPE.root().label(), vertex.encoding().root().label()));
        }
    }

    private RelationTypeImpl(GraphManager graphMgr, String label) {
        super(graphMgr, label, RELATION_TYPE);
    }

    public static RelationTypeImpl of(GraphManager graphMgr, TypeVertex vertex) {
        if (vertex.label().equals(RELATION.label())) return new RelationTypeImpl.Root(graphMgr, vertex);
        else return new RelationTypeImpl(graphMgr, vertex);
    }

    public static RelationType of(GraphManager graphMgr, String label) {
        return new RelationTypeImpl(graphMgr, label);
    }

    @Override
    public void setLabel(String label) {
        vertex.label(label);
        vertex.outs().edge(RELATES).to().forEachRemaining(v -> v.scope(label));
    }

    @Override
    public void setAbstract() {
        if (isAbstract()) return;
        if (getInstancesExplicit().first().isPresent()) {
            throw exception(TypeDBException.of(TYPE_HAS_INSTANCES_SET_ABSTRACT, getLabel()));
        }
        vertex.isAbstract(true);
        declaredRoles().forEachRemaining(RoleTypeImpl::setAbstract);
    }

    @Override
    public void unsetAbstract() {
        vertex.isAbstract(false);
        declaredRoles().forEachRemaining(RoleTypeImpl::unsetAbstract);
    }

    @Override
    public void setSupertype(RelationType superType) {
        validateIsNotDeleted();
        setSuperTypeVertex(((RelationTypeImpl) superType).vertex);
    }

    @Override
    public Forwardable<RelationTypeImpl, Order.Asc> getSubtypes() {
        return iterateSorted(graphMgr.schema().getSubtypes(vertex), ASC)
                .mapSorted(v -> of(graphMgr, v), relationType -> relationType.vertex, ASC);
    }

    @Override
    public Forwardable<RelationTypeImpl, Order.Asc> getSubtypesExplicit() {
        return super.getSubtypesExplicit(v -> of(graphMgr, v));
    }

    @Override
    public Forwardable<RelationImpl, Order.Asc> getInstances() {
        return instances(RelationImpl::of);
    }

    @Override
    public Forwardable<RelationImpl, Order.Asc> getInstancesExplicit() {
        return instancesExplicit(RelationImpl::of);
    }

    @Override
    public void setRelates(String roleLabel) {
        validateIsNotDeleted();
        TypeVertex roleTypeVertex = graphMgr.schema().getType(roleLabel, vertex.label());
        if (roleTypeVertex == null && getSupertypes()
                .filter(t -> !t.equals(this) && t.isRelationType()).map(TypeImpl::asRelationType)
                .flatMap(RelationType::getRelates).anyMatch(role -> role.getLabel().name().equals(roleLabel))) {
            throw exception(TypeDBException.of(RELATION_RELATES_ROLE_FROM_SUPERTYPE, roleLabel, getLabel()));
        } else {
            RoleTypeImpl roleType;
            if (roleTypeVertex == null) {
                roleType = RoleTypeImpl.of(graphMgr, roleLabel, vertex.label());
                if (this.isAbstract()) roleType.setAbstract();
                vertex.outs().put(RELATES, roleType.vertex);
            } else {
                roleType = RoleTypeImpl.of(graphMgr, roleTypeVertex);
                roleType.setSupertype(RoleTypeImpl.of(graphMgr, graphMgr.schema().rootRoleType()));
            }
            assert roleType.getSupertype() != null;
            vertex.outs().edge(RELATES, roleType.vertex).overridden(roleType.getSupertype().vertex);
        }
    }

    @Override
    public void setRelates(String roleLabel, String overriddenLabel) {
        validateIsNotDeleted();
        setRelates(roleLabel);
        RoleTypeImpl roleType = this.getRelates(roleLabel);

        Optional<RoleTypeImpl> inherited;
        assert getSupertype() != null;
        if (declaredRoles().anyMatch(r -> r.getLabel().name().equals(overriddenLabel)) ||
                !(inherited = getSupertype().asRelationType().getRelates()
                        .filter(role -> role.getLabel().name().equals(overriddenLabel)).first()
                ).isPresent()
        ) { throw exception(TypeDBException.of(RELATION_RELATES_ROLE_NOT_AVAILABLE, roleLabel, overriddenLabel)); }

        roleType.setSupertype(inherited.get());
        vertex.outs().edge(RELATES, roleType.vertex).overridden(inherited.get().vertex);
    }

    @Override
    public void unsetRelates(String roleLabel) {
        validateIsNotDeleted();
        getRelates(roleLabel).delete();
    }

    @Override
    public Forwardable<RoleTypeImpl, Order.Asc> getRelates() {
        return iterateSorted(graphMgr.schema().relatedRoleTypes(vertex), ASC)
                .mapSorted(v -> RoleTypeImpl.of(graphMgr, v), roleType -> roleType.vertex, ASC);
    }

    @Override
    public Forwardable<RoleTypeImpl, Order.Asc> getRelatesExplicit() {
        return vertex.outs().edge(RELATES).to()
                .mapSorted(v -> RoleTypeImpl.of(graphMgr, v), roleType -> roleType.vertex, ASC);
    }

    @Override
    public RoleType getRelatesOverridden(String roleLabel) {
        TypeVertex roleVertex = graphMgr.schema().getType(roleLabel, vertex.label());
        if (roleVertex != null) {
            TypeEdge relatesEdge = vertex.outs().edge(RELATES, roleVertex);
            if (relatesEdge != null &&
                    relatesEdge.overridden().isPresent() &&
                    !relatesEdge.overridden().get().equals(graphMgr.schema().rootRoleType()))
                return RoleTypeImpl.of(graphMgr, relatesEdge.overridden().get());
        }
        return null;
    }

    FunctionalIterator<RoleTypeImpl> overriddenRoles() {
        return vertex.outs().edge(RELATES).overridden().filter(Objects::nonNull).map(v -> RoleTypeImpl.of(graphMgr, v));
    }

    private FunctionalIterator<RoleTypeImpl> declaredRoles() {
        return vertex.outs().edge(RELATES).to().map(v -> RoleTypeImpl.of(graphMgr, v));
    }

    /**
     * Get the role type with a given {@code roleLabel} related by this relation type.
     *
     * First, look up the role type by the given label and it's scope: the relation label.
     * If the role type vertex do not exist, then call {@code role()} to get the inherited role types,
     * and see if the any of them has the {@code roleLabel} of interest.
     *
     * @param roleLabel the label of the role
     * @return the role type related in this relation
     */
    @Override
    public RoleTypeImpl getRelates(String roleLabel) {
        Optional<RoleTypeImpl> roleType;
        TypeVertex roleTypeVertex = graphMgr.schema().getType(roleLabel, vertex.label());
        if (roleTypeVertex != null) {
            return RoleTypeImpl.of(graphMgr, roleTypeVertex);
        } else if ((roleType = getRelates().filter(role -> role.getLabel().name().equals(roleLabel)).first()).isPresent()) {
            return roleType.get();
        } else return null;
    }

    @Override
    public RoleTypeImpl getRelatesExplicit(String roleLabel) {
        TypeVertex roleTypeVertex = graphMgr.schema().getType(roleLabel, vertex.label());
        if (roleTypeVertex != null) return RoleTypeImpl.of(graphMgr, roleTypeVertex);
        else return null;
    }

    @Override
    public void delete() {
        validateDelete();
        declaredRoles().forEachRemaining(RoleTypeImpl::delete);
        vertex.delete();
    }

    @Override
    public List<TypeDBException> validate() {
        List<TypeDBException> exceptions = super.validate();
        if (!isRoot() && !isAbstract() && !getRelates().filter(r -> !r.getLabel().equals(ROLE.properLabel())).hasNext()) {
            exceptions.add(TypeDBException.of(RELATION_NO_ROLE, this.getLabel()));
        } else if (!isAbstract()) {
            getRelates().filter(Type::isAbstract).forEachRemaining(
                    rt -> exceptions.add(TypeDBException.of(RELATION_ABSTRACT_ROLE, getLabel(), rt.getLabel()))
            );
        }
        return exceptions;
    }

    @Override
    public RelationImpl create() {
        return create(false);
    }

    @Override
    public RelationImpl create(boolean isInferred) {
        validateCanHaveInstances(Relation.class);
        ThingVertex.Write instance = graphMgr.data().create(vertex, isInferred);
        return RelationImpl.of(instance);
    }

    @Override
    public boolean isRelationType() {
        return true;
    }

    @Override
    public RelationTypeImpl asRelationType() {
        return this;
    }

    @Override
    public void getSyntax(StringBuilder builder) {
        writeSupertype(builder);
        writeAbstract(builder);
        writeOwnsAttributes(builder);
        writeRelates(builder);
        writePlays(builder);
        builder.append(StringBuilders.SEMICOLON_NEWLINE_X2);
    }

    private void writeRelates(StringBuilder builder) {
        getRelatesExplicit().stream().sorted(comparing(x -> x.getLabel().name())).forEach(roleType -> {
            builder.append(StringBuilders.COMMA_NEWLINE_INDENT)
                    .append(TypeQLToken.Constraint.RELATES).append(SPACE)
                    .append(roleType.getLabel().name());
            RoleType overridden = getRelatesOverridden(roleType.getLabel().name());
            if (overridden != null) {
                builder.append(SPACE).append(TypeQLToken.Constraint.AS).append(SPACE)
                        .append(overridden.getLabel().name());
            }
        });
    }

    public static class Root extends RelationTypeImpl {

        Root(GraphManager graphMgr, TypeVertex vertex) {
            super(graphMgr, vertex);
            assert vertex.label().equals(RELATION.label());
        }

        @Override
        public boolean isRoot() {
            return true;
        }

        @Override
        public void setLabel(String label) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void unsetAbstract() {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setSupertype(RelationType superType) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setRelates(String roleLabel) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setRelates(String roleLabel, String overriddenLabel) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void unsetRelates(String roleLabel) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setOwns(AttributeType attributeType, boolean isKey) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setPlays(RoleType roleType) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setPlays(RoleType roleType, RoleType overriddenType) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void unsetPlays(RoleType roleType) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }
    }
}
