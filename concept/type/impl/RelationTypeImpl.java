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

import grakn.core.common.collection.Streams;
import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.impl.RelationImpl;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.graph.GraphManager;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_ROOT_MISMATCH;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.RELATION_ABSTRACT_ROLE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.RELATION_NO_ROLE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.RELATION_RELATES_ROLE_FROM_SUPERTYPE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.RELATION_RELATES_ROLE_NOT_AVAILABLE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ROOT_TYPE_MUTATION;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.TYPE_HAS_INSTANCES;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.TYPE_HAS_SUBTYPES;
import static grakn.core.graph.util.Encoding.Edge.Type.RELATES;
import static grakn.core.graph.util.Encoding.Vertex.Type.RELATION_TYPE;
import static grakn.core.graph.util.Encoding.Vertex.Type.Root.RELATION;
import static grakn.core.graph.util.Encoding.Vertex.Type.Root.ROLE;

public class RelationTypeImpl extends ThingTypeImpl implements RelationType {

    private RelationTypeImpl(GraphManager graphMgr, TypeVertex vertex) {
        super(graphMgr, vertex);
        if (vertex.encoding() != RELATION_TYPE) {
            throw exception(GraknException.of(TYPE_ROOT_MISMATCH, vertex.label(),
                                              RELATION_TYPE.root().label(), vertex.encoding().root().label()));
        }
    }

    private RelationTypeImpl(GraphManager graphMgr, String label) {
        super(graphMgr, label, RELATION_TYPE);
    }

    public static RelationTypeImpl of(GraphManager graphMgr, TypeVertex vertex) {
        if (vertex.label().equals(RELATION.label()))
            return new RelationTypeImpl.Root(graphMgr, vertex);
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
        if (getInstances().findFirst().isPresent()) throw exception(GraknException.of(TYPE_HAS_INSTANCES, getLabel()));
        vertex.isAbstract(true);
        declaredRoles().forEach(RoleTypeImpl::setAbstract);
    }

    @Override
    public void unsetAbstract() {
        vertex.isAbstract(false);
        declaredRoles().forEach(RoleTypeImpl::unsetAbstract);
    }

    @Override
    public void setSupertype(RelationType superType) {
        super.setSuperTypeVertex(((RelationTypeImpl) superType).vertex);
    }

    @Nullable
    @Override
    public RelationTypeImpl getSupertype() {
        return super.getSupertype(v -> of(graphMgr, v));
    }

    @Override
    public Stream<RelationTypeImpl> getSupertypes() {
        return super.getSupertypes(v -> of(graphMgr, v));
    }

    @Override
    public Stream<RelationTypeImpl> getSubtypes() {
        return super.getSubtypes(v -> of(graphMgr, v));
    }

    @Override
    public Stream<RelationImpl> getInstances() {
        return instances(RelationImpl::of);
    }

    @Override
    public void setRelates(String roleLabel) {
        final TypeVertex roleTypeVertex = graphMgr.schema().getType(roleLabel, vertex.label());
        if (roleTypeVertex == null) {
            if (getSupertypes().filter(t -> !t.equals(this)).flatMap(RelationType::getRelates).anyMatch(role -> role.getLabel().name().equals(roleLabel))) {
                throw exception(GraknException.of(RELATION_RELATES_ROLE_FROM_SUPERTYPE, roleLabel));
            } else {
                final RoleTypeImpl roleType = RoleTypeImpl.of(graphMgr, roleLabel, vertex.label());
                if (this.isAbstract()) roleType.setAbstract();
                vertex.outs().put(RELATES, roleType.vertex);
                vertex.outs().edge(RELATES, roleType.vertex).overridden(roleType.getSupertype().vertex);
            }
        }
    }

    @Override
    public void setRelates(String roleLabel, String overriddenLabel) {
        this.setRelates(roleLabel);
        final RoleTypeImpl roleType = this.getRelates(roleLabel);

        final Optional<RoleTypeImpl> inherited;
        if (declaredRoles().anyMatch(r -> r.getLabel().name().equals(overriddenLabel)) ||
                !(inherited = getSupertype().getRelates().filter(role -> role.getLabel().name().equals(overriddenLabel)).findFirst()).isPresent()) {
            throw exception(GraknException.of(RELATION_RELATES_ROLE_NOT_AVAILABLE, roleLabel, overriddenLabel));
        }

        roleType.sup(inherited.get());
        vertex.outs().edge(RELATES, roleType.vertex).overridden(inherited.get().vertex);
    }

    @Override
    public void unsetRelates(String roleLabel) {
        this.getRelates(roleLabel).delete();
    }

    @Override
    public Stream<RoleTypeImpl> getRelates() {
        final ResourceIterator<RoleTypeImpl> roles = vertex.outs().edge(RELATES).to().map(v -> RoleTypeImpl.of(graphMgr, v));
        if (isRoot()) {
            return roles.stream();
        } else {
            final Set<RoleTypeImpl> direct = new HashSet<>();
            roles.forEachRemaining(direct::add);
            return Stream.concat(direct.stream(), getSupertype().getRelates().filter(
                    role -> overriddenRoles().noneMatch(o -> o.equals(role))
            ));
        }
    }

    Stream<RoleTypeImpl> overriddenRoles() {
        return vertex.outs().edge(RELATES).overridden().filter(Objects::nonNull).map(v -> RoleTypeImpl.of(graphMgr, v)).stream();
    }

    private Stream<RoleTypeImpl> declaredRoles() {
        return vertex.outs().edge(RELATES).to().map(v -> RoleTypeImpl.of(graphMgr, v)).stream();
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
        final Optional<RoleTypeImpl> roleType;
        final TypeVertex roleTypeVertex = graphMgr.schema().getType(roleLabel, vertex.label());
        if (roleTypeVertex != null) {
            return RoleTypeImpl.of(graphMgr, roleTypeVertex);
        } else if ((roleType = getRelates().filter(role -> role.getLabel().name().equals(roleLabel)).findFirst()).isPresent()) {
            return roleType.get();
        } else return null;
    }

    @Override
    public void delete() {
        if (getSubtypes().anyMatch(s -> !s.equals(this)))
            throw exception(GraknException.of(TYPE_HAS_SUBTYPES, getLabel()));
        else if (getInstances().findFirst().isPresent())
            throw exception(GraknException.of(TYPE_HAS_INSTANCES, getLabel()));

        declaredRoles().forEach(RoleTypeImpl::delete);
        vertex.delete();
    }

    @Override
    public List<GraknException> validate() {
        final List<GraknException> exceptions = super.validate();
        if (!isRoot() && !isAbstract() && Streams.compareSize(getRelates().filter(r -> !r.getLabel().name().equals(ROLE.label())), 1) < 0) {
            exceptions.add(GraknException.of(RELATION_NO_ROLE, this.getLabel()));
        } else if (!isAbstract()) {
            getRelates().filter(TypeImpl::isAbstract).forEach(roleType -> {
                exceptions.add(GraknException.of(RELATION_ABSTRACT_ROLE, getLabel(), roleType.getLabel()));
            });
        }
        return exceptions;
    }

    @Override
    public RelationImpl create() {
        return create(false);
    }

    @Override
    public RelationImpl create(boolean isInferred) {
        validateIsCommittedAndNotAbstract(Relation.class);
        final ThingVertex instance = graphMgr.data().create(vertex, isInferred);
        return RelationImpl.of(instance);
    }

    @Override
    public RelationTypeImpl asRelationType() { return this; }

    public static class Root extends RelationTypeImpl {

        Root(GraphManager graphMgr, TypeVertex vertex) {
            super(graphMgr, vertex);
            assert vertex.label().equals(RELATION.label());
        }

        @Override
        public boolean isRoot() { return true; }

        @Override
        public void setLabel(String label) {
            throw exception(GraknException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void unsetAbstract() {
            throw exception(GraknException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setSupertype(RelationType superType) {
            throw exception(GraknException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setRelates(String roleLabel) {
            throw exception(GraknException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setRelates(String roleLabel, String overriddenLabel) {
            throw exception(GraknException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void unsetRelates(String roleLabel) {
            throw exception(GraknException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setOwns(AttributeType attributeType, boolean isKey) {
            throw exception(GraknException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setOwns(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
            throw exception(GraknException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setPlays(RoleType roleType) {
            throw exception(GraknException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setPlays(RoleType roleType, RoleType overriddenType) {
            throw exception(GraknException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void unsetPlays(RoleType roleType) {
            throw exception(GraknException.of(ROOT_TYPE_MUTATION));
        }
    }
}
