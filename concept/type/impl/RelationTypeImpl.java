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
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.impl.RelationImpl;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.graph.TypeGraph;
import grakn.core.graph.util.Schema;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static grakn.core.common.exception.Error.TypeRead.TYPE_ROOT_MISMATCH;
import static grakn.core.common.exception.Error.TypeWrite.RELATION_ABSTRACT_ROLE;
import static grakn.core.common.exception.Error.TypeWrite.RELATION_NO_ROLE;
import static grakn.core.common.exception.Error.TypeWrite.RELATION_RELATES_ROLE_FROM_SUPERTYPE;
import static grakn.core.common.exception.Error.TypeWrite.RELATION_RELATES_ROLE_NOT_AVAILABLE;
import static grakn.core.common.exception.Error.TypeWrite.ROOT_TYPE_MUTATION;
import static grakn.core.common.exception.Error.TypeWrite.TYPE_HAS_INSTANCES;
import static grakn.core.common.exception.Error.TypeWrite.TYPE_HAS_SUBTYPES;
import static grakn.core.common.iterator.Iterators.apply;
import static grakn.core.common.iterator.Iterators.filter;
import static grakn.core.common.iterator.Iterators.stream;
import static grakn.core.graph.util.Schema.Vertex.Type.Root.ROLE;

public class RelationTypeImpl extends ThingTypeImpl implements RelationType {

    private RelationTypeImpl(TypeVertex vertex) {
        super(vertex);
        if (vertex.schema() != Schema.Vertex.Type.RELATION_TYPE) {
            throw new GraknException(TYPE_ROOT_MISMATCH.message(
                    vertex.label(),
                    Schema.Vertex.Type.RELATION_TYPE.root().label(),
                    vertex.schema().root().label()
            ));
        }
    }

    private RelationTypeImpl(TypeGraph graph, String label) {
        super(graph, label, Schema.Vertex.Type.RELATION_TYPE);
    }

    public static RelationTypeImpl of(TypeVertex vertex) {
        if (vertex.label().equals(Schema.Vertex.Type.Root.RELATION.label())) return new RelationTypeImpl.Root(vertex);
        else return new RelationTypeImpl(vertex);
    }

    public static RelationType of(TypeGraph graph, String label) {
        return new RelationTypeImpl(graph, label);
    }

    @Override
    public void label(String label) {
        vertex.label(label);
        vertex.outs().edge(Schema.Edge.Type.RELATES).to().forEachRemaining(v -> v.scope(label));
    }

    @Override
    public void isAbstract(boolean isAbstract) {
        if (isAbstract && instances().findFirst().isPresent()) {
            throw new GraknException(TYPE_HAS_INSTANCES.message(label()));
        }
        vertex.isAbstract(isAbstract);
        declaredRoles().forEach(role -> role.isAbstract(isAbstract));
    }

    @Override
    public void sup(RelationType superType) {
        super.superTypeVertex(((RelationTypeImpl) superType).vertex);
    }

    @Nullable
    @Override
    public RelationTypeImpl sup() {
        return super.sup(RelationTypeImpl::of);
    }

    @Override
    public Stream<RelationTypeImpl> sups() {
        return super.sups(RelationTypeImpl::of);
    }

    @Override
    public Stream<RelationTypeImpl> subs() {
        return super.subs(RelationTypeImpl::of);
    }

    @Override
    public Stream<RelationImpl> instances() {
        return super.instances(RelationImpl::of);
    }

    @Override
    public RoleTypeImpl relates(String roleLabel) {
        TypeVertex roleTypeVertex = vertex.graph().get(roleLabel, vertex.label());
        if (roleTypeVertex == null) {
            if (sups().filter(t -> !t.equals(this)).flatMap(RelationType::roles).anyMatch(role -> role.label().equals(roleLabel))) {
                throw new GraknException(RELATION_RELATES_ROLE_FROM_SUPERTYPE.message(roleLabel));
            } else {
                RoleTypeImpl roleType = RoleTypeImpl.of(vertex.graph(), roleLabel, vertex.label());
                roleType.isAbstract(this.isAbstract());
                vertex.outs().put(Schema.Edge.Type.RELATES, roleType.vertex);
                vertex.outs().edge(Schema.Edge.Type.RELATES, roleType.vertex).overridden(roleType.sup().vertex);
                return roleType;
            }
        }
        return RoleTypeImpl.of(roleTypeVertex);
    }

    @Override
    public RoleTypeImpl relates(String roleLabel, String overriddenLabel) {
        RoleTypeImpl roleType = this.relates(roleLabel);

        Optional<RoleTypeImpl> inherited;
        if (declaredRoles().anyMatch(r -> r.label().equals(overriddenLabel)) ||
                !(inherited = sup().roles().filter(role -> role.label().equals(overriddenLabel)).findFirst()).isPresent()) {
            throw new GraknException(RELATION_RELATES_ROLE_NOT_AVAILABLE.message(roleLabel, overriddenLabel));
        }

        roleType.sup(inherited.get());
        vertex.outs().edge(Schema.Edge.Type.RELATES, roleType.vertex).overridden(inherited.get().vertex);
        return roleType;
    }

    @Override
    public Stream<RoleTypeImpl> roles() {
        Iterator<RoleTypeImpl> roles = apply(vertex.outs().edge(Schema.Edge.Type.RELATES).to(), RoleTypeImpl::of);
        if (isRoot()) {
            return stream(roles);
        } else {
            Set<RoleTypeImpl> direct = new HashSet<>(), overridden = new HashSet<>();
            roles.forEachRemaining(direct::add);
            filter(vertex.outs().edge(Schema.Edge.Type.RELATES).overridden(), Objects::nonNull)
                    .apply(RoleTypeImpl::of)
                    .forEachRemaining(overridden::add);
            return Stream.concat(direct.stream(), sup().roles().filter(role -> !overridden.contains(role)));
        }
    }

    private Stream<RoleTypeImpl> declaredRoles() {
        return stream(apply(vertex.outs().edge(Schema.Edge.Type.RELATES).to(), RoleTypeImpl::of));
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
    public RoleTypeImpl role(String roleLabel) {
        Optional<RoleTypeImpl> roleType;
        TypeVertex roleTypeVertex = vertex.graph().get(roleLabel, vertex.label());
        if (roleTypeVertex != null) {
            return RoleTypeImpl.of(roleTypeVertex);
        } else if ((roleType = roles().filter(role -> role.label().equals(roleLabel)).findFirst()).isPresent()) {
            return roleType.get();
        } else return null;
    }

    @Override
    public void delete() {
        if (subs().anyMatch(s -> !s.equals(this))) {
            throw new GraknException(TYPE_HAS_SUBTYPES.message(label()));
        } else if (subs().flatMap(RelationTypeImpl::instances).findFirst().isPresent()) {
            throw new GraknException(TYPE_HAS_INSTANCES.message(label()));
        } else {
            declaredRoles().forEach(RoleTypeImpl::delete);
            vertex.delete();
        }
    }

    @Override
    public List<GraknException> validate() {
        List<GraknException> exceptions = super.validate();
        if (!isRoot() && Streams.compareSize(roles().filter(r -> !r.label().equals(ROLE.label())), 1) < 0) {
            exceptions.add(new GraknException(RELATION_NO_ROLE.message(this.label())));
        } else if (!isAbstract()) {
            roles().filter(TypeImpl::isAbstract).forEach(roleType -> {
                exceptions.add(new GraknException(RELATION_ABSTRACT_ROLE.message(label(), roleType.label())));
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
        validateIsCommitedAndNotAbstract(Relation.class);
        ThingVertex instance = vertex.graph().thing().create(vertex.iid(), isInferred);
        return RelationImpl.of(instance);
    }

    public static class Root extends RelationTypeImpl {

        Root(TypeVertex vertex) {
            super(vertex);
            assert vertex.label().equals(Schema.Vertex.Type.Root.RELATION.label());
        }

        @Override
        public boolean isRoot() { return true; }

        @Override
        public void label(String label) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }

        @Override
        public void isAbstract(boolean isAbstract) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }

        @Override
        public void sup(RelationType superType) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }

        @Override
        public void has(AttributeType attributeType, boolean isKey) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }

        @Override
        public void has(AttributeType attributeType, AttributeType overriddenType, boolean isKey) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }

        @Override
        public void plays(RoleType roleType) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }

        @Override
        public void plays(RoleType roleType, RoleType overriddenType) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }

        @Override
        public void unplay(RoleType roleType) {
            throw new GraknException(ROOT_TYPE_MUTATION);
        }
    }
}
