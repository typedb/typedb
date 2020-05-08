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

import hypergraph.common.exception.HypergraphException;
import hypergraph.common.iterator.Iterators;
import hypergraph.concept.type.RelationType;
import hypergraph.concept.type.Type;
import hypergraph.graph.Graph;
import hypergraph.graph.Schema;
import hypergraph.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static hypergraph.common.collection.Streams.compareSize;
import static hypergraph.common.exception.Error.TypeDefinition.INVALID_ROOT_TYPE_MUTATION;
import static hypergraph.common.iterator.Iterators.apply;
import static hypergraph.common.iterator.Iterators.filter;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;

public class RelationTypeImpl extends ThingTypeImpl implements RelationType {

    private RelationTypeImpl(TypeVertex vertex) {
        super(vertex);
        if (vertex.schema() != Schema.Vertex.Type.RELATION_TYPE) {
            throw new HypergraphException("Invalid Relation Type: " + vertex.label() +
                                                  " subtypes " + vertex.schema().root().label());
        }
    }

    private RelationTypeImpl(Graph.Type graph, String label) {
        super(graph, label, Schema.Vertex.Type.RELATION_TYPE);
    }

    public static RelationTypeImpl of(TypeVertex vertex) {
        if (vertex.label().equals(Schema.Vertex.Type.Root.RELATION.label())) return new RelationTypeImpl.Root(vertex);
        else return new RelationTypeImpl(vertex);
    }

    public static RelationType of(Graph.Type graph, String label) {
        return new RelationTypeImpl(graph, label);
    }

    @Override
    public void label(String label) {
        vertex.label(label);
        vertex.outs().edge(Schema.Edge.Type.RELATES).to().forEachRemaining(v -> v.scope(label));
    }

    @Override
    public void isAbstract(boolean isAbstract) {
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
        TypeVertex vertex = super.superTypeVertex();
        return vertex != null ? of(vertex) : null;
    }

    @Override
    public Stream<RelationTypeImpl> sups() {
        Iterator<RelationTypeImpl> sups = Iterators.apply(super.superTypeVertices(), RelationTypeImpl::of);
        return stream(spliteratorUnknownSize(sups, ORDERED | IMMUTABLE), false);
    }

    @Override
    public Stream<RelationTypeImpl> subs() {
        Iterator<RelationTypeImpl> subs = Iterators.apply(super.subTypeVertices(), RelationTypeImpl::of);
        return stream(spliteratorUnknownSize(subs, ORDERED | IMMUTABLE), false);
    }

    @Override
    public void relates(String roleLabel) {
        TypeVertex roleTypeVertex = vertex.graph().get(roleLabel, vertex.label());
        if (roleTypeVertex == null) {
            if (sups().flatMap(RelationType::roles).anyMatch(role -> role.label().equals(roleLabel))) {
                throw new HypergraphException("Invalid RoleType Assignment: role type already exists in a supertype relation");
            } else {
                RoleTypeImpl roleType = RoleTypeImpl.of(vertex.graph(), roleLabel, vertex.label());
                vertex.outs().put(Schema.Edge.Type.RELATES, roleType.vertex);
                vertex.outs().edge(Schema.Edge.Type.RELATES, roleType.vertex).overridden(roleType.sup().vertex);
            }
        }
    }

    @Override
    public void relates(String roleLabel, String overriddenLabel) {
        this.relates(roleLabel);
        RoleTypeImpl roleType = role(roleLabel);

        Optional<RoleTypeImpl> inherited;
        if (declaredRoles().anyMatch(r -> r.label().equals(overriddenLabel)) ||
                !(inherited = sup().roles().filter(role -> role.label().equals(overriddenLabel)).findAny()).isPresent()) {
            throw new HypergraphException("Invalid Role Type Overriding: " + overriddenLabel + " cannot be overridden");
        }

        roleType.sup(inherited.get());
        vertex.outs().edge(Schema.Edge.Type.RELATES, roleType.vertex).overridden(inherited.get().vertex);
    }

    @Override
    public void unrelate(String roleLabel) {
        TypeVertex roleTypeVertex = vertex.graph().get(roleLabel, vertex.label());
        if (roleTypeVertex != null) {
            RoleTypeImpl.of(roleTypeVertex).delete();
        } else {
            throw new HypergraphException("Invalid RoleType Removal: " + roleLabel + " does not exist in " + vertex.label());
        }
    }

    @Override
    public Stream<RoleTypeImpl> roles() {
        Iterator<RoleTypeImpl> roles = apply(vertex.outs().edge(Schema.Edge.Type.RELATES).to(), RoleTypeImpl::of);
        if (isRoot()) {
            return stream(spliteratorUnknownSize(roles, ORDERED | IMMUTABLE), false);
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
        Iterator<RoleTypeImpl> roles = apply(vertex.outs().edge(Schema.Edge.Type.RELATES).to(), RoleTypeImpl::of);
        return stream(spliteratorUnknownSize(roles, ORDERED | IMMUTABLE), false);
    }

    @Override
    public RoleTypeImpl role(String roleLabel) {
        Optional<RoleTypeImpl> roleType;
        TypeVertex roleTypeVertex = vertex.graph().get(roleLabel, vertex.label());
        if (roleTypeVertex != null) {
            return RoleTypeImpl.of(roleTypeVertex);
        } else if ((roleType = roles().filter(role -> role.label().equals(roleLabel)).findAny()).isPresent()) {
            return roleType.get();
        } else return null;
    }

    @Override
    public void delete() {
        if (compareSize(subs(), 1) == 0) {
            declaredRoles().forEach(Type::delete);
            vertex.delete();
        } else {
            throw new HypergraphException("Invalid RoleType Removal: " + label() + " has subtypes");
        }
    }

    public static class Root extends RelationTypeImpl {

        Root(TypeVertex vertex) {
            super(vertex);
            assert vertex.label().equals(Schema.Vertex.Type.Root.RELATION.label());
        }

        @Override
        public boolean isRoot() { return true; }

        @Override
        public void label(String label) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

        @Override
        public void isAbstract(boolean isAbstract) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

        @Override
        public void sup(RelationType superType) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }
    }
}
