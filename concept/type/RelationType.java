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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.StreamSupport.stream;

public class RelationType extends ThingType<RelationType> {

    public static RelationType of(TypeVertex vertex) {
        if (vertex.label().equals(Schema.Vertex.Type.Root.RELATION.label())) return new RelationType.Root(vertex);
        else return new RelationType(vertex);
    }

    public static RelationType of(Graph.Type graph, String label) {
        return new RelationType(graph, label);
    }

    private RelationType(TypeVertex vertex) {
        super(vertex);
        if (vertex.schema() != Schema.Vertex.Type.RELATION_TYPE) {
            throw new HypergraphException("Invalid Relation Type: " + vertex.label() +
                                                  " subtypes " + vertex.schema().root().label());
        }
    }

    private RelationType(Graph.Type graph, String label) {
        super(graph, label, Schema.Vertex.Type.RELATION_TYPE);
    }

    @Override
    RelationType newInstance(TypeVertex vertex) {
        return of(vertex);
    }

    @Override
    public void label(String label) {
        vertex.label(label);
        vertex.outs().edge(Schema.Edge.Type.RELATES).to().forEachRemaining(v -> v.scope(label));
    }

    @Override
    public void setAbstract(boolean isAbstract) {
        vertex.setAbstract(isAbstract);
        vertex.outs().edge(Schema.Edge.Type.RELATES).to().forEachRemaining(v -> v.setAbstract(isAbstract));
    }

    public RelationType.Builder relates(String roleLabel) {
        TypeVertex roleTypeVertex = vertex.graph().get(roleLabel, vertex.label());
        if (roleTypeVertex != null) {
            return new Builder(RoleType.of(roleTypeVertex));
        } else {
            RoleType roleType = RoleType.of(vertex.graph(), roleLabel, vertex.label());
            vertex.outs().put(Schema.Edge.Type.RELATES, roleType.vertex);
            return new Builder(roleType);
        }
    }

    public Stream<RoleType> roles() {
        Iterator<RoleType> iterator = Iterators.apply(vertex.outs().edge(Schema.Edge.Type.RELATES).to(), RoleType::of);
        if (sup() == null) {
            return stream(spliteratorUnknownSize(iterator, ORDERED | IMMUTABLE), false);
        } else {
            Set<RoleType> direct = new HashSet<>();
            iterator.forEachRemaining(direct::add);
            Set<RoleType> overridden = direct.stream().map(Type::sup).filter(Objects::nonNull).collect(toSet());
            return Stream.concat(direct.stream(), sup().roles().filter(r -> !overridden.contains(r)));
        }
    }

    public RoleType role(String roleLabel) {
        TypeVertex roleTypeVertex = vertex.graph().get(roleLabel, vertex.label());
        if (roleTypeVertex != null) return RoleType.of(roleTypeVertex);
        else return null;
    }

    public class Builder {

        private final RoleType roleType;

        Builder(RoleType roleType) {
            this.roleType = roleType;
        }

        public void as(String roleLabel) {
            Optional<RoleType> inherited = sup().roles().filter(role -> role.label().equals(roleLabel)).findFirst();
            if (inherited.isPresent()) {
                roleType.sup(inherited.get());
            } else {
                throw new HypergraphException("Invalid Role Type Overriding: inherited roles does not contain " + roleLabel);
            }
        }

    }

    public static class Root extends RelationType {

        Root(TypeVertex vertex) {
            super(vertex);
            assert vertex.label().equals(Schema.Vertex.Type.Root.RELATION.label());
        }

        @Override
        public void label(String label) {
            throw new HypergraphException("Invalid Operation Exception: root types are immutable");
        }

        @Override
        public void setAbstract(boolean isAbstract) {
            throw new HypergraphException("Invalid Operation Exception: root types are immutable");
        }

        @Override
        public RelationType sup() {
            return null;
        }

        @Override
        public void sup(RelationType superType) {
            throw new HypergraphException("Invalid Operation Exception: root types are immutable");
        }
    }
}
