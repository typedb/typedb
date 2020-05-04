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
import hypergraph.graph.Graph;
import hypergraph.graph.Schema;
import hypergraph.graph.vertex.TypeVertex;

import static hypergraph.common.exception.Error.TypeDefinition.INVALID_ROOT_TYPE_MUTATION;

public class RoleType extends Type<RoleType> {

    private RoleType(TypeVertex vertex) {
        super(vertex);
        assert vertex.schema() == Schema.Vertex.Type.ROLE_TYPE;
        if (vertex.schema() != Schema.Vertex.Type.ROLE_TYPE) {
            throw new HypergraphException("Invalid Role Type: " + vertex.label() +
                                                  " subtypes " + vertex.schema().root().label());
        }
    }

    private RoleType(Graph.Type graph, String label, String relation) {
        super(graph, label, Schema.Vertex.Type.ROLE_TYPE, relation);
    }

    public static RoleType of(TypeVertex vertex) {
        if (vertex.label().equals(Schema.Vertex.Type.Root.ROLE.label())) return new RoleType.Root(vertex);
        else return new RoleType(vertex);
    }

    public static RoleType of(Graph.Type graph, String label, String relation) {
        return new RoleType(graph, label, relation);
    }

    @Override
    RoleType getThis() { return this; }

    @Override
    RoleType newInstance(TypeVertex vertex) {
        return of(vertex);
    }

    public String scopedLabel() {
        return vertex.scopedLabel();
    }

    public static class Root extends RoleType {

        Root(TypeVertex vertex) {
            super(vertex);
            assert vertex.label().equals(Schema.Vertex.Type.Root.ROLE.label());
        }

        @Override
        public boolean isRoot() { return true; }

        @Override
        public void label(String label) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

        @Override
        protected void isAbstract(boolean isAbstract) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }

        @Override
        public RoleType sup() { return null; }

        @Override
        protected void sup(RoleType superType) { throw new HypergraphException(INVALID_ROOT_TYPE_MUTATION); }
    }
}
