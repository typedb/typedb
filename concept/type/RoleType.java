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

public class RoleType extends Type<RoleType> {

    public static RoleType of(TypeVertex vertex) {
        if (vertex.label().equals(Schema.Vertex.Type.Root.ROLE.label())) return new RoleType.Root(vertex);
        else return new RoleType(vertex);
    }

    public static RoleType of(Graph graph, String label) {
        return new RoleType(graph, label);
    }

    private RoleType(TypeVertex vertex) {
        super(vertex);
        assert(vertex.schema() == Schema.Vertex.Type.ROLE_TYPE);
    }

    private RoleType(Graph graph, String label) {
        super(graph, label, Schema.Vertex.Type.ROLE_TYPE);
    }

    @Override
    RoleType newInstance(TypeVertex vertex) {
        return new RoleType(vertex);
    }

    @Override
    RoleType getThis() {
        return this;
    }

    public static class Root extends RoleType {

        Root(TypeVertex vertex) {
            super(vertex);
            assert(vertex.label().equals(Schema.Vertex.Type.Root.ROLE.label()));
        }

        @Override
        public RoleType label(String label) {
            throw new HypergraphException("Invalid Operation Exception: root types are immutable");
        }

        @Override
        public RoleType setAbstract(boolean isAbstract) {
            throw new HypergraphException("Invalid Operation Exception: root types are immutable");
        }

        @Override
        public RoleType sup() {
            return null;
        }

        @Override
        public RoleType sup(RoleType superType) {
            throw new HypergraphException("Invalid Operation Exception: root types are immutable");
        }
    }
}
