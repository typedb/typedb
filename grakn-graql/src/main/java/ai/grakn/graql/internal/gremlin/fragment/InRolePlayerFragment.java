/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.graql.VarName;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

class InRolePlayerFragment extends AbstractFragment {

    InRolePlayerFragment(VarName start, VarName end) {
        super(start, end);
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        traversal.in(Schema.EdgeLabel.ROLE_PLAYER.getLabel());
    }

    @Override
    public String getName() {
        return "<-[role-player]-";
    }

    @Override
    public double fragmentCost(double previousCost) {
        return previousCost * NUM_CASTINGS_PER_INSTANCE;
    }
}
