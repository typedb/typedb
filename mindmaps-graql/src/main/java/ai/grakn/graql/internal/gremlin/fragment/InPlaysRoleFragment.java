/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs
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

import ai.grakn.graql.internal.gremlin.FragmentPriority;
import ai.grakn.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static ai.grakn.graql.internal.gremlin.fragment.Fragments.inSubs;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.outSubs;
import static ai.grakn.util.Schema.EdgeLabel.PLAYS_ROLE;

class InPlaysRoleFragment extends AbstractFragment {

    InPlaysRoleFragment(String start, String end) {
        super(start, end);
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        Fragments.inSubs(Fragments.outSubs(traversal).in(PLAYS_ROLE.getLabel()));
    }

    @Override
    public String getName() {
        return "<-[plays-role]-";
    }

    @Override
    public FragmentPriority getPriority() {
        return FragmentPriority.EDGE_BOUNDED;
    }

    @Override
    public long fragmentCost(long previousCost) {
        return previousCost * NUM_TYPES_PER_ROLE;
    }

}
