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

import static ai.grakn.util.Schema.EdgeLabel.PLAYS;

class InPlaysFragment extends AbstractFragment {

    private final boolean required;

    InPlaysFragment(VarName start, VarName end, boolean required) {
        super(start, end);
        this.required = required;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        if (required) {
            traversal.inE(PLAYS.getLabel()).has(Schema.EdgeProperty.REQUIRED.name()).otherV();
        } else {
            traversal.in(PLAYS.getLabel());
        }

        Fragments.inSubs(traversal);
    }

    @Override
    public String getName() {
        if (required) {
            return "<-[plays:required]-";
        } else {
            return "<-[plays]-";
        }
    }

    @Override
    public double fragmentCost(double previousCost) {
        return previousCost * NUM_TYPES_PER_ROLE;
    }

}
