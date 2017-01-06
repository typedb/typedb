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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

class InIsaFragment extends AbstractFragment {

    private final boolean allowCastings;

    InIsaFragment(VarName start, VarName end, boolean allowCastings) {
        super(start, end);
        this.allowCastings = allowCastings;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        Fragments.inSubs(traversal).in(Schema.EdgeLabel.ISA.getLabel());
        if (!allowCastings) {
            // Make sure we never get any castings
            traversal.not(__.hasLabel(Schema.BaseType.CASTING.name()));
        }
    }

    @Override
    public String getName() {
        return "<-[isa" + (allowCastings ? ":allow-castings" : "") + "]-";
    }

    @Override
    public double fragmentCost(double previousCost) {
        return previousCost * NUM_INSTANCES_PER_TYPE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        InIsaFragment that = (InIsaFragment) o;

        return allowCastings == that.allowCastings;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (allowCastings ? 1 : 0);
        return result;
    }
}
