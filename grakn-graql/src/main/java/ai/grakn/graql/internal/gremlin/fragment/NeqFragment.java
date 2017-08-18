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

import ai.grakn.GraknTx;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarProperty;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

class NeqFragment extends AbstractFragment {

    private final Var other;

    NeqFragment(VarProperty varProperty, Var start, Var other) {
        super(varProperty, start);
        this.other = other;
    }

    @Override
    public GraphTraversal<Element, ? extends Element> applyTraversal(
            GraphTraversal<Element, ? extends Element> traversal, GraknTx graph) {
        return traversal.where(P.neq(other.getValue()));
    }

    @Override
    public String getName() {
        return "[neq:" + other.shortName() + "]";
    }

    @Override
    public double fragmentCost() {
        // This is arbitrary - we imagine about half the results are filtered out
        return COST_NEQ;
    }

    @Override
    public ImmutableSet<Var> getDependencies() {
        return ImmutableSet.of(other);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        NeqFragment that = (NeqFragment) o;

        return other.equals(that.other);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + other.hashCode();
        return result;
    }
}
