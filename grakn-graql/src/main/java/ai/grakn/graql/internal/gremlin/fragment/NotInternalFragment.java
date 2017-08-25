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
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Optional;

class NotInternalFragment extends Fragment {

    private final Var start;
    private final Optional<Var> end = Optional.empty();
    private final ImmutableSet<Var> otherVarNames = ImmutableSet.of();
    private VarProperty varProperty; // For reasoner to map fragments to atoms

    NotInternalFragment(VarProperty varProperty, Var start) {
        super();
        this.varProperty = varProperty;
        this.start = start;
    }

    @Override
    public GraphTraversal<Element, ? extends Element> applyTraversal(
            GraphTraversal<Element, ? extends Element> traversal, GraknTx graph) {
        return traversal.not(__.hasLabel(Schema.BaseType.SHARD.name()));
    }

    @Override
    public String getName() {
        return "[not-internal]";
    }

    @Override
    public boolean isStartingFragment() {
        return true;
    }

    @Override
    public double fragmentCost() {
        return COST_SAME_AS_PREVIOUS;
    }

    /**
     * Get the corresponding property
     */
    public VarProperty getVarProperty() {
        return varProperty;
    }

    /**
     * @return the variable name that this fragment starts from in the query
     */
    @Override
    public final Var getStart() {
        return start;
    }

    /**
     * @return the variable name that this fragment ends at in the query, if this query has an end variable
     */
    @Override
    public final Optional<Var> getEnd() {
        return end;
    }

    @Override
    ImmutableSet<Var> otherVarNames() {
        return otherVarNames;
    }
}
