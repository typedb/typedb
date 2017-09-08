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
import ai.grakn.util.Schema;
import com.google.auto.value.AutoValue;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Collection;

@AutoValue
abstract class NotInternalFragment extends Fragment {

    @Override
    public GraphTraversal<Element, ? extends Element> applyTraversalInner(
            GraphTraversal<Element, ? extends Element> traversal, GraknTx graph, Collection<Var> vars) {
        return traversal.not(__.hasLabel(Schema.BaseType.SHARD.name()));
    }

    @Override
    public String name() {
        return "[not-internal]";
    }

    @Override
    public boolean isStartingFragment() {
        return true;
    }

    @Override
    public double fragmentCost() {
        return COST_NODE_NOT_INTERNAL;
    }
}
