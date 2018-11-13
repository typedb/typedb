/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.graql.internal.gremlin.fragment;

import grakn.core.graql.ValuePredicate;
import grakn.core.graql.Var;
import grakn.core.graql.admin.VarPatternAdmin;
import grakn.core.server.kb.internal.TransactionImpl;
import com.google.auto.value.AutoValue;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.Set;

import static grakn.core.util.CommonUtil.optionalToStream;
import static java.util.stream.Collectors.toSet;

@AutoValue
abstract class ValueFragment extends Fragment {

    abstract ValuePredicate predicate();

    @Override
    public GraphTraversal<Vertex, ? extends Element> applyTraversalInner(
            GraphTraversal<Vertex, ? extends Element> traversal, TransactionImpl<?> graph, Collection<Var> vars) {

        return predicate().applyPredicate(traversal);
    }

    @Override
    public String name() {
        return "[value:" + predicate() + "]";
    }

    @Override
    public double internalFragmentCost() {
        if (predicate().isSpecific()) {
            return COST_NODE_INDEX_VALUE;
        } else {
            // Assume approximately half of values will satisfy a filter
            return COST_NODE_UNSPECIFIC_PREDICATE;
        }
    }

    @Override
    public boolean hasFixedFragmentCost() {
        return predicate().isSpecific() && dependencies().isEmpty();
    }

    @Override
    public Set<Var> dependencies() {
        return optionalToStream(predicate().getInnerVar()).map(VarPatternAdmin::var).collect(toSet());
    }
}
