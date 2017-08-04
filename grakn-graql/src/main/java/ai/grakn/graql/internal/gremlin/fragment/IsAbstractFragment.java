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

import ai.grakn.GraknGraph;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

class IsAbstractFragment extends AbstractFragment {

    IsAbstractFragment(VarProperty varProperty, Var start) {
        super(varProperty, start);
    }

    @Override
    public GraphTraversal<Element, ? extends Element> applyTraversal(
            GraphTraversal<Element, ? extends Element> traversal, GraknGraph graph) {

        return traversal.has(Schema.VertexProperty.IS_ABSTRACT.name(), true);
    }

    @Override
    public String getName() {
        return "[is-abstract]";
    }

    @Override
    public double fragmentCost() {
        return COST_SAME_AS_PREVIOUS;
    }
}
