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
 *
 */

package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.GraknTx;
import ai.grakn.concept.Label;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

import static ai.grakn.util.Schema.VertexProperty.INDEX;

class ResourceIndexFragment extends AbstractFragment {

    private final String resourceIndex;

    ResourceIndexFragment(VarProperty varProperty, Var start, Label label, Object value) {
        super(varProperty, start);
        this.resourceIndex = Schema.generateResourceIndex(label, value.toString());
    }

    @Override
    public GraphTraversal<Element, ? extends Element> applyTraversal(
            GraphTraversal<Element, ? extends Element> traversal, GraknTx graph) {

        return traversal.has(INDEX.name(), resourceIndex);
    }

    @Override
    public String getName() {
        return "[index:" + resourceIndex + "]";
    }

    @Override
    public double fragmentCost() {
        return COST_INDEX;
    }

    @Override
    public boolean hasFixedFragmentCost() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ResourceIndexFragment that = (ResourceIndexFragment) o;

        return resourceIndex.equals(that.resourceIndex);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + resourceIndex.hashCode();
        return result;
    }
}
