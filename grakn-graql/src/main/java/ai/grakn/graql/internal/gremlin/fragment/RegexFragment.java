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
import ai.grakn.util.StringUtil;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

import static ai.grakn.util.Schema.VertexProperty.REGEX;

class RegexFragment extends AbstractFragment {

    private final String regex;

    RegexFragment(VarProperty varProperty, Var start, String regex) {
        super(varProperty, start);
        this.regex = regex;
    }

    @Override
    public GraphTraversal<Element, ? extends Element> applyTraversal(
            GraphTraversal<Element, ? extends Element> traversal, GraknGraph graph) {

        return traversal.has(REGEX.name(), regex);
    }

    @Override
    public String getName() {
        return "[regex:" + StringUtil.valueToString(regex) + "]";
    }

    @Override
    public double fragmentCost() {
        return COST_SAME_AS_PREVIOUS;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        RegexFragment that = (RegexFragment) o;

        return regex != null ? regex.equals(that.regex) : that.regex == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (regex != null ? regex.hashCode() : 0);
        return result;
    }
}
