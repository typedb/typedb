/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.graql.planning.gremlin.fragment;

import grakn.core.kb.concept.manager.ConceptManager;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;
import graql.lang.util.StringUtil;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;

import static grakn.core.core.Schema.VertexProperty.REGEX;

class RegexFragment extends FragmentImpl {
    private final String regex;

    RegexFragment(@Nullable VarProperty varProperty, Variable start, String regex) {
        super(varProperty, start);
        this.regex = regex;
    }

    private String regex() {
        return regex;
    }

    @Override
    public GraphTraversal<Vertex, ? extends Element> applyTraversalInner(
            GraphTraversal<Vertex, ? extends Element> traversal, ConceptManager conceptManager, Collection<Variable> vars) {

        return traversal.has(REGEX.name(), regex());
    }

    @Override
    public String name() {
        return "[regex:" + StringUtil.valueToString(regex()) + "]";
    }

    @Override
    public double internalFragmentCost() {
        return COST_NODE_REGEX;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof RegexFragment) {
            RegexFragment that = (RegexFragment) o;
            return ((this.varProperty == null) ? (that.varProperty() == null) : this.varProperty.equals(that.varProperty()))
                    && (this.start.equals(that.start()))
                    && (this.regex.equals(that.regex()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(varProperty, start, regex);
    }
}
