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
import grakn.core.kb.graql.planning.gremlin.Fragment;
import grakn.core.kb.graql.planning.spanningtree.graph.Node;
import grakn.core.kb.graql.planning.spanningtree.graph.NodeId;
import grakn.core.kb.graql.planning.spanningtree.graph.SchemaNode;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;

/**
 * Fragment for following out sub edges, potentially limited to some number of `sub` edges
 *
 */

public class OutSubFragment extends EdgeFragment {
    private final int subTraversalDepthLimit;

    OutSubFragment(
            @Nullable VarProperty varProperty,
            Variable start,
            Variable end,
            int subTraversalDepthLimit) {
        super(varProperty, start, end);
        this.subTraversalDepthLimit = subTraversalDepthLimit;
    }

    // -1 implies no depth limit
    private int subTraversalDepthLimit() {
        return subTraversalDepthLimit;
    }

    @Override
    public GraphTraversal<Vertex, ? extends Element> applyTraversalInner(
            GraphTraversal<Vertex, ? extends Element> traversal, ConceptManager conceptManager, Collection<Variable> vars) {
        return Fragments.outSubs(Fragments.isVertex(traversal), this.subTraversalDepthLimit());
    }

    @Override
    public String name() {
        if (subTraversalDepthLimit() == Fragments.TRAVERSE_ALL_SUB_EDGES) {
            return "-[sub]->";
        } else {
            return "-[sub!" + Integer.toString(subTraversalDepthLimit()) + "]->";
        }
    }

    @Override
    public Fragment getInverse() {
        // TODO figure out the inverse with depth limit correctly
        return Fragments.inSub(varProperty(), end(), start(), this.subTraversalDepthLimit());
    }

    @Override
    public double internalFragmentCost() {
        return COST_SAME_AS_PREVIOUS;
    }

    @Override
    protected Node startNode() {
        return new SchemaNode(NodeId.of(NodeId.Type.VAR, start()));
    }

    @Override
    protected Node endNode() {
        return new SchemaNode(NodeId.of(NodeId.Type.VAR, end()));
    }

    @Override
    protected NodeId getMiddleNodeId() {
        return NodeId.of(NodeId.Type.SUB, new HashSet<>(Arrays.asList(start(), end())));
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof OutSubFragment) {
            OutSubFragment that = (OutSubFragment) o;
            return ((this.varProperty == null) ? (that.varProperty() == null) : this.varProperty.equals(that.varProperty()))
                    && (this.start.equals(that.start()))
                    && (this.end.equals(that.end()))
                    && (this.subTraversalDepthLimit == that.subTraversalDepthLimit());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(varProperty, start, end, subTraversalDepthLimit);
    }
}
