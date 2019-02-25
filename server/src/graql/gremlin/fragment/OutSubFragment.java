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

package grakn.core.graql.gremlin.fragment;

import com.google.auto.value.AutoValue;
import grakn.core.graql.gremlin.spanningtree.graph.DirectedEdge;
import grakn.core.graql.gremlin.spanningtree.graph.Node;
import grakn.core.graql.gremlin.spanningtree.graph.NodeId;
import grakn.core.graql.gremlin.spanningtree.util.Weighted;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.statement.Variable;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Fragment for following out sub edges, potentially limited to some number of `sub` edges
 *
 */

@AutoValue
public abstract class OutSubFragment extends Fragment {
    @Override
    public abstract Variable end();

    // -1 implies no depth limit
    public abstract int subTraversalDepthLimit();

    @Override
    public GraphTraversal<Vertex, ? extends Element> applyTraversalInner(
            GraphTraversal<Vertex, ? extends Element> traversal, TransactionOLTP graph, Collection<Variable> vars) {
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
    public Set<Weighted<DirectedEdge<Node>>> directedEdges(Map<NodeId, Node> nodes,
                                                           Map<Node, Map<Node, Fragment>> edges) {
        return directedEdges(NodeId.NodeType.SUB, nodes, edges);
    }
}
