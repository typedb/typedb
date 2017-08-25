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
import ai.grakn.graql.internal.gremlin.spanningtree.graph.DirectedEdge;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.Node;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.NodeId;
import ai.grakn.graql.internal.gremlin.spanningtree.util.Weighted;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static ai.grakn.util.Schema.EdgeLabel.PLAYS;

class InPlaysFragment extends Fragment {

    private final boolean required;
    private final Var start;
    private final Optional<Var> end;
    private final ImmutableSet<Var> otherVarNames = ImmutableSet.of();
    private VarProperty varProperty; // For reasoner to map fragments to atoms

    InPlaysFragment(VarProperty varProperty, Var start, Var end, boolean required) {
        super();
        this.varProperty = varProperty;
        this.start = start;
        this.end = Optional.of(end);
        this.required = required;
    }

    @Override
    public GraphTraversal<Element, ? extends Element> applyTraversal(
            GraphTraversal<Element, ? extends Element> traversal, GraknTx graph) {
        GraphTraversal<Element, Vertex> vertexTraversal = Fragments.isVertex(traversal);
        if (required) {
            vertexTraversal.inE(PLAYS.getLabel()).has(Schema.EdgeProperty.REQUIRED.name()).otherV();
        } else {
            vertexTraversal.in(PLAYS.getLabel());
        }

        return Fragments.inSubs(vertexTraversal);
    }

    @Override
    public String getName() {
        if (required) {
            return "<-[plays:required]-";
        } else {
            return "<-[plays]-";
        }
    }

    @Override
    public double fragmentCost() {
        return COST_TYPES_PER_ROLE;
    }

    @Override
    public Set<Weighted<DirectedEdge<Node>>> getDirectedEdges(Map<NodeId, Node> nodes,
                                                              Map<Node, Map<Node, Fragment>> edges) {
        return getDirectedEdges(NodeId.NodeType.PLAYS, nodes, edges);
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
