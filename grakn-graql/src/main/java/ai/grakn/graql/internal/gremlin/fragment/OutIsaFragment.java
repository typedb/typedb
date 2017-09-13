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
import ai.grakn.graql.internal.gremlin.spanningtree.graph.DirectedEdge;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.Node;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.NodeId;
import ai.grakn.graql.internal.gremlin.spanningtree.util.Weighted;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static ai.grakn.util.Schema.EdgeLabel.ISA;
import static ai.grakn.util.Schema.EdgeLabel.SHARD;
import static ai.grakn.util.Schema.EdgeProperty.RELATIONSHIP_TYPE_LABEL_ID;

@AutoValue
abstract class OutIsaFragment extends Fragment {

    @Override
    public abstract Var end();

    @Override
    public GraphTraversal<Element, ? extends Element> applyTraversalInner(
            GraphTraversal<Element, ? extends Element> traversal, GraknTx graph, Collection<Var> vars) {

        GraphTraversal<Element, Vertex> vertexTraversal = Fragments.union(traversal, ImmutableSet.of(
                Fragments.isVertex(__.identity()).out(ISA.getLabel()).out(SHARD.getLabel()),
                edgeTraversal()
        ));

        return Fragments.outSubs(vertexTraversal);
    }

    private GraphTraversal<Element, Vertex> edgeTraversal() {
        return Fragments.traverseSchemaConceptFromEdge(Fragments.isEdge(__.identity()), RELATIONSHIP_TYPE_LABEL_ID);
    }

    @Override
    public String name() {
        return "-[isa]->";
    }

    @Override
    public double fragmentCost() {
        return COST_SAME_AS_PREVIOUS;
    }

    @Override
    public Set<Weighted<DirectedEdge<Node>>> directedEdges(Map<NodeId, Node> nodes,
                                                           Map<Node, Map<Node, Fragment>> edges) {
        return directedEdges(NodeId.NodeType.ISA, nodes, edges);
    }

    @Override
    public boolean canOperateOnEdges() {
        return true;
    }
}
