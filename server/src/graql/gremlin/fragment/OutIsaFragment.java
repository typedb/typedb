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
import com.google.common.collect.ImmutableSet;
import grakn.core.graql.gremlin.spanningtree.graph.DirectedEdge;
import grakn.core.graql.gremlin.spanningtree.graph.Node;
import grakn.core.graql.gremlin.spanningtree.graph.NodeId;
import grakn.core.graql.gremlin.spanningtree.util.Weighted;
import grakn.core.graql.reasoner.utils.Pair;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.statement.Variable;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static grakn.core.server.kb.Schema.EdgeLabel.ISA;
import static grakn.core.server.kb.Schema.EdgeLabel.SHARD;
import static grakn.core.server.kb.Schema.EdgeProperty.RELATION_TYPE_LABEL_ID;

/**
 * A fragment representing traversing an isa edge from instance to type.
 *
 */

@AutoValue
public abstract class OutIsaFragment extends Fragment {

    @Override
    public abstract Variable end();

    @Override
    public GraphTraversal<Vertex, ? extends Element> applyTraversalInner(
            GraphTraversal<Vertex, ? extends Element> traversal, TransactionOLTP graph, Collection<Variable> vars) {

        // from the traversal, branch to take either of these paths
        return Fragments.union(traversal, ImmutableSet.of(
                Fragments.isVertex(__.identity()).out(ISA.getLabel()).out(SHARD.getLabel()),
                edgeTraversal() // what is this doing?
        ));
    }

    private GraphTraversal<Element, Vertex> edgeTraversal() {
        return Fragments.traverseSchemaConceptFromEdge(Fragments.isEdge(__.identity()), RELATION_TYPE_LABEL_ID);
    }

    @Override
    public String name() {
        return "-[isa]->";
    }

    @Override
    public double internalFragmentCost() {
        return COST_SAME_AS_PREVIOUS;
    }

    @Override
    public Set<Node> getNodes() {
        Node start = new Node(NodeId.of(NodeId.NodeType.VAR, start()));
        Node end = new Node(NodeId.of(NodeId.NodeType.VAR, end()));
        Node middle = new Node(NodeId.of(NodeId.NodeType.ISA, new HashSet<>(Arrays.asList(start(), end()))));
        middle.setInvalidStartingPoint();
        return new HashSet<>(Arrays.asList(start, end, middle));
    }

    @Override
    public Pair<Node, Node> getMiddleNodeDirectedEdge(Map<NodeId, Node> nodes) {
        Node start = nodes.get(NodeId.of(NodeId.NodeType.VAR, start()));
        Node middle = new Node(NodeId.of(NodeId.NodeType.ISA, new HashSet<>(Arrays.asList(start(), end()))));
        // directed edge: middle -> start
        return new Pair<>(middle, start);
    }

    @Override
    public Set<Weighted<DirectedEdge>> directedEdges(Map<NodeId, Node> nodes) {
        return directedEdges(NodeId.NodeType.ISA, nodes);
    }

    @Override
    public boolean canOperateOnEdges() {
        return true;
    }
}
