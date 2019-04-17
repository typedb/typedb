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

import com.google.common.collect.Sets;
import grakn.core.graql.gremlin.spanningtree.graph.DirectedEdge;
import grakn.core.graql.gremlin.spanningtree.graph.Node;
import grakn.core.graql.gremlin.spanningtree.graph.NodeId;
import grakn.core.graql.gremlin.spanningtree.util.Weighted;
import grakn.core.graql.reasoner.utils.Pair;

import java.util.Map;
import java.util.Set;

import static grakn.core.graql.gremlin.spanningtree.util.Weighted.weighted;

/**
 * Fragments that represent JanusGraph edges have some different properties when it comes to query planning
 * Most importantly, they create a virtual middle node in the Query Planner. Each subtype of EdgeFragment
 * has it's own definiton of the middle node's ID. However, they all have the same implementation of
 * methods to do with building the Query Planning Graph
 */
public abstract class EdgeFragment extends Fragment {

    abstract NodeId getMiddleNodeId();

    public Set<Node> getNodes() {
        Node start = new Node(NodeId.of(NodeId.NodeType.VAR, start()));
        Node end = new Node(NodeId.of(NodeId.NodeType.VAR, end()));
        Node middle = new Node(getMiddleNodeId());
        middle.setInvalidStartingPoint();
        return Sets.newHashSet(start, end, middle);
    }

    @Override
    public Pair<Node, Node> getMiddleNodeDirectedEdge(Map<NodeId, Node> nodes) {
        Node start = nodes.get(NodeId.of(NodeId.NodeType.VAR, start()));
        Node middle = nodes.get(getMiddleNodeId());
        // directed edge: middle -> start
        return new Pair<>(middle, start);
    }

    @Override
    public final Set<Weighted<DirectedEdge>> directedEdges(Map<NodeId, Node> nodes) {

        // this call to `directedEdges` handles converting janus edges that the user cannot address
        // (ie. not role edges), into edges with a middle node to force the query planner to traverse to this middle
        // node that represents the actual Janus edge
        // since the middle node cannot be addressed it does not have a variable, so we create a new ID for it
        // as the combination of start() and end() with the type

        Node start = nodes.get(NodeId.of(NodeId.NodeType.VAR, start()));
        Node end = nodes.get(NodeId.of(NodeId.NodeType.VAR, end()));
        Node middle = nodes.get(getMiddleNodeId());

        return Sets.newHashSet(
                weighted(DirectedEdge.from(start).to(middle), -fragmentCost()),
                weighted(DirectedEdge.from(middle).to(end), 0));
    }
}
