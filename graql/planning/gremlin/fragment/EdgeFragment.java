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

import com.google.common.collect.Sets;
import grakn.common.util.Pair;
import grakn.core.kb.graql.planning.spanningtree.graph.DirectedEdge;
import grakn.core.kb.graql.planning.spanningtree.graph.EdgeNode;
import grakn.core.kb.graql.planning.spanningtree.graph.Node;
import grakn.core.kb.graql.planning.spanningtree.graph.NodeId;
import grakn.core.kb.graql.planning.spanningtree.util.Weighted;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

import static grakn.common.util.Collections.pair;
import static grakn.core.kb.graql.planning.spanningtree.util.Weighted.weighted;

/**
 * Fragments that represent JanusGraph edges have some different properties when it comes to query planning
 * Most importantly, they create a virtual middle node in the Query Planner. Each subtype of EdgeFragment
 * has it's own definiton of the middle node's ID. However, they all have the same implementation of
 * methods to do with building the Query Planning Graph
 */
public abstract class EdgeFragment extends FragmentImpl {

    protected Variable end;

    abstract protected NodeId getMiddleNodeId();
    abstract protected Node startNode();
    abstract protected Node endNode();

    EdgeFragment(@Nullable VarProperty varProperty, Variable start, Variable end) {
        super(varProperty, start);
        this.end = end;
    }

    public Variable end() {
        return end;
    }

    @Override
    public Set<Node> getNodes() {
        Node start = startNode();
        Node end = endNode();
        Node middle = new EdgeNode(getMiddleNodeId());
        middle.setInvalidStartingPoint();
        return Sets.newHashSet(start, end, middle);
    }

    @Override
    public Pair<Node, Node> getMiddleNodeDirectedEdge(Map<NodeId, Node> nodes) {
        Node start = nodes.get(NodeId.of(NodeId.Type.VAR, start()));
        Node middle = nodes.get(getMiddleNodeId());
        // directed edge: middle -> start
        return pair(middle, start);
    }

    @Override
    public final Set<Weighted<DirectedEdge>> directedEdges(Map<NodeId, Node> nodes) {

        // this call to `directedEdges` handles converting janus edges that the user cannot address
        // (ie. not role edges), into edges with a middle node to force the query planner to traverse to this middle
        // node that represents the actual Janus edge
        // since the middle node cannot be addressed it does not have a variable, so we create a new ID for it
        // as the combination of start() and end() with the type

        Node start = nodes.get(NodeId.of(NodeId.Type.VAR, start()));
        Node end = nodes.get(NodeId.of(NodeId.Type.VAR, end()));
        Node middle = nodes.get(getMiddleNodeId());

        return Sets.newHashSet(
                weighted(DirectedEdge.from(start).to(middle), -fragmentCost()),
                weighted(DirectedEdge.from(middle).to(end), 0));
    }
}
