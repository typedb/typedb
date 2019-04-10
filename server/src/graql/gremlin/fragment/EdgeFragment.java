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
 * has it's own definiton of the middle node's ID, which is a required
 */
public abstract class EdgeFragment extends Fragment {

    abstract NodeId getMiddleNodeId();

    public Set<Node> getNodes() {
        Node start = new Node(NodeId.of(NodeId.NodeType.VAR, start()));
        Node end = new Node(getMiddleNodeId());
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
