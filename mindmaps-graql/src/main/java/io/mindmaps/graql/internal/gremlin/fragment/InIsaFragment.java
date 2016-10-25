package io.mindmaps.graql.internal.gremlin.fragment;

import io.mindmaps.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static io.mindmaps.graql.internal.gremlin.Traversals.inSubs;
import static io.mindmaps.util.Schema.EdgeLabel.ISA;

class InIsaFragment extends AbstractFragment {

    InIsaFragment(String start, String end) {
        super(start, end);
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        inSubs(inSubs(traversal).in(ISA.getLabel()));
    }

    @Override
    public FragmentPriority getPriority() {
        return FragmentPriority.EDGE_UNBOUNDED;
    }

}
