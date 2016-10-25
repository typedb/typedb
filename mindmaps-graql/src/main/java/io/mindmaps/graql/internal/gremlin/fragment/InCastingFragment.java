package io.mindmaps.graql.internal.gremlin.fragment;

import io.mindmaps.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static io.mindmaps.util.Schema.EdgeLabel.CASTING;

class InCastingFragment extends AbstractFragment {

    InCastingFragment(String start, String end) {
        super(start, end);
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        traversal.in(CASTING.getLabel());
    }

    @Override
    public FragmentPriority getPriority() {
        return FragmentPriority.EDGE_UNBOUNDED;
    }
}
