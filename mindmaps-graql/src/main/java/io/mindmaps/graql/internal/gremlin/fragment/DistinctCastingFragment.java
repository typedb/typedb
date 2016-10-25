package io.mindmaps.graql.internal.gremlin.fragment;

import io.mindmaps.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

class DistinctCastingFragment extends AbstractFragment {

    private final String otherCastingName;

    DistinctCastingFragment(String start, String otherCastingName) {
        super(start);
        this.otherCastingName = otherCastingName;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        traversal.where(P.neq(otherCastingName));
    }

    @Override
    public FragmentPriority getPriority() {
        return FragmentPriority.DISTINCT_CASTING;
    }
}
