package io.mindmaps.graql.internal.gremlin.fragment;

import io.mindmaps.graql.internal.gremlin.FragmentPriority;
import io.mindmaps.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

class NotCastingFragment extends AbstractFragment {

    NotCastingFragment(String start) {
        super(start);
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        traversal.not(__.hasLabel(Schema.BaseType.CASTING.name()));
    }

    @Override
    public String getName() {
        return "[not-casting]";
    }

    @Override
    public boolean isStartingFragment() {
        return true;
    }

    @Override
    public FragmentPriority getPriority() {
        return FragmentPriority.NOT_CASTING;
    }

    @Override
    public long fragmentCost(long previousCost) {
        return previousCost;
    }
}
