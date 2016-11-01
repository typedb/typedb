package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.graql.internal.gremlin.FragmentPriority;
import ai.grakn.util.Schema;
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
    public FragmentPriority getPriority() {
        return FragmentPriority.NOT_CASTING;
    }

    @Override
    public long fragmentCost(long previousCost) {
        return previousCost;
    }
}
