package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static io.grakn.util.Schema.EdgeLabel.HAS_SCOPE;

class InHasScopeFragment extends AbstractFragment {

    InHasScopeFragment(String start, String end) {
        super(start, end);
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        traversal.in(HAS_SCOPE.getLabel());
    }

    @Override
    public String getName() {
        return "<-[has-scope]-";
    }

    @Override
    public FragmentPriority getPriority() {
        return FragmentPriority.EDGE_BOUNDED;
    }

    @Override
    public long fragmentCost(long previousCost) {
        return previousCost * NUM_SCOPES_PER_INSTANCE;
    }
}
