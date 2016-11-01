package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static io.grakn.util.Schema.ConceptProperty.IS_ABSTRACT;

class IsAbstractFragment extends AbstractFragment {

    IsAbstractFragment(String start) {
        super(start);
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        traversal.has(IS_ABSTRACT.name(), true);
    }

    @Override
    public String getName() {
        return "[is-abstract]";
    }

    @Override
    public FragmentPriority getPriority() {
        return FragmentPriority.VALUE_NONSPECIFIC;
    }

    @Override
    public long fragmentCost(long previousCost) {
        return previousCost;
    }
}
