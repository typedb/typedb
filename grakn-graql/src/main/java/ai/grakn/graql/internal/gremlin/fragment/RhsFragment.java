package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static io.grakn.graql.internal.util.StringConverter.valueToString;
import static io.grakn.util.Schema.ConceptProperty.RULE_RHS;

class RhsFragment extends AbstractFragment {

    private final String rhs;

    RhsFragment(String start, String rhs) {
        super(start);
        this.rhs = rhs;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        traversal.has(RULE_RHS.name(), rhs);
    }

    @Override
    public String getName() {
        return "[rhs:" + valueToString(rhs) + "]";
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
