package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.graql.internal.gremlin.FragmentPriority;
import ai.grakn.graql.internal.util.StringConverter;
import ai.grakn.util.Schema;
import ai.grakn.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static ai.grakn.graql.internal.util.StringConverter.valueToString;
import static ai.grakn.util.Schema.ConceptProperty.RULE_LHS;

class LhsFragment extends AbstractFragment {

    private final String lhs;

    LhsFragment(String start, String lhs) {
        super(start);
        this.lhs = lhs;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        traversal.has(Schema.ConceptProperty.RULE_LHS.name(), lhs);
    }

    @Override
    public String getName() {
        return "[lhs:" + StringConverter.valueToString(lhs) + "]";
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
