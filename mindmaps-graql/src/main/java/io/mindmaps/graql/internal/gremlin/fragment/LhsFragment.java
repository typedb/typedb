package io.mindmaps.graql.internal.gremlin.fragment;

import io.mindmaps.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static io.mindmaps.graql.internal.util.StringConverter.valueToString;
import static io.mindmaps.util.Schema.ConceptProperty.RULE_LHS;

class LhsFragment extends AbstractFragment {

    private final String lhs;

    LhsFragment(String start, String lhs) {
        super(start);
        this.lhs = lhs;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        traversal.has(RULE_LHS.name(), lhs);
    }

    @Override
    public String getName() {
        return "[lhs:" + valueToString(lhs) + "]";
    }

    @Override
    public FragmentPriority getPriority() {
        return FragmentPriority.VALUE_NONSPECIFIC;
    }

    @Override
    public long fragmentCost(long previousCost) {
        return previousCost;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        LhsFragment that = (LhsFragment) o;

        return lhs != null ? lhs.equals(that.lhs) : that.lhs == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (lhs != null ? lhs.hashCode() : 0);
        return result;
    }
}
