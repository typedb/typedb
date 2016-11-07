package io.mindmaps.graql.internal.gremlin.fragment;

import io.mindmaps.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static io.mindmaps.graql.internal.util.StringConverter.valueToString;
import static io.mindmaps.util.Schema.ConceptProperty.RULE_RHS;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        RhsFragment that = (RhsFragment) o;

        return rhs != null ? rhs.equals(that.rhs) : that.rhs == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (rhs != null ? rhs.hashCode() : 0);
        return result;
    }
}
