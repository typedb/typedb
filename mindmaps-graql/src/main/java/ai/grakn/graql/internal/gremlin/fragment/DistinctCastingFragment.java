package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.graql.internal.gremlin.FragmentPriority;
import ai.grakn.graql.internal.gremlin.FragmentPriority;
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
    public String getName() {
        return "[distinct-casting:$" + otherCastingName + "]";
    }

    @Override
    public FragmentPriority getPriority() {
        return FragmentPriority.DISTINCT_CASTING;
    }

    @Override
    public long fragmentCost(long previousCost) {
        return previousCost / NUM_ROLES_PER_RELATION;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        DistinctCastingFragment that = (DistinctCastingFragment) o;

        return otherCastingName != null ? otherCastingName.equals(that.otherCastingName) : that.otherCastingName == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (otherCastingName != null ? otherCastingName.hashCode() : 0);
        return result;
    }
}
