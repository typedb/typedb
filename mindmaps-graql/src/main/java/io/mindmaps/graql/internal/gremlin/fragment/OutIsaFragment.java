package io.mindmaps.graql.internal.gremlin.fragment;

import io.mindmaps.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static io.mindmaps.graql.internal.gremlin.fragment.Fragments.outSubs;
import static io.mindmaps.util.Schema.BaseType.CASTING;
import static io.mindmaps.util.Schema.EdgeLabel.ISA;

class OutIsaFragment extends AbstractFragment {

    private final boolean allowCastings;

    OutIsaFragment(String start, String end, boolean allowCastings) {
        super(start, end);
        this.allowCastings = allowCastings;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        if (!allowCastings) {
            // Make sure we never get castings' types
            traversal.not(__.hasLabel(CASTING.name()));
        }
        outSubs(outSubs(traversal).out(ISA.getLabel()));
    }

    @Override
    public String getName() {
        return "-[isa]->";
    }

    @Override
    public FragmentPriority getPriority() {
        return FragmentPriority.EDGE_UNIQUE;
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

        OutIsaFragment that = (OutIsaFragment) o;

        return allowCastings == that.allowCastings;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (allowCastings ? 1 : 0);
        return result;
    }
}
