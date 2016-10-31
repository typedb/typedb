package io.grakn.graql.internal.gremlin.fragment;

import io.grakn.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static io.grakn.graql.internal.gremlin.fragment.Fragments.outSubs;
import static io.grakn.util.Schema.BaseType.CASTING;
import static io.grakn.util.Schema.EdgeLabel.ISA;

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

}
