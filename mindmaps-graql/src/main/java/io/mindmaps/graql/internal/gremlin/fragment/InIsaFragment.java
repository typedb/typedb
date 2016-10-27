package io.mindmaps.graql.internal.gremlin.fragment;

import io.mindmaps.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static io.mindmaps.graql.internal.gremlin.fragment.Fragments.inSubs;
import static io.mindmaps.util.Schema.BaseType.ROLE_TYPE;
import static io.mindmaps.util.Schema.EdgeLabel.ISA;

class InIsaFragment extends AbstractFragment {

    private final boolean allowCastings;

    InIsaFragment(String start, String end, boolean allowCastings) {
        super(start, end);
        this.allowCastings = allowCastings;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        if (!allowCastings) {
            // Make sure we never get instances of role types
            traversal.not(__.hasLabel(ROLE_TYPE.name()));
        }
        inSubs(inSubs(traversal).in(ISA.getLabel()));
    }

    @Override
    public String getName() {
        return "<-[isa" + (allowCastings ? ":allow-castings" : "") + "]-";
    }

    @Override
    public FragmentPriority getPriority() {
        return FragmentPriority.EDGE_UNBOUNDED;
    }

    @Override
    public long fragmentCost(long previousCost) {
        return previousCost * NUM_INSTANCES_PER_TYPE;
    }

}
