package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.graql.internal.gremlin.FragmentPriority;
import ai.grakn.util.Schema;
import ai.grakn.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static ai.grakn.graql.internal.gremlin.fragment.Fragments.outSubs;
import static ai.grakn.util.Schema.BaseType.CASTING;
import static ai.grakn.util.Schema.EdgeLabel.ISA;

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
            traversal.not(__.hasLabel(Schema.BaseType.CASTING.name()));
        }
        Fragments.outSubs(Fragments.outSubs(traversal).out(Schema.EdgeLabel.ISA.getLabel()));
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
