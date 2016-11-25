package ai.grakn.graql.internal.gremlin.fragment;

import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

class NeqFragment extends AbstractFragment {

    private final String other;

    NeqFragment(String start, String other) {
        super(start);
        this.other = other;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        traversal.where(P.neq(other));
    }

    @Override
    public String getName() {
        return "[neq:$" + other + "]";
    }

    @Override
    public double fragmentCost(double previousCost) {
        return previousCost;
    }

    @Override
    public ImmutableSet<String> getDependencies() {
        return ImmutableSet.of(other);
    }
}
