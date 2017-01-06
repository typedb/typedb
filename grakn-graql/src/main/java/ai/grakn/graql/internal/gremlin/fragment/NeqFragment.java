package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.graql.VarName;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

class NeqFragment extends AbstractFragment {

    private final VarName other;

    NeqFragment(VarName start, VarName other) {
        super(start);
        this.other = other;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        traversal.where(P.neq(other.getValue()));
    }

    @Override
    public String getName() {
        return "[neq:" + other.shortName() + "]";
    }

    @Override
    public double fragmentCost(double previousCost) {
        return previousCost;
    }

    @Override
    public ImmutableSet<VarName> getDependencies() {
        return ImmutableSet.of(other);
    }
}
