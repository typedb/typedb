package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static ai.grakn.graql.internal.gremlin.fragment.Fragments.inSubs;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.outSubs;
import static ai.grakn.util.Schema.EdgeLabel.PLAYS_ROLE;

class OutPlaysRoleFragment extends AbstractFragment {

    OutPlaysRoleFragment(String start, String end) {
        super(start, end);
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        inSubs(outSubs(traversal).out(PLAYS_ROLE.getLabel()));
    }

    @Override
    public String getName() {
        return "-[plays-role]->";
    }

    @Override
    public FragmentPriority getPriority() {
        return FragmentPriority.EDGE_BOUNDED;
    }

    @Override
    public long fragmentCost(long previousCost) {
        return previousCost * NUM_ROLES_PER_TYPE;
    }

}
