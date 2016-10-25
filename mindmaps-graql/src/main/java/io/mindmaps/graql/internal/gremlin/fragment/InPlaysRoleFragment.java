package io.mindmaps.graql.internal.gremlin.fragment;

import io.mindmaps.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static io.mindmaps.graql.internal.gremlin.Traversals.inSubs;
import static io.mindmaps.graql.internal.gremlin.Traversals.outSubs;
import static io.mindmaps.util.Schema.EdgeLabel.PLAYS_ROLE;

class InPlaysRoleFragment extends AbstractFragment {

    InPlaysRoleFragment(String start, String end) {
        super(start, end);
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        inSubs(outSubs(traversal).in(PLAYS_ROLE.getLabel()));
    }

    @Override
    public FragmentPriority getPriority() {
        return FragmentPriority.EDGE_BOUNDED;
    }

}
