package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.graql.VarName;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static ai.grakn.util.Schema.EdgeLabel.PLAYS_ROLE;
import static ai.grakn.util.Schema.EdgeLabel.SUB;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class OutPlaysRoleFragmentTest {

    private final VarName start = VarName.anon();
    private final VarName end = VarName.anon();
    private final OutPlaysRoleFragment fragment = new OutPlaysRoleFragment(start, end, false);

    @Test
    @SuppressWarnings("unchecked")
    public void testApplyTraversalFollowsSubsUpwards() {
        GraphTraversal<Vertex, Vertex> traversal = __.V();
        fragment.applyTraversal(traversal);

        // Make sure we traverse upwards subs once and plays role
        assertThat(traversal, is(__.V()
                .union(__.identity(), __.repeat(__.out(SUB.getLabel())).emit()).unfold()
                .out(PLAYS_ROLE.getLabel())
        ));
    }
}
