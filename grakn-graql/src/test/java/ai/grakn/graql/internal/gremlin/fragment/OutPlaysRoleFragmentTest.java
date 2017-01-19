package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.Mocks;
import ai.grakn.graql.VarName;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.mockito.InOrder;

import static ai.grakn.util.Schema.EdgeLabel.PLAYS_ROLE;
import static ai.grakn.util.Schema.EdgeLabel.SUB;
import static org.mockito.Mockito.inOrder;

public class OutPlaysRoleFragmentTest {

    private final VarName start = VarName.anon();
    private final VarName end = VarName.anon();
    private final OutPlaysRoleFragment fragment = new OutPlaysRoleFragment(start, end, false);

    @Test
    @SuppressWarnings("unchecked")
    public void testApplyTraversalFollowsSubsUpwards() {
        GraphTraversal<Vertex, Vertex> mockTraversal = Mocks.traversal();
        fragment.applyTraversal(mockTraversal);

        InOrder order = inOrder(mockTraversal);

        // Make sure we follow subs upwards
        order.verify(mockTraversal).union(__.identity(), __.repeat(__.out(SUB.getLabel())).emit());
        order.verify(mockTraversal).unfold();

        // Make sure we traverse plays role
        order.verify(mockTraversal).out(PLAYS_ROLE.getLabel());

        // Make sure we DO NOT traverse subs downwards
        order.verifyNoMoreInteractions();
    }
}
