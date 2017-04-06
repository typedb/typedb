package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.graql.VarName;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static ai.grakn.util.Schema.EdgeLabel.PLAYS;
import static ai.grakn.util.Schema.EdgeLabel.SUB;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class InPlaysFragmentTest {

    private final VarName start = VarName.anon();
    private final VarName end = VarName.anon();
    private final InPlaysFragment fragment = new InPlaysFragment(start, end, false);

    @Test
    @SuppressWarnings("unchecked")
    public void testApplyTraversalFollowsSubsDownwards() {
        GraphTraversal<Vertex, Vertex> traversal = __.V();
        fragment.applyTraversal(traversal);

        // Make sure we traverse plays and downwards subs once
        assertThat(traversal, is(__.V()
                .in(PLAYS.getLabel())
                .union(__.identity(), __.repeat(__.in(SUB.getLabel())).emit()).unfold()
        ));
    }
}
