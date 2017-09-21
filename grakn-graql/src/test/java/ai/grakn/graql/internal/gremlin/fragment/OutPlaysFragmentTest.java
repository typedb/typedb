package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static ai.grakn.util.Schema.EdgeLabel.PLAYS;
import static ai.grakn.util.Schema.EdgeLabel.SUB;
import static ai.grakn.util.Schema.VertexProperty.THING_TYPE_LABEL_ID;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class OutPlaysFragmentTest {

    private final Var start = Graql.var();
    private final Var end = Graql.var();
    private final Fragment fragment = Fragments.outPlays(null, start, end, false);

    @Test
    @SuppressWarnings("unchecked")
    public void testApplyTraversalFollowsSubsUpwards() {
        GraphTraversal<Vertex, Vertex> traversal = __.V();
        fragment.applyTraversalInner(traversal, null, ImmutableSet.of());

        // Make sure we check this is a vertex, then traverse upwards subs once and plays
        assertThat(traversal, is(__.V()
                .has(Schema.VertexProperty.ID.name())
                .union(__.<Vertex>not(__.has(THING_TYPE_LABEL_ID.name())).not(__.hasLabel(Schema.BaseType.SHARD.name())), __.repeat(__.out(SUB.getLabel())).emit()).unfold()
                .out(PLAYS.getLabel())
        ));
    }
}
