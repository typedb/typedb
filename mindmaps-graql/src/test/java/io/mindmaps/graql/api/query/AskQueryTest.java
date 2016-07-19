package io.mindmaps.graql.api.query;

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.example.MovieGraphFactory;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static io.mindmaps.graql.api.query.QueryBuilder.var;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AskQueryTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    private QueryBuilder qb;

    @Before
    public void setUp() {
        MindmapsGraph mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
        MovieGraphFactory.loadGraph(mindmapsGraph);
        MindmapsTransaction transaction = mindmapsGraph.newTransaction();
        qb = QueryBuilder.build(transaction);
    }

    @Test
    public void testPositiveQuery() {
        assertTrue(qb.match(var().isa("movie").has("tmdb-vote-count", 1000)).ask().execute());
    }

    @Test
    public void testNegativeQuery() {
        assertFalse(qb.match(var("y").isa("award")).ask().execute());
    }

    @Test
    public void testNegativeQueryShortcutEdge() {
        assertFalse(qb.match(
                var().rel("x").rel("y").isa("directed-by"),
                var("x").value("Apocalypse Now"),
                var("y").value("Martin Sheen")
        ).ask().execute());
    }
}
