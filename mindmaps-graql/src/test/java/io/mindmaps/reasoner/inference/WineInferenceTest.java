package io.mindmaps.reasoner.inference;

import com.google.common.collect.Sets;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.graql.api.parser.QueryParser;
import io.mindmaps.graql.api.query.MatchQuery;
import io.mindmaps.reasoner.MindmapsReasoner;
import io.mindmaps.reasoner.graphs.WineGraph;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class WineInferenceTest {

    private static MindmapsReasoner reasoner;
    private static QueryParser qp;

    @BeforeClass
    public static void setUpClass() {

        MindmapsTransaction graph = WineGraph.getTransaction();
        reasoner = new MindmapsReasoner(graph);
        qp = QueryParser.create(graph);
    }

    @Test
    public void testRecommendation() {

        String queryString = "match $x isa person;$y isa wine;($x, $y) isa wine-recommendation";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);

        reasoner.printMatchQueryResults(expandedQuery.distinct());

        String explicitQuery = "match $x isa person;$y isa wine;" +
                               "{$x value 'Alice';$y value 'Cabernet Sauvignion'} or" +
                "{$x value 'Bob';$y value 'White Champagne'} or" +
                "{$x value 'Charlie';$y value 'Pinot Grigio Rose'} or" +
                "{$x value 'Denis';$y value 'Busuioaca Romaneasca'} or" +
                "{$x value 'Eva';$y value 'Tamaioasa Romaneasca'} or" +
                "{$x value 'Frank';$y value 'Riojo Blanco CVNE 2003'}";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());

    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
