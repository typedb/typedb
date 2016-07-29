package io.mindmaps.reasoner;


import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.graql.api.parser.QueryParser;
import io.mindmaps.graql.api.query.MatchQuery;
import io.mindmaps.graql.api.query.QueryBuilder;
import io.mindmaps.reasoner.graphs.SNBGraph;
import io.mindmaps.reasoner.internal.container.Query;
import io.mindmaps.reasoner.internal.predicate.Atom;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.mindmaps.reasoner.internal.Utility.printMatchQueryResults;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AtomicTest {
    private static MindmapsTransaction graph;
    private static QueryParser qp;
    private static QueryBuilder qb;

    @BeforeClass
    public static void setUpClass() {

        graph = SNBGraph.getTransaction();
        qp = QueryParser.create(graph);
        qb = QueryBuilder.build(graph);
    }

    @Test
    public void testValuePredicate(){

        String queryString = "match " +
                "$x1 isa person;\n" +
                "$x2 isa tag;\n" +
                "($x1, $x2) isa recommendation";

        Query query = new Query(queryString, graph);
        MatchQuery MQ = qp.parseMatchQuery(queryString).getMatchQuery();
        printMatchQueryResults(MQ);

    }


}
