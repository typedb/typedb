package io.mindmaps.reasoner.inference;

import com.google.common.collect.Sets;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.graql.api.parser.QueryParser;
import io.mindmaps.graql.api.query.MatchQuery;
import io.mindmaps.reasoner.MindmapsReasoner;
import io.mindmaps.reasoner.graphs.GeoGraph;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class GeoInferenceTest {

    private static MindmapsTransaction graph;
    private static MindmapsReasoner reasoner;
    private static QueryParser qp;

    @BeforeClass
    public static void setUpClass() {

        graph = GeoGraph.getTransaction();
        reasoner = new MindmapsReasoner(graph);
        qp = QueryParser.create(graph);
    }

    @Test
    @Ignore
    public void testQuery()
    {
        //show me all cities in Poland
        String queryString = "match " +
                        "$x isa city;\n" +
                        "($x, $y) isa isLocatedIn;\n"+
                        "$y isa country;\n" +
                        "$y value 'Poland'; select $x";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);

        reasoner.printMatchQueryResults(expandedQuery.distinct());


        String explicitQuery = "match " +
                "$x isa city;\n" +
                "($x, $y) isa isLocatedIn or"+
                "{($x, $z) isa isLocatedIn; ($z, $y) isa isLocatedIn};\n" +
                "$y isa country;\n" +
                "$y value 'Poland'; select $x";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());

    }

    @Test
    @Ignore
    public void testQuery2()
    {
        //show me all universities in Poland
        String queryString = "match " +
                "$x isa university;\n" +
                "($x, $y) isa isLocatedIn;\n"+
                "$y isa country;\n" +
                "$y value 'Poland'; select $x";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);

        reasoner.printMatchQueryResults(expandedQuery.distinct());


        String explicitQuery = "match " +
                "$x isa university;\n" +
                "($x, $y) isa isLocatedIn or"+
                "{($x, $zz) isa isLocatedIn; ($zz, $z) isa isLocatedIn;($z, $y) isa isLocatedIn};\n" +
                "$y isa country;\n" +
                "$y value 'Poland'; select $x";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());

    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
