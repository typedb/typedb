package io.mindmaps.reasoner.inference;

import com.google.common.collect.Sets;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.graql.api.parser.QueryParser;
import io.mindmaps.graql.api.query.MatchQuery;
import io.mindmaps.reasoner.MindmapsReasoner;
import io.mindmaps.reasoner.graphs.AbstractGraph;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class AbstractInferenceTest {

    private static MindmapsReasoner reasoner;
    private static QueryParser qp;

    @BeforeClass
    public static void setUpClass() {

        MindmapsTransaction graph = AbstractGraph.getTransaction();
        reasoner = new MindmapsReasoner(graph);
        qp = QueryParser.create(graph);
    }


    @Test
    public void testQuery()
    {
        String queryString = "match $x isa Q;";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expQuery = reasoner.expandQuery(query);
        reasoner.printMatchQueryResults(expQuery);

        String explicitQuery = "match " +
                "{$x isa Q} or {\n" +
                "{$y isa q} or {$y isa t};\n" +
                "{{$x isa p} or {$x isa s}} or {{$x isa r} or {$x isa u}};\n" +
                "($x, $y) isa rel\n" +
                "}; select $x";

        assertQueriesEqual(expQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    @Test
    public void testQuery2()
    {
        String queryString = "match " +
                        "$y isa Q;\n" +
                        "$x isa P;\n" +
                        "($y, $x) isa REL; select $y";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expQuery = reasoner.expandQuery(query);
        String expQueryString = expQuery.toString().replace(" or ", "\nor\n").replace("};", "};\n").replace("; {", ";\n{");
        System.out.println(expQueryString);
        reasoner.printMatchQueryResults(expQuery);

        String explicitQuery = "match " +
                                "{$y isa Q} or {{$yy isa q} or {$yy isa t};\n" +
                                "($y, $yy) isa rel;\n" +
                                "{{$y isa p} or {$y isa s}} or {{$y isa r} or {$y isa u}}};\n" +
                                "$x isa P;\n" +
                                "($y, $x) isa REL; select $y";
        assertQueriesEqual(expQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }

}
