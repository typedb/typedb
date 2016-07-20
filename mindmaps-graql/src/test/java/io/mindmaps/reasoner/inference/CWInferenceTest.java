package io.mindmaps.reasoner.inference;

import com.google.common.collect.Sets;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.model.Type;
import io.mindmaps.core.model.Rule;
import io.mindmaps.core.model.RuleType;
import io.mindmaps.graql.api.parser.QueryParser;
import io.mindmaps.graql.api.query.MatchQuery;
import io.mindmaps.reasoner.MindmapsReasoner;
import io.mindmaps.reasoner.graphs.CWGraph;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class CWInferenceTest {

    private static MindmapsTransaction graph;
    private static MindmapsReasoner reasoner;
    private static QueryParser qp;

    @BeforeClass
    public static void setUpClass() {

        graph = CWGraph.getTransaction();
        reasoner = new MindmapsReasoner(graph);
        qp = QueryParser.create(graph);
    }

    private static void printMatchQuery(MatchQuery query) {
        System.out.println(query.toString().replace(" or ", "\nor\n").replace("};", "};\n").replace("; {", ";\n{"));
    }

    @Test
    public void testTransactionQuery() {
        String queryString = "match" +
                       "$x isa person;\n" +
                        "$z isa country;\n" +
                        "$y isa weapon;\n" +
                        "($x, $y, $z) isa transaction";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);
        printMatchQuery(expandedQuery);
        reasoner.printMatchQueryResults(expandedQuery.distinct());

        String explicitQuery = "match \n" +
                "$x isa person;\n" +
                "$z isa country;\n" +
                "{$y isa weapon} or {\n" +
                "{{$y isa missile} or {$y isa rocket;$y has propulsion 'gsp';}} or {$y isa rocket;$y has propulsion 'gsp';};\n" +
                "};\n" +
                "{($x, $y, $z) isa transaction} or {\n" +
                "$x isa person;\n" +
                "$z isa country;\n" +
                "{{$y isa weapon} or {\n" +
                "{{$y isa missile} or {$y isa rocket;$y has propulsion 'gsp';}} or {$y isa rocket;$y has propulsion 'gsp';};\n" +
                "}} or {{$y isa missile} or {$y isa rocket;$y has propulsion 'gsp';};};\n" +
                "($x, $z) isa is-paid-by;\n" +
                "($z, $y) isa owns\n" +
                "}";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());

    }

    @Test
    public void testQuery()
    {
        String queryString = "match $x isa criminal;";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);
        printMatchQuery(expandedQuery);

        reasoner.printMatchQueryResults(expandedQuery.distinct());

        String explicitQuery = "match " +
                "{$x isa criminal} or {" +
                "$x has nationality 'American';\n" +
                "($x, $y, $z) isa transaction or {" +
                    "$x isa person;\n" +
                    "$z isa country;\n" +
                    "{ {$y isa weapon} or { {$y isa missile} or {$y isa rocket;$y has propulsion 'gsp'} } };\n" +
                    "($x, $z) isa is-paid-by;\n" +
                    "($z, $y) isa owns\n" +
                    "};\n" +
                "{$y isa weapon} or {$y isa missile} or {$y has propulsion 'gsp';$y isa rocket};\n" +
                "{$z has alignment 'hostile'} or {" +
                    "$yy value 'America';\n" +
                    "($z, $yy) isa is-enemy-of;\n" +
                    "$z isa country;" +
                    "$yy isa country" +
                    "};\n" +
                "$x isa person;\n" +
                "$z isa country\n" +
                "}; select $x";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    @Test
    public void testQueryWithOr()
    {
        String queryString = "match {$x isa criminal} or {$x has nationality 'American';$x isa person}";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);

        reasoner.printMatchQueryResults(expandedQuery.distinct());

        String explicitQuery = "match " +
            "{{$x isa criminal} or {$x has nationality 'American';\n" +
            "{$z has alignment 'hostile'} or {" +
                "$yy value 'America';\n" +
                "($z, $yy) isa is-enemy-of;\n" +
                "$z isa country;\n" +
                "$yy isa country" +
            "};\n" +
            "($x, $y, $z) isa transaction or {" +
                "$x isa person;\n" +
                "$z isa country;\n" +
                "{ {$y isa weapon} or { {$y isa missile} or {$y isa rocket;$y has propulsion 'gsp'} } };\n" +
                "($x, $z) isa is-paid-by;\n" +
                "($z, $y) isa owns\n" +
            "};\n" +
            "{$y isa weapon} or {{$y isa missile} or {$y has propulsion 'gsp';$y isa rocket}};\n" +
            "$x isa person;\n" +
            "$z isa country}} or {$x has nationality 'American';$x isa person} select $x";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());

    }


    @Test
    public void testGraphCase()
    {
        RuleType inferenceRule = graph.getRuleType("inference-rule");
        Rule R6 = graph.putRule("R6", inferenceRule);

        Type region = graph.putEntityType("region").setValue("region");

        String R6_LHS = "match x isa region";
        String R6_RHS = "match $x isa country";

        R6.setLHS(R6_LHS);
        R6.setRHS(R6_RHS);

        reasoner.linkConceptTypes();
        String queryString = "match $x isa criminal;";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);
        printMatchQuery(expandedQuery);

        reasoner.printMatchQueryResults(expandedQuery.distinct());

        String explicitQuery = "match " +
                "{$x isa criminal} or {\n" +
                "$x has nationality 'American';\n" +
                "($x, $y, $z) isa transaction or {" +
                    "$x isa person ;\n" +
                    "{$z isa country} or {$z isa region};\n" +
                    "{ {$y isa weapon} or { {$y isa missile} or {$y isa rocket;$y has propulsion 'gsp'} } };\n" +
                    "($x, $z) isa is-paid-by;\n" +
                    "($z, $y) isa owns\n" +
                "};\n" +
                "{$y isa weapon} or {{$y isa missile} or {$y has propulsion 'gsp';$y isa rocket}};\n" +
                "{$z has alignment 'hostile'} or {" +
                    "$yy value 'America';\n" +
                    "($z, $yy) isa is-enemy-of;\n" +
                    "$z isa country;\n" +
                    "$yy isa country" +
                "}" +
                "} select $x";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
