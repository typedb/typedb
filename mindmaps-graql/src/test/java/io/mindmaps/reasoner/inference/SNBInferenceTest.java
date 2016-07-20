package io.mindmaps.reasoner.inference;

import com.google.common.collect.Sets;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.graql.api.parser.QueryParser;
import io.mindmaps.graql.api.query.MatchQuery;
import io.mindmaps.reasoner.MindmapsReasoner;
import io.mindmaps.reasoner.graphs.SNBGraph;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SNBInferenceTest {

    private static MindmapsReasoner reasoner;
    private static QueryParser qp;

    @BeforeClass
    public static void setUpClass() {

        MindmapsTransaction graph = SNBGraph.getTransaction();
        qp = QueryParser.create(graph);
        reasoner = new MindmapsReasoner(graph);

    }
    private static void printMatchQuery(MatchQuery query) {
        System.out.println(query.toString().replace(" or ", "\nor\n").replace("};", "};\n").replace("; {", ";\n{"));
    }

    /**
     * Tests transitivity and Bug #7343
     */
    @Test
    @Ignore
    public void test()
    {
        String queryString = "match " +
                "{$x isa university} or {$x isa company};\n" +
                "$y isa country;\n" +
                "($x, $y) isa resides";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);
        printMatchQuery(expandedQuery);

        reasoner.printMatchQueryResults(expandedQuery.distinct());

        String explicitQuery = "match " +
                "{$x isa university;$x id 'University of Cambridge'} or" +
                "{$x isa company;$x id 'Mindmaps'};" +
                "$y isa country;$y id 'UK'";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());

    }

    /**
     * Tests transitivity and Bug #7343
     */
    @Test
    @Ignore
    public void test2()
    {
        String queryString = " match" +
                "{$x isa university} or {$x isa company};\n" +
                "$y isa continent;\n" +
                "($x, $y) isa resides";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);
        printMatchQuery(expandedQuery);

        reasoner.printMatchQueryResults(expandedQuery.distinct());

        String explicitQuery = "match " +
                            "{$x isa university} or {$x isa company};\n" +
                            "$y isa continent;\n" +
                            "{($x, $y) isa resides} or\n" +
                            "{($x, $yy) isa resides; {(container-location $y, member-location $yy) isa sublocate} or\n" +
                            "{(container-location $y, member-location $yyyy) isa sublocate; (container-location $yyyy, member-location $yy) isa sublocate}}" +
                            "select $x, $y";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());

    }

    /**
     * Tests relation filtering and rel vars matching
     */
    @Test
    public void testTag()
    {
        String queryString = "match " +
                "$x isa person;\n" +
                "$y isa tag;\n" +
                "($x, $y) isa recommendation";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);
        printMatchQuery(expandedQuery);

        reasoner.printMatchQueryResults(expandedQuery.distinct());

        String explicitQuery = "match " +
                "$x isa person;$y isa tag;" +
                "{$x id 'Charlie';{$y id 'yngwie-malmsteen'} or {$y id 'cacophony'} or {$y id 'steve-vai'} or {$y id 'black-sabbath'}} or " +
                "{$x id 'Gary';$y id 'pink-floyd'}";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());

    }

    /**
     * Tests relation filtering and rel vars matching
     */
    @Test
    public void testProduct()
    {
        String queryString = "match " +
                "$x isa person;\n" +
                "$y isa product;\n" +
                "($x, $y) isa recommendation";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);
        printMatchQuery(query);

        reasoner.printMatchQueryResults(expandedQuery.distinct());

        String explicitQuery = "match " +
                "$x isa person;$y isa product;" +
                "{$x id 'Alice';$y id 'war-of-the-worlds'} or" +
                "{$x id 'Bob';{$y id 'Ducatti-1299'} or {$y id 'The-good-the-bad-the-ugly'}} or" +
                "{$x id 'Charlie';{$y id 'blizzard-of-ozz'} or {$y id 'stratocaster'}} or " +
                "{$x id 'Denis';{$y id 'colour-of-magic'} or {$y id 'dorian-gray'}} or"+
                "{$x id 'Frank';$y id 'nocturnes'} or" +
                "{$x id 'Karl Fischer';{$y id 'faust'} or {$y id 'nocturnes'}} or " +
                "{$x id 'Gary';$y id 'the-wall'}";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());

    }

    @Test
    public void testBook()
    {
        String queryString = "match $x isa person;\n" +
                "($x, $y) isa recommendation;\n" +
                "$c isa category;$c value 'book';\n" +
                "($y, $c) isa typing; select $x, $y";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);
        printMatchQuery(expandedQuery);
        reasoner.printMatchQueryResults(expandedQuery.distinct());

        String explicitQuery = "match " +
                "$x isa person;$y isa product;" +
                "{$x id 'Alice';$y id 'war-of-the-worlds'} or" +
                "{$x id 'Karl Fischer';$y id 'faust'} or " +
                "{$x id 'Denis';{$y id 'colour-of-magic'} or {$y id 'dorian-gray'}}";


        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());

    }

    @Test
    public void testBand()
    {
        String queryString = "match $x isa person;\n" +
                "($x, $y) isa recommendation;\n" +
                "$c isa category;$c value 'Band';\n" +
                "($y, $c) isa grouping; select $x, $y";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);

        reasoner.printMatchQueryResults(expandedQuery.distinct());

        String explicitQuery = "match $x isa person;$y isa tag;" +
                "{$x id 'Charlie';{$y id 'cacophony'} or {$y id 'black-sabbath'}} or " +
                "{$x id 'Gary';$y id 'pink-floyd'}";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());

    }

    /**
     * Tests global variable consistency (Bug #7344)
     */
    @Test
    public void testVarConsistency(){

        String queryString = "match $x isa person;$y isa product;\n" +
                    "($x, $y) isa recommendation;\n" +
                    "$z isa category;$z value 'motorbike';\n" +
                    "($y, $z) isa typing; select $x(value), $y(value)";

        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);
        printMatchQuery(expandedQuery);

        reasoner.printMatchQueryResults(expandedQuery.distinct());

        String explicitQuery = "match $x isa person;$y isa product;" +
                "{$x id 'Bob';$y id 'Ducatti-1299'}";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    /**
     * tests whether rules are filtered correctly (rules recommending products other than Chopin should not be attached)
     */
    @Test
    public void testVarConsistency2(){

        //select people that have Chopin as a recommendation
        String queryString = "match $x isa person; $y isa tag; ($x, $y) isa tagging;\n" +
                        "$z isa product, value 'Chopin - Nocturnes'; ($x, $z) isa recommendation; select $x(value), $y(value)";

        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);
        printMatchQuery(expandedQuery);

        reasoner.printMatchQueryResults(expandedQuery.distinct());

        String explicitQuery = "match " +
                "{$x id 'Frank';$y id 'Ludwig_van_Beethoven'} or" +
                "{$x id 'Karl Fischer';" +
                "{$y id 'Ludwig_van_Beethoven'} or {$y id 'Johann Wolfgang von Goethe'} or {$y id 'Wolfgang_Amadeus_Mozart'}}";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    @Test
    public void testVarConsistency3(){

        String queryString = "match $x isa person;$pr isa product, value \"Chopin - Nocturnes\";($x, $pr) isa recommendation; select $x(value)";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);
        printMatchQuery(expandedQuery);

        reasoner.printMatchQueryResults(expandedQuery.distinct());

        String explicitQuery = "match {$x id 'Frank'} or {$x id 'Karl Fischer'}";

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }

    /**
     * Tests transitivity and Bug #7416
     */
    @Test
    @Ignore
    public void testQueryConsistency() {

        String queryString = "match $x isa person; $y isa place; ($x, $y) isa resides;\n" +
                        "$z isa person, value \"Miguel Gonzalez\"; ($x, $z) isa knows; select $x(value), $y(value)";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);
        printMatchQuery(expandedQuery);

        System.out.println();

        String queryString2 = "match $x isa person; $y isa person, value \"Miguel Gonzalez\";\n" +
                        "$z isa place; ($x, $y) isa knows; ($x, $z) isa resides; select $x(value), $z(value)";
        MatchQuery query2 = qp.parseMatchQuery(queryString2).getMatchQuery();
        MatchQuery expandedQuery2 = reasoner.expandQuery(query2);

        System.out.println(expandedQuery2);

    }

    /**
     * Tests Bug #7416
     * the $t variable in the query matches with $t from rules so if the rule var is not changed an extra condition is created
     * which renders the query unsatisfiable
     */
    @Test
    public void testOrdering() {

        //select recommendationS of Karl Fischer and their types
        String queryString = "match $p isa product;$x isa person, value \"Karl Fischer\";" +
                        "($x, $p) isa recommendation; ($p, $t) isa typing; select $p(value), $t(value)";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);
        printMatchQuery(expandedQuery);

        String queryString2 = "match $p isa product; $x isa person, value \"Karl Fischer\";" +
                        "($p, $c) isa typing; ($x, $p) isa recommendation; select $p(value), $c(value)";
        MatchQuery query2 = qp.parseMatchQuery(queryString2).getMatchQuery();
        MatchQuery expandedQuery2 = reasoner.expandQuery(query2);

        String explicitQuery = "match $p isa product;\n" +
                "$x isa person, value 'Karl Fischer';{($x, $p) isa recommendation} or" +
                "{$x isa person;$tt isa tag, value 'Johann_Wolfgang_von_Goethe';($x, $tt) isa tagging;$p isa product, value 'Faust'} or" +
                "{$x isa person; $p isa product, value \"Chopin - Nocturnes\"; $tt isa tag; ($tt, $x), isa tagging};" +
                "($p, $t) isa typing; select $p(value), $t(value)";

        String explicitQuery2 = "match $p isa product;\n" +
                "$x isa person, value 'Karl Fischer';{($x, $p) isa recommendation} or" +
                "{$x isa person;$t isa tag, value 'Johann_Wolfgang_von_Goethe';($x, $t) isa tagging;$p isa product, value 'Faust'} or" +
                "{$x isa person; $p isa product, value \"Chopin - Nocturnes\"; $t isa tag; ($t, $x), isa tagging};" +
                "($p, $c) isa typing; select $p(value), $c(value)";

        reasoner.printMatchQueryResults(expandedQuery.distinct());

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());
        assertQueriesEqual(expandedQuery2, qp.parseMatchQuery(explicitQuery2).getMatchQuery());
    }

    /**
     * Tests Bug #7422
     * Currently the necessary replacement $t->$tt doesn't take place.
     */
    @Test
    public void testInverseVars() {

        //select recommendation of Karl Fischer and their types
        String queryString = "match $p isa product;\n" +
                "$x isa person, value \"Karl Fischer\"; ($p, $x) isa recommendation; ($p, $t) isa typing; select $p(value), $t(value)";
        MatchQuery query = qp.parseMatchQuery(queryString).getMatchQuery();
        MatchQuery expandedQuery = reasoner.expandQuery(query);
        printMatchQuery(expandedQuery);

        String explicitQuery = "match $p isa product;" +
                "$x isa person, value 'Karl Fischer';{($x, $p) isa recommendation} or" +
                "{$x isa person; $p isa product, value \"Chopin - Nocturnes\"; $tt isa tag; ($tt, $x), isa tagging} or" +
                "{$x isa person;$tt isa tag, value 'Johann_Wolfgang_von_Goethe';($x, $tt) isa tagging;$p isa product, value 'Faust'}" +
                ";($p, $t) isa typing; select $p(value), $t(value)";

        reasoner.printMatchQueryResults(expandedQuery.distinct());

        assertQueriesEqual(expandedQuery, qp.parseMatchQuery(explicitQuery).getMatchQuery());
    }
    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
