package io.mindmaps.reasoner;

import com.google.common.collect.Sets;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.model.Rule;
import io.mindmaps.graql.api.parser.QueryParser;
import io.mindmaps.graql.api.query.MatchQuery;
import io.mindmaps.reasoner.graphs.SNBGraph;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class QueryTest {

    private static MindmapsTransaction graph;
    private static QueryParser qp;

    @BeforeClass
    public static void setUpClass() {

        graph = SNBGraph.getTransaction();
        qp = QueryParser.create(graph);
    }

    @Test
    public void testValuePredicate(){

        String queryString = "match $x isa person;$x value 'Bob'";

        Query query = new Query(queryString, graph);
        boolean containsAtom = false;
        for(Atom atom : query.getAtoms())
            if (atom.toString().equals("$x value \"Bob\"")) containsAtom = true;
        assertTrue(containsAtom);
        assertEquals(query.getValue("x"), "Bob");

    }
    @Test
    public void testCopyConstructor(){

        String queryString = "match $x isa person;$y isa product;($x, $y) isa recommendation";

        Rule r = graph.getRule("R53");

        Query query = new Query(queryString, graph);
        Query rule = new Query(r.getLHS(), graph);

        Atom recommendation = query.getAtomsWithType(graph.getType("recommendation")).iterator().next();
        query.expandAtomByQuery(recommendation, rule);

        Query copy = new Query(query);

        assertQueriesEqual( query.getExpandedMatchQuery(), copy.getExpandedMatchQuery());
    }

    @Test
    public void testExpansion()
    {
        String queryString = "match $x isa person;$y isa product;($x, $y) isa recommendation";

        String LHS = "match $x isa person;$t isa tag, value 'Michelangelo';\n" +
                "($x, $t) isa tagging;\n" +
                "$y isa product, value 'Michelangelo - The Last Judgement'; select $x, $y";

        Query query = new Query(queryString, graph);
        Query ruleLHS = new Query(LHS, graph);

        Atom recommendation = query.getAtomsWithType(graph.getType("recommendation")).iterator().next();
        query.expandAtomByQuery(recommendation, ruleLHS);

        query.getExpandedMatchQuery();

        assertQueriesEqual( query.getMatchQuery(), query.getExpandedMatchQuery());
    }

    @Test
    public void testDisjunctiveQuery()
    {
        String queryString = "match $x isa person;{$y isa product} or {$y isa tag};($x, $y) isa recommendation";

        MatchQuery sq = qp.parseMatchQuery(queryString).getMatchQuery();
        System.out.println(sq.toString());

        Query query = new Query(queryString, graph);
        Atom atom = query.getAtomsWithType(graph.getEntityType("product")).iterator().next();
        query.expandAtomByQuery(atom, new Query("match $y isa product;$y value 'blabla'", graph) );
        System.out.println(query.getExpandedMatchQuery().toString());

    }

    @Test
    public void testDisjunctiveRule()
    {
        String queryString = "match $x isa person;{$y isa product} or {$y isa tag};($x, $y) isa recommendation";

        MatchQuery sq = qp.parseMatchQuery(queryString).getMatchQuery();
        System.out.println(sq.toString());

        Query query = new Query(queryString, graph);
        Query rule = new Query(graph.getRule("R33").getLHS(), graph);

        Atom atom = query.getAtomsWithType(graph.getRelationType("recommendation")).iterator().next();
        query.expandAtomByQuery(atom, rule);
        System.out.println(query.getExpandedMatchQuery().toString());

    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
