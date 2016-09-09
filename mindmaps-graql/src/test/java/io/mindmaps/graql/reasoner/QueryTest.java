/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.reasoner;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Rule;
import io.mindmaps.graql.*;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.admin.Conjunction;
import io.mindmaps.graql.admin.Disjunction;
import io.mindmaps.graql.internal.reasoner.container.QueryAnswers;
import io.mindmaps.graql.reasoner.graphs.SNBGraph;
import io.mindmaps.graql.internal.reasoner.container.Query;
import io.mindmaps.graql.internal.reasoner.predicate.Atomic;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class QueryTest {

    private static MindmapsGraph graph;
    private static QueryParser qp;
    private static QueryBuilder qb;

    @BeforeClass
    public static void setUpClass() {

        graph = SNBGraph.getGraph();
        qp = QueryParser.create(graph);
        qb = Graql.withGraph(graph);
    }

    @Test
    public void testValuePredicate(){
        String queryString = "match $x isa person;$x value 'Bob'";

        Query query = new Query(queryString, graph);
        boolean containsAtom = false;
        for(Atomic atom : query.getAtoms())
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

        Atomic recommendation = query.getAtomsWithType(graph.getType("recommendation")).iterator().next();
        query.expandAtomByQuery(recommendation, rule);

        Query copy = new Query(query);

        assertQueriesEqual( query.getExpandedMatchQuery(), copy.getExpandedMatchQuery());
    }

    @Test
    public void testExpansion() {
        String queryString = "match $x isa person;$y isa product;($x, $y) isa recommendation";

        String LHS = "match $x isa person;$t isa tag, value 'Michelangelo';\n" +
                "($x, $t) isa tagging;\n" +
                "$y isa product, value 'Michelangelo - The Last Judgement'; select $x, $y";

        Query query = new Query(queryString, graph);
        Query ruleLHS = new Query(LHS, graph);

        Atomic recommendation = query.getAtomsWithType(graph.getType("recommendation")).iterator().next();
        query.expandAtomByQuery(recommendation, ruleLHS);

        query.getExpandedMatchQuery();

        assertQueriesEqual( query.getMatchQuery(), query.getExpandedMatchQuery());
    }

    @Test
    public void testExpansion2() {
        String queryString = "match $x isa person;$y isa product;($x, $y) isa recommendation";

        String LHS = "match $x isa person;$t isa tag, value 'Michelangelo';\n" +
                "($x, $t) isa tagging;\n" +
                "$y isa product, value 'Michelangelo - The Last Judgement'; select $x, $y";

        Query query = new Query(queryString, graph);
        Query ruleLHS = new Query(LHS, graph);

        Atomic recommendation = query.getAtomsWithType(graph.getType("recommendation")).iterator().next();
        query.expandAtomByQuery(recommendation, ruleLHS);

        MatchQuery mq = query.getExpandedMatchQuery();

        Query queryCopy = new Query(query);
        assertQueriesEqual( queryCopy.getExpandedMatchQuery(), query.getExpandedMatchQuery());

        query.removeExpansionFromAtom(recommendation, ruleLHS);
        assertQueriesEqual( (new Query(queryString, graph)).getExpandedMatchQuery(), query.getExpandedMatchQuery());

        Atomic recommendationAgain = queryCopy.getAtomsWithType(graph.getType("recommendation")).iterator().next();
        Query expansion = recommendationAgain.getExpansions().iterator().next();
        queryCopy.removeExpansionFromAtom(queryCopy.getAtomsWithType(graph.getType("recommendation")).iterator().next(), expansion);
        assertQueriesEqual((new Query(queryString, graph)).getExpandedMatchQuery(), queryCopy.getExpandedMatchQuery());
    }

    @Test
    public void testDisjunctiveNormalForm() {
        String queryString = "match $x isa person;$y isa product;($x, $y) isa recommendation";

        String LHS = "match $x isa person;$t isa tag, value 'Michelangelo';\n" +
                "($x, $t) isa tagging;\n" +
                "$y isa product, value 'Michelangelo - The Last Judgement'; select $x, $y";

        Query query = new Query(queryString, graph);
        Query ruleLHS = new Query(LHS, graph);
        Atomic recommendation = query.getAtomsWithType(graph.getType("recommendation")).iterator().next();
        query.expandAtomByQuery(recommendation, ruleLHS);

        MatchQuery mq = query.getExpandedMatchQuery();

        Disjunction<Conjunction<VarAdmin>> disjunction = mq.admin().getPattern().getDisjunctiveNormalForm();
        System.out.println(disjunction.toString());

        query.changeVarName("y", "z");

        disjunction = query.getExpandedMatchQuery().admin().getPattern().getDisjunctiveNormalForm();

        System.out.println(disjunction.toString());
        }

    @Test
    public void testDisjunctiveQuery() {
        String queryString = "match $x isa person;{$y isa product} or {$y isa tag};($x, $y) isa recommendation";

        MatchQuery sq = qp.parseMatchQuery(queryString).getMatchQuery();
        System.out.println(sq.toString());

        Query query = new Query(queryString, graph);
        Atomic atom = query.getAtomsWithType(graph.getEntityType("product")).iterator().next();
        query.expandAtomByQuery(atom, new Query("match $y isa product;$y value 'blabla'", graph) );
        System.out.println(query.getExpandedMatchQuery().toString());

        System.out.println();
    }

    @Test
    public void testDisjunctiveRule() {
        String queryString = "match $x isa person;{$y isa product} or {$y isa tag};($x, $y) isa recommendation";

        MatchQuery sq = qp.parseMatchQuery(queryString).getMatchQuery();
        System.out.println(sq.toString());

        Query query = new Query(queryString, graph);
        Query rule = new Query(graph.getRule("R33").getLHS(), graph);

        Atomic atom = query.getAtomsWithType(graph.getRelationType("recommendation")).iterator().next();
        query.expandAtomByQuery(atom, rule);
        System.out.println(query.getExpandedMatchQuery().toString());
    }

    @Test
    public void testNonHornClauseQuery() {
        String queryString = "match $x isa person;$y isa product;($x, $y) isa recommendation";

        Query query = new Query(queryString, graph);
        Query expansionQuery = new Query(graph.getRule("R7a").getLHS(), graph);

        Atomic atom = query.getAtomsWithType(graph.getRelationType("recommendation")).iterator().next();
        query.expandAtomByQuery(atom, expansionQuery);
        System.out.println(query.getExpandedMatchQuery().toString());
    }

    @Test
    public void testTwinPattern() {
        String queryString = "match $x isa person;$x id 'Bob'";
        String queryString2 = "match $x isa person, id 'Bob'";
        String queryString3 = "match $x isa person, value 'Bob'";
        String queryString4 = "match $x isa person;$x value 'Bob'";

        Query query = new Query(queryString, graph);
        Query query2 = new Query(queryString2, graph);
        Query query3 = new Query(queryString3, graph);
        Query query4 = new Query(queryString4, graph);

        assertTrue(query.isEquivalent(query2));
        assertTrue(query3.isEquivalent(query4));
    }

    @Test
    public void testAlphaEquivalence() {
        String queryString = "match $x isa person;$t isa tag;$t value 'Michelangelo';" +
            "($x, $t) isa tagging;" +
            "$y isa product;$y value 'Michelangelo - The Last Judgement'; select $x, $y";

        String queryString2 = "match $x isa person;$y isa tag;$y value 'Michelangelo';" +
                "($x, $y) isa tagging;" +
                "$pr isa product;$pr value 'Michelangelo - The Last Judgement'; select $x, $pr";

        Query query = new Query(queryString, graph);
        Query query2 = new Query(queryString2, graph);

        assertTrue(query.isEquivalent(query2));
    }

    @Test
    public void testQueryResults(){
        //QueryResults answers = new QueryResults(Sets.newHashSet(qp.parseMatchQuery("match $x isa person").getMatchQuery()));

        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(qp.parseMatchQuery("match $x isa person").getMatchQuery()));

        answers.forEach(ans -> System.out.println(ans.toString()));
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }
}
