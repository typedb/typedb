/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.test.graql.reasoner.inference;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Reasoner;
import ai.grakn.graql.internal.reasoner.query.Query;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.test.graql.reasoner.graphs.GenealogyGraph;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GenealogyTest{

    private static GraknGraph graph;
    private static Reasoner reasoner;
    private static QueryBuilder qb;

    @BeforeClass
    public static void setUpClass() {
        graph = GenealogyGraph.getGraph();
        reasoner = new Reasoner(graph);
        qb = graph.graql();
        /*
        //prerunning analytics
        graph.graql().parse("compute degreesAndPersist in event, conclusion-evidence;").execute();
        System.out.println("degree persisted...");
        graph = Grakn.factory(Grakn.DEFAULT_URI, graph.getKeyspace()).getGraph();
        System.out.println("Graph reopened...");
        */
    }

    /*
    test for first rule file:
    match $x isa person has first-name $name;
    match (child: $x, parent: $y) isa parentship;
    match (spouse1: $x, spouse2: $y) isa marriage;
    */

    @Test
    public void testSpecificPerson(){
        Concept concept = Sets.newHashSet(graph.graql().<MatchQuery>parse("match $x isa person;"))
                .iterator().next()
                .entrySet().iterator().next().getValue();
        String queryString = "match $x id '" + concept.getId() + "' has gender $g;";
        MatchQuery query = new Query(queryString, graph);
        Reasoner reasoner = new Reasoner(graph);
        QueryAnswers answers = reasoner.resolve(query);
        assert(answers.size() == 1);
    }

    @Test
    public void testGender() {
        String queryString = "match $x isa person has gender $gender;";
        MatchQuery query = new Query(queryString, graph);
        Reasoner reasoner = new Reasoner(graph);
        QueryAnswers answers = reasoner.resolve(query);
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testName() {
        String queryString = "match $x isa person, has firstname $n;";
        MatchQuery query = new Query(queryString, graph);
        Reasoner reasoner = new Reasoner(graph);
        QueryAnswers answers = reasoner.resolve(query);
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testMiddleName() {
        String queryString = "match $x has identifier $i has middlename $mn;";
        MatchQuery query = new Query(queryString, graph);
        Reasoner reasoner = new Reasoner(graph);
        QueryAnswers answers = reasoner.resolve(query);
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testSurName() {
        String queryString = "match $x isa person has surname $srn;";
        MatchQuery query = new Query(queryString, graph);
        Reasoner reasoner = new Reasoner(graph);
        QueryAnswers answers = reasoner.resolve(query);
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testParentship() {
        String queryString = "match (child: $x, parent: $y) isa parentship;";
        MatchQuery query = new Query(queryString, graph);
        Reasoner reasoner = new Reasoner(graph);
        QueryAnswers answers = reasoner.resolve(query);
        assertTrue(!answers.isEmpty());
    }

    @Test
    @Ignore
    public void testMarriageType() {
        String queryString = "match $x isa marriage;";
        MatchQuery query = new Query(queryString, graph);
        Reasoner reasoner = new Reasoner(graph);
        QueryAnswers answers = reasoner.resolve(query);
        assertTrue(!answers.isEmpty());
    }

    //Bug #11149
    @Test
    public void testMarriageMaterialisation() {
        String queryString = "match $rel ($x, $y) isa marriage;";
        MatchQuery query = new Query(queryString, graph);
        Reasoner reasoner = new Reasoner(graph);
        QueryAnswers answers = reasoner.resolve(query);
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testMarriage() {
        String queryString = "match (spouse1: $x, spouse2: $y) isa marriage;";
        MatchQuery query = new Query(queryString, graph);
        Reasoner reasoner = new Reasoner(graph);
        QueryAnswers answers = reasoner.resolve(query);
        assertTrue(!answers.isEmpty());
    }

    /*
    test for the second rule file:
    match ($x, $y) isa cousins;
    match (parent-in-law: $x, child-in-law: $y) isa in-laws;
    */

    @Test
    public void testSiblings() {
        String queryString = "match (sibling1:$x, sibling2:$y) isa siblings;";
        MatchQuery query = new Query(queryString, graph);
        Reasoner reasoner = new Reasoner(graph);
        QueryAnswers answers = reasoner.resolve(query);
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testCousins() {
        String queryString = "match ($x, $y) isa cousins;";
        MatchQuery query = new Query(queryString, graph);
        Reasoner reasoner = new Reasoner(graph);
        QueryAnswers answers = reasoner.resolve(query);
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testInLaws() {
        String queryString = "match (parent-in-law: $x, child-in-law: $y) isa in-laws;";
        MatchQuery query = new Query(queryString, graph);
        Reasoner reasoner = new Reasoner(graph);

        QueryAnswers answers = reasoner.resolve(query);
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testMotherInLaw() {
        String queryString = "match (mother-in-law: $x); $x has identifier $id;";
        String queryString2 = "match (parent-in-law: $x) isa in-laws;" +
                "$x has gender 'female';$x has identifier $id;";
        MatchQuery query = new Query(queryString, graph);
        MatchQuery query2 = new Query(queryString2, graph);
        Reasoner reasoner = new Reasoner(graph);

        QueryAnswers answers = reasoner.resolve(query);
        QueryAnswers answers2 = reasoner.resolve(query2);
        assertEquals(answers, answers2);
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testFatherInLaw() {
        String queryString = "match (father-in-law: $x); $x has identifier $id;";
        String queryString2 = "match (parent-in-law: $x) isa in-laws;" +
                "$x has gender 'male';$x has identifier $id;";
        MatchQuery query = new Query(queryString, graph);
        MatchQuery query2 = new Query(queryString2, graph);
        Reasoner reasoner = new Reasoner(graph);

        QueryAnswers answers = reasoner.resolve(query);
        QueryAnswers answers2 = reasoner.resolve(query2);
        assertEquals(answers, answers2);
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testSonInLaw() {
        String queryString = "match (son-in-law: $x); $x has identifier $id;";
        String queryString2 = "match (child-in-law: $x) isa in-laws;" +
                "$x has gender 'male';$x has identifier $id;";
        MatchQuery query = new Query(queryString, graph);
        MatchQuery query2 = new Query(queryString2, graph);
        Reasoner reasoner = new Reasoner(graph);

        QueryAnswers answers = reasoner.resolve(query);
        QueryAnswers answers2 = reasoner.resolve(query2);
        assertEquals(answers, answers2);
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testDaughterInLaw() {
        String queryString = "match (daughter-in-law: $x); $x has identifier $id;";
        String queryString2 = "match (child-in-law: $x) isa in-laws;" +
                "$x has gender 'female';$x has identifier $id;";
        MatchQuery query = new Query(queryString, graph);
        MatchQuery query2 = new Query(queryString2, graph);
        Reasoner reasoner = new Reasoner(graph);

        QueryAnswers answers = reasoner.resolve(query);
        QueryAnswers answers2 = reasoner.resolve(query2);
        assertEquals(answers, answers2);
        assertTrue(!answers.isEmpty());
    }

    /*
    test for the last rule-file:
    match (mother: $x);
    match (son: $x);
    match (grandfather: $x); (grandparent: $x, grandchild: $y);
    */

    @Test
    public void testSon() {
        String queryString = "match (son: $x);$x has identifier $id;";
        String queryString2 = "match (child: $x) isa parentship;" +
                "$x has gender 'male';$x has identifier $id;";
        MatchQuery query = new Query(queryString, graph);
        MatchQuery query2 = new Query(queryString2, graph);
        Reasoner reasoner = new Reasoner(graph);

        QueryAnswers answers = reasoner.resolve(query);
        QueryAnswers answers2 = reasoner.resolve(query2);
        assertEquals(answers, answers2);
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testMother() {
        String queryString = "match (mother: $x);";
        String queryString2 = "match (parent: $x) isa parentship;" +
                "$x has gender 'female';";
        MatchQuery query = new Query(queryString, graph);
        MatchQuery query2 = new Query(queryString2, graph);
        Reasoner reasoner = new Reasoner(graph);

        QueryAnswers answers = reasoner.resolve(query);
        QueryAnswers answers2 = reasoner.resolve(query2);
        assertEquals(answers, answers2);
        assertTrue(!answers.isEmpty());
    }

    @Test
    //TODO  (grandmother: $x) works, (grandmother: $x, $y) gets filtered too aggressively and returns no results
    public void testGrandMother() {
        String queryString = "match (grandmother: $x) isa grandparentship;" +
                "$x has identifier $pidX;select $pidX;";
        String queryString2 = "match (grandparent: $x) isa grandparentship;" +
                "$x has gender 'female';" +
                "$x has identifier $pidX; select $pidX;";
        MatchQuery query = new Query(queryString, graph);
        MatchQuery query2 = new Query(queryString2, graph);
        Reasoner reasoner = new Reasoner(graph);

        QueryAnswers answers = reasoner.resolve(query);
        QueryAnswers answers2 = reasoner.resolve(query2);
        assertEquals(answers, answers2);
    }

    @Test
    public void testGrandMother2() {
        String queryString = "match (grandmother: $x) isa grandparentship; $x has gender $g;";
        String queryString2 = "match (grandparent: $x) isa grandparentship;" +
                "$x has gender $g;$g value 'female';";

        MatchQuery query = new Query(queryString, graph);
        MatchQuery query2 = new Query(queryString2, graph);
        Reasoner reasoner = new Reasoner(graph);

        QueryAnswers answers = reasoner.resolve(query);
        QueryAnswers answers2 = reasoner.resolve(query2);
        assertEquals(answers, answers2);
    }

    @Test
    @Ignore
    public void testGrandParentship(){
        String queryString = "match "+
                "(grandfather: $x); (granddaughter: $y); (grandparent: $x, grandchild: $y) isa grandparentship;";
        MatchQuery query = new Query(queryString, graph);
        Reasoner reasoner = new Reasoner(graph);

        QueryAnswers answers = reasoner.resolve(query);
        assertTrue(!answers.isEmpty());
    }

}
