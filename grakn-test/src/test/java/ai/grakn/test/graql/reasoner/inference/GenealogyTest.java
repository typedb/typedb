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
import ai.grakn.test.AbstractEngineTest;
import ai.grakn.test.graql.reasoner.graphs.GenealogyGraph;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GenealogyTest extends AbstractEngineTest{

    private static GraknGraph graph;
    private static Reasoner reasoner;
    private static QueryBuilder qb;

    @BeforeClass
    public static void setUpClass() {
        GenealogyGraph genealogyGraph = new GenealogyGraph();
        reasoner = new Reasoner(genealogyGraph.graph());
        graph = genealogyGraph.graph();
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
    public void testNonEquals(){
        String queryString= "match " +
                "$w isa wedding has confidence 'high';" +
                "$rel1 (happening: $w, protagonist: $s1) isa event-protagonist;" +
                "$rel1 has role 'spouse';"+
                "$rel2 (happening: $w, protagonist: $s2) isa event-protagonist;" +
                "$rel2 has role 'spouse';" +
                "$s1 != $s2;select $s1, $s2;";
        Query query = new Query(queryString, graph);
        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        assertTrue(!hasDuplicates(answers));
    }

    @Test
    public void testSpecificPerson(){
        Concept concept = Sets.newHashSet(graph.graql().<MatchQuery>parse("match $x isa person;"))
                .iterator().next()
                .entrySet().iterator().next().getValue();
        String queryString = "match $x id '" + concept.getId() + "' has gender $g;";
        MatchQuery query = new Query(queryString, graph);
        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        assert(answers.size() == 1);
    }

    @Test
    public void testGender() {
        String queryString = "match $x isa person has identifier $id has gender $gender;";
        MatchQuery query = new Query(queryString, graph);
        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testName() {
        String queryString = "match $x isa person, has firstname $n;";
        MatchQuery query = new Query(queryString, graph);
        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testMiddleName() {
        String queryString = "match $x has identifier $i has middlename $mn;";
        MatchQuery query = new Query(queryString, graph);
        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testSurName() {
        String queryString = "match $x isa person has surname $srn;";
        MatchQuery query = new Query(queryString, graph);
        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testParentship() {
        String queryString = "match (child: $x, parent: $y) isa parentship;";
        MatchQuery query = new Query(queryString, graph);
        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        answers.forEach(answer -> assertTrue(answer.size() == 2));
        assertTrue(answers.size() == 66);
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
    }

    //It is expected that results are different due to how rules are defined
    @Ignore
    @Test
    public void testParentship2() {
        String queryString = "match (child: $x, $y) isa parentship;select $x;";
        String queryString2 = "match (child: $x) isa parentship;";
        MatchQuery query = new Query(queryString, graph);
        MatchQuery query2 = new Query(queryString2, graph);
        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        QueryAnswers answers2 = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query2)));
        answers.forEach(answer -> assertTrue(answer.size() == 2));
        assertEquals(answers, answers2);
        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
    }

    @Test
    public void testParentship3(){
        String queryString = "match ($x, son: $y) isa parentship;";
        MatchQuery query = new Query(queryString, graph);
        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        assertTrue(answers.isEmpty());
    }

    @Ignore
    @Test
    public void testMarriageType() {
        String queryString = "match $x isa marriage;";
        MatchQuery query = new Query(queryString, graph);
        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        assertTrue(!answers.isEmpty());
    }

    //TODO need to do all combinations for roles missing
    @Ignore
    @Test
    public void testMarriageMaterialisation() {
        String queryString = "match $rel ($x, $y) isa marriage;";
        MatchQuery query = new Query(queryString, graph);
        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        QueryAnswers answers2 = new QueryAnswers(Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
        assertTrue(!answers.isEmpty());
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers, answers2);
    }

    //Bug #11149
    @Test
    public void testMarriageMaterialisation2() {
        String queryString = "match $rel (spouse1: $x, spouse2: $y) isa marriage;";
        MatchQuery query = new Query(queryString, graph);
        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        QueryAnswers answers2 = new QueryAnswers(Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
        assertTrue(!answers.isEmpty());
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers, answers2);
    }

    @Test
    public void testMarriage() {
        String queryString = "match (spouse1: $x, spouse2: $y) isa marriage;";
        MatchQuery query = new Query(queryString, graph);
        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        assertTrue(!answers.isEmpty());
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
    }

    @Ignore
    @Test
    public void testMarriage2(){
        String queryString = "match ($r: $x) isa marriage; $r isa wife;";
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
        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        assertTrue(!answers.isEmpty());
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
    }

    @Test
    public void testCousins() {
        String queryString = "match ($x, $y) isa cousins;";
        MatchQuery query = new Query(queryString, graph);
        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        assertTrue(!answers.isEmpty());
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
    }

    @Test
    public void testInLaws() {
        String queryString = "match (parent-in-law: $x, child-in-law: $y) isa in-laws;$y has gender 'male';";
        MatchQuery query = new Query(queryString, graph);

        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        assertTrue(!answers.isEmpty());
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
    }

    @Test
    public void testMotherInLaw() {
        String queryString = "match (parent-in-law: $x) isa in-laws;" +
                "$x has gender $g;$g value 'female';$x has identifier $id;";
        MatchQuery query = new Query(queryString, graph);

        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        assertTrue(checkResource(answers, "g", "female"));
        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
    }

    @Test
    public void testMotherInLaw2() {
        String queryString = "match (mother-in-law: $x);$x has identifier $id;$x has gender $g;";
        String queryString2 = "match (parent-in-law: $x, $y) isa in-laws;" +
                "$x has gender $g;$g value 'female';$x has identifier $id; select $x, $g, $id;";
        MatchQuery query = new Query(queryString, graph);
        MatchQuery query2 = new Query(queryString2, graph);

        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        QueryAnswers answers2 = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query2)));
        assertEquals(answers, answers2);
        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testFatherInLaw() {
        String queryString = "match (father-in-law: $x); $x has identifier $id;";
        String queryString2 = "match (parent-in-law: $x, $y) isa in-laws;" +
                "$x has gender 'male';$x has identifier $id; select $x, $id;";
        MatchQuery query = new Query(queryString, graph);
        MatchQuery query2 = new Query(queryString2, graph);

        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        QueryAnswers answers2 = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query2)));
        assertEquals(answers, answers2);
        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testSonInLaw() {
        String queryString = "match (son-in-law: $x);$x has gender $g;";
        String queryString2 = "match (child-in-law: $x, $y) isa in-laws;" +
                "$x has gender $g;$g value 'male';select $x, $g;";
        MatchQuery query = new Query(queryString, graph);
        MatchQuery query2 = new Query(queryString2, graph);

        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        QueryAnswers answers2 = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query2)));
        assertEquals(answers, answers2);
        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testDaughterInLaw() {
        String queryString = "match (daughter-in-law: $x); $x has identifier $id;";
        String queryString2 = "match (child-in-law: $x, $y) isa in-laws;" +
                "$x has gender 'female';$x has identifier $id; select $x, $id;";
        MatchQuery query = new Query(queryString, graph);
        MatchQuery query2 = new Query(queryString2, graph);

        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        QueryAnswers answers2 = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query2)));
        assertEquals(answers, answers2);
        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
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
        String queryString = "match (son: $x);$x has gender $g;";
        String queryString2 = "match (child: $x, $y) isa parentship;" +
                "$x has gender $g;$g value 'male'; select $x, $g;";
        MatchQuery query = new Query(queryString, graph);
        MatchQuery query2 = new Query(queryString2, graph);

        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        QueryAnswers answers2 = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query2)));
        assertEquals(answers, answers2);
        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
        assertTrue(checkResource(answers, "g", "male"));
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testDaughter() {
        String queryString = "match (daughter: $x);$x has gender $g;";
        String queryString2 = "match (child: $x, $y) isa parentship;" +
                "$x has gender $g;$g value 'female'; select $x, $g;";
        MatchQuery query = new Query(queryString, graph);
        MatchQuery query2 = new Query(queryString2, graph);

        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        QueryAnswers answers2 = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query2)));
        assertEquals(answers, answers2);
        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
        assertTrue(checkResource(answers, "g", "female"));
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testFather() {
        String queryString = "match (father: $x);";
        String queryString2 = "match (parent: $x, $y) isa parentship;" +
                "$x has gender 'male'; select $x;";
        MatchQuery query = new Query(queryString, graph);
        MatchQuery query2 = new Query(queryString2, graph);

        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        QueryAnswers answers2 = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query2)));
        assertEquals(answers, answers2);
        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testMother() {
        String queryString = "match (mother: $x);";
        String queryString2 = "match (parent: $x, $y) isa parentship;" +
                "$x has gender 'female';select $x;";
        MatchQuery query = new Query(queryString, graph);
        MatchQuery query2 = new Query(queryString2, graph);

        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        QueryAnswers answers2 = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query2)));
        assertEquals(answers, answers2);
        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testFemaleFather() {
        String queryString = "match (father: $x) isa parentship; $x has gender $g; $g value 'female';";
        MatchQuery query = new Query(queryString, graph);
        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        assertTrue(answers.isEmpty());
    }


    @Test
    public void testGrandMother() {
        String queryString = "match (grandmother: $x) isa grandparentship;" +
                "$x has identifier $pidX;select $pidX;";
        String queryString2 = "match (grandparent: $x, $y) isa grandparentship;" +
                "$x has gender 'female';" +
                "$x has identifier $pidX; select $pidX;";
        MatchQuery query = new Query(queryString, graph);
        MatchQuery query2 = new Query(queryString2, graph);

        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        QueryAnswers answers2 = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query2)));
        assertEquals(answers, answers2);
        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
    }

    @Test
    public void testGrandMother2() {
        String queryString = "match (grandmother: $x) isa grandparentship; $x has gender $g;";
        String queryString2 = "match (grandparent: $x, $y) isa grandparentship;" +
                "$x has gender $g;$g value 'female';select $x, $g;";

        MatchQuery query = new Query(queryString, graph);
        MatchQuery query2 = new Query(queryString2, graph);

        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        QueryAnswers answers2 = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query2)));
        assertEquals(answers, answers2);
        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
    }

    @Test
    public void testGrandDaughter(){
        String queryString = "match (granddaughter: $x); $x has gender $g;";
        String queryString2 = "match (grandchild: $x, $y) isa grandparentship;" +
                "$x has gender $g;$g value 'female';select $x, $g;";

        MatchQuery query = new Query(queryString, graph);
        MatchQuery query2 = new Query(queryString2, graph);

        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        QueryAnswers answers2 = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query2)));
        assertEquals(answers, answers2);
        assertTrue(checkResource(answers, "g", "female"));
        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testGrandParentship(){
        String queryString = "match "+
                "(grandchild: $x); (granddaughter: $x);$x has gender $g;";
        MatchQuery query = new Query(queryString, graph);

        QueryAnswers answers = new QueryAnswers(Sets.newHashSet(reasoner.resolveToQuery(query)));
        assertTrue(checkResource(answers, "g", "female"));
        assertEquals(answers, Sets.newHashSet(qb.<MatchQuery>parse(queryString)));
        assertTrue(!answers.isEmpty());
    }

    //Bug #11150 Relations with resources as single VarAdmin
    @Test
    public void testRelationResources(){
        String queryString = "match $rel (happening: $b, protagonist: $p) isa event-protagonist has role 'parent';";
        String queryString2 = "match $rel (happening: $b, protagonist: $p) isa event-protagonist; $rel has role 'parent';";
        Query query = new Query(queryString, graph);
        Query query2 = new Query(queryString2, graph);

        assertTrue(query.equals(query2));
    }

    private boolean checkResource(QueryAnswers answers, String var, String value){
        boolean isOk = true;
        Iterator<Map<String, Concept>> it =  answers.iterator();
        while (it.hasNext() && isOk){
            Concept c = it.next().get(var);
            isOk = c.asResource().getValue().equals(value);
        }
        return isOk;
    }

    private boolean hasDuplicates(QueryAnswers answers){
        boolean hasDuplicates = false;
        Iterator<Map<String, Concept>> it = answers.iterator();
        while(it.hasNext() && !hasDuplicates){
            Map<String, Concept> answer = it.next();
            Set<Concept> existing = new HashSet<>();
            hasDuplicates = answer.entrySet()
                    .stream()
                    .filter(entry -> existing.add(entry.getValue()))
                    .count() != answer.size();
            if(hasDuplicates) System.out.println(answer.toString());
        }
        return hasDuplicates;
    }
}
