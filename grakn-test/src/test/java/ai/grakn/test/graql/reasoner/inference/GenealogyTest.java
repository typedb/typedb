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

import ai.grakn.concept.Concept;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.MatchQueryAdmin;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graphs.GenealogyGraph;
import ai.grakn.test.GraphContext;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GenealogyTest {

    private static QueryBuilder qb;
    private static QueryBuilder iqb;

    @ClassRule
    public static final GraphContext genealogyGraph = GraphContext.preLoad(GenealogyGraph.get());

    @BeforeClass
    public static void setUpClass() throws Exception {
        qb = genealogyGraph.graph().graql().infer(false);
        iqb = genealogyGraph.graph().graql().infer(true).materialise(true);
    }

    /*
    test for first rule file:
    match $x isa person has first-name $name;
    match (child: $x, parent: $y) isa parentship;
    match (spouse1: $x, spouse2: $y) isa marriage;
    */

    @Test
    public void testMatchAll(){
        String queryString = "match $x isa document; ($x, $y);";
        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertTrue(answers.isEmpty());
    }

    @Test
    public void testMatchAll2(){
        String queryString = "match $x isa document; ($x, $y); $y isa entity;";
        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertTrue(answers.isEmpty());
    }

    @Test
    public void testNonEquals(){
        String queryString= "match " +
                "$w isa wedding has confidence 'high';" +
                "$rel1 (happening: $w, protagonist: $s1) isa event-protagonist;" +
                "$rel1 has event-role 'spouse';"+
                "$rel2 (happening: $w, protagonist: $s2) isa event-protagonist;" +
                "$rel2 has event-role 'spouse';" +
                "$s1 != $s2;select $s1, $s2;";
        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertTrue(!hasDuplicates(answers));
    }

    @Test
    public void testSpecificPerson(){
        Concept concept = Sets.newHashSet(genealogyGraph.graph().graql().infer(false).<MatchQuery>parse("match $x isa person;"))
                .iterator().next()
                .entrySet().iterator().next().getValue();
        String queryString = "match $x id '" + concept.getId() + "' has gender $g;";

        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 1);
    }

    @Test
    public void testFemale() {
        String queryString = "match $x isa person has identifier $id has gender 'female';";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        assertEquals(answers, Sets.newHashSet(qb.<MatchQueryAdmin>parse(queryString).results()));
        assertEquals(answers.size(), 32);
    }

    @Test
    public void testGender() {
        String queryString = "match $x isa person has identifier $id has gender $gender;";
        MatchQuery query = qb.parse(queryString);
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        assertEquals(answers, Sets.newHashSet(qb.<MatchQueryAdmin>parse(queryString).results()));
        assertEquals(answers.size(), qb.<MatchQueryAdmin>parse("match $x isa person;").execute().size());
    }

    @Test
    public void testName() {
        String queryString = "match $x isa person, has firstname $n;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testMiddleName() {
        String queryString = "match $x has identifier $i has middlename $mn;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testSurName() {
        String queryString = "match $x isa person has surname $srn;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testParentship() {
        String queryString = "match (child: $c, parent: $p) isa parentship;";
        String queryString2 = "match $b isa birth has confidence 'high';" +
        "$rel1 (happening: $b, protagonist: $p) isa event-protagonist;" +
        "$rel1 has event-role 'parent';" +
        "$rel2 (happening: $b, protagonist: $c) isa event-protagonist;" +
        "$rel2 has event-role 'newborn';select $c, $p;";
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = qb.parse(queryString2);
        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertTrue(!hasDuplicates(answers));
        answers.forEach(answer -> assertEquals(answer.size(), 2));
        assertEquals(76, answers.size());
        assertEquals(answers, answers2);
    }

    @Test
    public void testParentship2() {
        String queryString = "match (child: $x, $y) isa parentship;select $x;";
        String queryString2 = "match (child: $x) isa parentship;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        QueryAnswers answers2 = queryAnswers(iqb.parse(queryString2));
        answers.forEach(answer -> assertEquals(answer.size(), 1));
        assertEquals(answers, answers2);
        assertEquals(answers, Sets.newHashSet(qb.<MatchQueryAdmin>parse(queryString).results()));
    }

    @Test
    public void testParentship3(){
        String queryString = "match ($x, son: $y) isa parentship;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        assertTrue(answers.isEmpty());
    }

    @Test
    public void testComplexQuery(){
        String queryString = "match $a has firstname 'Ann' has surname 'Niesz';" +
                    "(wife: $a, husband: $w); (husband: $w, wife: $b) isa marriage;$a != $b;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers.size(), 1);
    }

    @Test
    public void testMarriageNotEquals(){
        String queryString = "match ($x, $y) isa marriage; ($y, $z) isa marriage;$x != $z;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers.size(), 4);
    }

    @Test
    public void testMarriageType() {
        String queryString = "match $x isa marriage;";
        String queryString2 = "match $x($x1, $x2) isa marriage;select $x;";
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertTrue(!answers.isEmpty());
        assertEquals(answers, answers2);
    }

    @Test
    public void testMarriageMaterialisation() {
        String queryString = "match $rel ($x, $y) isa marriage;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        QueryAnswers answers2 = queryAnswers(qb.parse(queryString));
        assertTrue(!answers.isEmpty());
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers, answers2);
    }

    //Bug #11149
    @Test
    public void testMarriageMaterialisation2() {
        String queryString = "match $rel (spouse1: $x, spouse2: $y) isa marriage;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        QueryAnswers answers2 = queryAnswers(qb.parse(queryString));
        assertTrue(!answers.isEmpty());
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers, answers2);
    }

    @Test
    public void testMarriage() {
        String queryString = "match (spouse1: $x, spouse2: $y) isa marriage;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        QueryAnswers answers2 = queryAnswers(qb.parse(queryString));
        assertTrue(!answers.isEmpty());
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers, answers2);
    }

    @Test
    public void testWife(){
        String queryString = "match $r (wife: $x) isa marriage;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        List<Map<String, Concept>> answerList = qb.<MatchQuery>parse(queryString).execute();
        QueryAnswers requeriedAnswers = queryAnswers(iqb.parse(queryString));
        assertEquals(answers, requeriedAnswers);
        List<Map<String, Concept>> answerList2 = qb.<MatchQuery>parse(queryString).execute();
        assertEquals(answerList, answerList2);
    }

    //TODO
    @Ignore
    @Test
    public void testWife2(){
        String queryString = "match ($r: $x) isa marriage;$r type-name 'wife';select $x;";
        String queryString2 = "match (wife: $x) isa marriage;";
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers2, queryAnswers(qb.<MatchQueryAdmin>parse(queryString2)));
        QueryAnswers answers = queryAnswers(query);
        assertTrue(!answers.isEmpty());
        assertEquals(answers, answers2);
    }

    /*
    test for the second rule file:
    match ($x, $y) isa cousins;
    match (parent-in-law: $x, child-in-law: $y) isa in-laws;
    */

    @Test
    public void testSiblings() {
        String queryString = "match (sibling1:$x, sibling2:$y) isa siblings;";
        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertTrue(!answers.isEmpty());
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
    }

    @Test
    public void testCousins() {
        String queryString = "match ($x, $y) isa cousins;";
        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertTrue(!answers.isEmpty());
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
    }

    @Test
    public void testInLaws() {
        String queryString = "match $x(parent-in-law: $x1, child-in-law: $x2) isa in-laws;";
        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        QueryAnswers requeriedAnswers = queryAnswers(query);
        assertTrue(!answers.isEmpty());
        assertEquals(answers.size(), requeriedAnswers.size());
    }

    @Test
    public void testInLaws2() {
        String queryString = "match (parent-in-law: $x, child-in-law: $y) isa in-laws;$y has gender 'male';";
        String queryString2 = "match $x isa in-laws;";
        String queryString3 = "match (parent-in-law: $x, child-in-law: $y) isa in-laws;";
        String queryString4 = "match $x(parent-in-law: $x1, child-in-law: $x2) isa in-laws;";
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        MatchQuery query3 = iqb.parse(queryString3);
        MatchQuery query4 = iqb.parse(queryString4);
        
        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        QueryAnswers answers4 = queryAnswers(query4);
        QueryAnswers answers3 =queryAnswers(query3);

        assertEquals(answers.size(), 22);
        assertEquals(answers2.size(), 92);
        assertEquals(answers3.size(), 50);
        assertEquals(answers3.size(), answers4.size());
    }

    @Test
    public void testMotherInLaw() {
        String queryString = "match (parent-in-law: $x) isa in-laws;" +
                "$x has gender $g;$g value 'female';$x has identifier $id;";
        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertTrue(checkResource(answers, "g", "female"));
        assertEquals(answers, Sets.newHashSet(qb.<MatchQueryAdmin>parse(queryString).results()));
    }

    @Test
    public void testMotherInLaw2() {
        String queryString = "match (mother-in-law: $x);$x has identifier $id;$x has gender $g;";
        String queryString2 = "match (parent-in-law: $x, $y) isa in-laws;" +
                "$x has gender $g;$g value 'female';$x has identifier $id; select $x, $g, $id;";
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);

        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers, answers2);
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testFatherInLaw() {
        String queryString = "match (father-in-law: $x); $x has identifier $id;";
        String queryString2 = "match (parent-in-law: $x, $y) isa in-laws;" +
                "$x has gender 'male';$x has identifier $id; select $x, $id;";
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);

        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers, answers2);
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testSonInLaw() {
        String queryString = "match (son-in-law: $x);$x has gender $g;";
        String queryString2 = "match (child-in-law: $x, $y) isa in-laws;" +
                "$x has gender $g;$g value 'male';select $x, $g;";
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);

        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers, answers2);
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testDaughterInLaw() {
        String queryString = "match (daughter-in-law: $x); $x has identifier $id;";
        String queryString2 = "match (child-in-law: $x, $y) isa in-laws;" +
                "$x has gender 'female';$x has identifier $id; select $x, $id;";
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);

        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers, answers2);
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
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
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);

        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers, answers2);
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
        assertTrue(checkResource(answers, "g", "male"));
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testDaughter() {
        String queryString = "match (daughter: $x);$x has gender $g;";
        String queryString2 = "match (child: $x, $y) isa parentship;" +
                "$x has gender $g;$g value 'female'; select $x, $g;";
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);

        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers, answers2);
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
        assertTrue(checkResource(answers, "g", "female"));
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testFather() {
        String queryString = "match (father: $x);";
        String queryString2 = "match (parent: $x, $y) isa parentship;" +
                "$x has gender 'male'; select $x;";
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);

        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers, answers2);
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testMother() {
        String queryString = "match (mother: $x);";
        String queryString2 = "match (parent: $x, $y) isa parentship;" +
                "$x has gender 'female';select $x;";
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);

        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers, answers2);
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testFemaleFather() {
        String queryString = "match (father: $x) isa parentship; $x has gender $g; $g value 'female';";
        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 =  queryAnswers(genealogyGraph.graph().graql().infer(true).materialise(false).parse(queryString));
        assertTrue(answers.isEmpty());
        assertEquals(answers, answers2);
    }


    @Test
    public void testGrandMother() {
        String queryString = "match (grandmother: $x) isa grandparentship;" +
                "$x has identifier $pidX;select $pidX;";
        String queryString2 = "match (grandparent: $x, $y) isa grandparentship;" +
                "$x has gender 'female';" +
                "$x has identifier $pidX; select $pidX;";
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);

        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers, answers2);
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
    }

    @Test
    public void testGrandMother2() {
        String queryString = "match (grandmother: $x) isa grandparentship; $x has gender $g;";
        String queryString2 = "match (grandparent: $x, $y) isa grandparentship;" +
                "$x has gender $g;$g value 'female';select $x, $g;";
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);

        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers, answers2);
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
    }

    @Test
    public void testGrandDaughter(){
        String queryString = "match (granddaughter: $x); $x has gender $g;";
        String queryString2 = "match (grandchild: $x, $y) isa grandparentship;" +
                "$x has gender $g;$g value 'female';select $x, $g;";
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);

        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers, answers2);
        assertTrue(checkResource(answers, "g", "female"));
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
        assertTrue(!answers.isEmpty());
    }

    @Test
    public void testGrandParentship(){
        String queryString = "match "+
                "(grandchild: $x); (granddaughter: $x);$x has gender $g;";
        MatchQuery query = iqb.parse(queryString);

        QueryAnswers answers = queryAnswers(query);
        assertTrue(checkResource(answers, "g", "female"));
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
        assertTrue(!answers.isEmpty());
    }

    private boolean checkResource(QueryAnswers answers, String var, String value){
        boolean isOk = true;
        Iterator<Map<VarName, Concept>> it =  answers.iterator();
        while (it.hasNext() && isOk){
            Concept c = it.next().get(VarName.of(var));
            isOk = c.asResource().getValue().equals(value);
        }
        return isOk;
    }

    private boolean hasDuplicates(QueryAnswers answers){
        boolean hasDuplicates = false;
        Iterator<Map<VarName, Concept>> it = answers.iterator();
        while(it.hasNext() && !hasDuplicates){
            Map<VarName, Concept> answer = it.next();
            Set<Concept> existing = new HashSet<>();
            hasDuplicates = answer.entrySet()
                    .stream()
                    .filter(entry -> existing.add(entry.getValue()))
                    .count() != answer.size();
            if(hasDuplicates) System.out.println(answer.toString());
        }
        return hasDuplicates;
    }

    private QueryAnswers queryAnswers(MatchQuery query) {
        return new QueryAnswers(query.admin().results());
    }
}
