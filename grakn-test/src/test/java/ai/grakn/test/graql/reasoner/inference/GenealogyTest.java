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
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.MatchQueryAdmin;
import ai.grakn.graql.internal.reasoner.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graphs.GenealogyGraph;
import ai.grakn.test.GraphContext;
import com.google.common.collect.Sets;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
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
        String queryString = "match $x isa person has gender 'female';";
        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 32);
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
    }

    @Test
    public void testGender() {
        String queryString = "match $x isa person has gender $gender;";
        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
        assertEquals(answers.size(), qb.<MatchQueryAdmin>parse("match $x isa person;").execute().size());
    }

    @Test
    public void testName() {
        String queryString = "match $x isa person, has firstname $n;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        assertEquals(answers.size(), 60);
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
    }

    @Test
    public void testMiddleName() {
        String queryString = "match $x has identifier $i has middlename $mn;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        assertEquals(answers.size(), 60);
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
    }

    @Test
    public void testSurName() {
        String queryString = "match $x isa person has surname $srn;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        assertEquals(answers.size(), 60);
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
    }

    @Test
    public void testParentship() {
        String queryString = "match (child: $c, parent: $p) isa parentship;";
        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 76);
        assertTrue(!hasDuplicates(answers));
        answers.forEach(answer -> assertEquals(answer.size(), 2));
    }

    @Test
    public void testParentship2() {
        String queryString = "match (child: $x, $y) isa parentship;select $x;";
        String queryString2 = "match (child: $x) isa parentship;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        QueryAnswers answers2 = queryAnswers(iqb.parse(queryString2));
        answers.forEach(answer -> assertEquals(answer.size(), 1));
        assertEquals(answers, answers2);
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
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
    public void testMarriedToThemselves(){
        String queryString = "match (spouse2: $x, spouse1: $x) isa marriage;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        assertTrue(answers.isEmpty());
    }

    @Test
    public void testMarriageType() {
        String queryString = "match $x isa marriage;";
        String queryString2 = "match $x($x1, $x2) isa marriage;select $x;";
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);

        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers2.size(), answers.size());
        assertEquals(answers2.size(), 66);
    }

    @Test
    public void testMarriageType2() {
        String queryString = "match $x isa marriage;";
        String queryString2 = "match $x($x1, $x2) isa marriage;select $x;";
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);

        String qs = "match ($x, $y) isa marriage; ($y, $z) isa marriage;";
        iqb.parse(qs).execute();

        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers2.size(), answers.size());
        assertEquals(answers2.size(), 66);
    }

    @Test
    public void testMarriageMaterialisation() {
        String queryString = "match $rel ($x, $y) isa marriage;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        QueryAnswers answers2 = queryAnswers(qb.parse(queryString));
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers.size(), answers2.size());
        assertEquals(answers.size(), 132);
    }

    //Bug #11149
    @Test
    public void testMarriageMaterialisation2() {
        String queryString = "match $rel (spouse1: $x, spouse2: $y) isa marriage;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        QueryAnswers answers2 = queryAnswers(qb.parse(queryString));
        assertEquals(answers.size(), 44);
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers, answers2);
    }

    //2 relations per wife-husband pair - (spouse1: $x, spouse2 :$y) and (spouse1: $y, spouse2: $x)
    //wife and husband roles do not sub spouse1, spouse2
    @Test
    public void testMarriage() {
        String queryString = "match (spouse1: $x, spouse2: $y) isa marriage;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        QueryAnswers answers2 = queryAnswers(qb.parse(queryString));
        assertEquals(answers.size(), 44);
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers, answers2);
    }

    @Test
    public void testMarriage2() {
        String queryString = "match (wife: $x, husband: $y) isa marriage;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        QueryAnswers answers2 = queryAnswers(qb.parse(queryString));
        assertEquals(answers.size(), 22);
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
        String queryString = "match ($r: $x) isa marriage;$r label 'wife';select $x;";
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
        MatchQuery query = iqb.materialise(true).parse(queryString);

        QueryAnswers answers = new QueryAnswers(query.admin().streamWithAnswers().collect(Collectors.toSet()));
        assertEquals(answers.size(), 166);
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
    }

    @Test
    public void testCousins() {
        String queryString = "match ($x, $y) isa cousins;";
        MatchQuery query = iqb.parse(queryString);

        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 192);
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
    }

    @Test
    public void testInLaws() {
        String queryString = "match $x(parent-in-law: $x1, child-in-law: $x2) isa in-laws;";
        MatchQuery query = iqb.parse(queryString);

        QueryAnswers answers = queryAnswers(query);
        QueryAnswers requeriedAnswers = queryAnswers(query);
        assertEquals(answers.size(), 50);
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
        QueryAnswers answers3 = queryAnswers(query3);
        QueryAnswers answers4 = queryAnswers(query4);

        assertEquals(answers.size(), 22);
        assertEquals(answers2.size(), 92);
        assertEquals(answers3.size(), 50);
        assertEquals(answers3.size(), answers4.size());
    }

    @Test
    public void testMotherInLaw() {
        String queryString = "match (mother-in-law: $x);$x has gender $g;";
        String queryString2 = "match (parent-in-law: $x, child-in-law: $y) isa in-laws;$x has gender $g;$g val 'female'; select $x, $g;";
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers.size(), 8);
        assertEquals(answers, answers2);
        assertTrue(checkResource(answers, "g", "female"));
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
    }

    @Test
    public void testFatherInLaw() {
        String queryString = "match (father-in-law: $x);$x has gender $g;";
        String queryString2 = "match (parent-in-law: $x, child-in-law: $y) isa in-laws;$x has gender $g;$g val'male'; select $x, $g;";
        MatchQuery query = iqb.parse(queryString);
        MatchQuery query2 = iqb.parse(queryString2);
        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers.size(), 9);
        assertEquals(answers, answers2);
        assertTrue(checkResource(answers, "g", "male"));
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
    }

    @Test
    public void testSonInLaw() {
        String queryString = "match (son-in-law: $x);$x has gender $g;";
        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 11);
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
    }

    @Test
    public void testDaughterInLaw() {
        String queryString = "match (daughter-in-law: $x); $x has identifier $id;";
        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 14);
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
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
        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 18);
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
        assertTrue(checkResource(answers, "g", "male"));
    }

    @Test
    public void testDaughter() {
        String queryString = "match (daughter: $x);$x has gender $g;";
        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 20);
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
        assertTrue(checkResource(answers, "g", "female"));
    }

    @Test
    public void testChild() {
        String queryString = "match (child: $x);";
        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 38);
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
    }

    @Test
    public void testFather() {
        String queryString = "match (father: $x);";
        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 10);
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
    }

    @Test
    public void testMother() {
        String queryString = "match (mother: $x);";
        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 9);
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
    }

    @Test
    public void testParent() {
        String queryString = "match (parent: $x);";
        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 19);
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
    }

    @Test
    public void testFemaleFather() {
        String queryString = "match (father: $x) isa parentship; $x has gender $g; $g val 'female';";
        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 =  queryAnswers(genealogyGraph.graph().graql().infer(true).materialise(true).parse(queryString));
        assertTrue(answers.isEmpty());
        assertEquals(answers, answers2);
    }

    @Test
    public void testGrandMother() {
        String queryString = "match (grandmother: $x) isa grandparentship; $x has gender $g;";
        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 4);
        assertTrue(checkResource(answers, "g", "female"));
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
    }

    @Test
    public void testGrandDaughter(){
        String queryString = "match (granddaughter: $x); $x has gender $g;";
        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 18);
        assertTrue(checkResource(answers, "g", "female"));
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
    }

    @Test
    public void testGrandParentship(){
        String queryString = "match "+
                "(grandchild: $x); (granddaughter: $x);$x has gender $g;";
        MatchQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 18);
        assertTrue(checkResource(answers, "g", "female"));
        assertEquals(answers, queryAnswers(qb.<MatchQueryAdmin>parse(queryString)));
    }

    private boolean checkResource(QueryAnswers answers, String var, String value){
        boolean isOk = true;
        Iterator<Answer> it =  answers.iterator();
        while (it.hasNext() && isOk){
            Concept c = it.next().get(VarName.of(var));
            isOk = c.asResource().getValue().equals(value);
        }
        return isOk;
    }

    private boolean hasDuplicates(QueryAnswers answers){
        boolean hasDuplicates = false;
        Iterator<Answer> it = answers.iterator();
        while(it.hasNext() && !hasDuplicates){
            Answer answer = it.next();
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
        return new QueryAnswers(query.admin().streamWithVarNames().map(QueryAnswer::new).collect(toSet()));
    }
}
