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

package ai.grakn.graql.internal.reasoner.inference;

import ai.grakn.concept.Concept;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.test.SampleKBContext;
import ai.grakn.test.kbs.GenealogyKB;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GenealogyTest {

    private static QueryBuilder qb;
    private static QueryBuilder iqb;

    @ClassRule
    public static final SampleKBContext genealogyKB = SampleKBContext.preLoad(GenealogyKB.get());

    @BeforeClass
    public static void setUpClass() throws Exception {
        qb = genealogyKB.tx().graql().infer(false);
        iqb = genealogyKB.tx().graql().infer(true).materialise(true);
    }

    /*
    test for first rule file:
    match $x isa person has first-name $name;
    match (child: $x, parent: $y) isa parentship;
    match (spouse: $x, spouse: $y) isa marriage;
    */

    @Test
    public void testMatchAll(){
        String queryString = "match $x isa document; ($x, $y); get;";
        GetQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertTrue(answers.isEmpty());
    }

    @Test
    public void testMatchAll2(){
        String queryString = "match $x isa document; ($x, $y); $y isa entity; get;";
        GetQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertTrue(answers.isEmpty());
    }

    @Test
    public void testSpecificPerson(){
        Concept concept = Sets.newHashSet(genealogyKB.tx().graql().infer(false).<GetQuery>parse("match $x isa person; get;"))
                .iterator().next()
                .entrySet().iterator().next().getValue();
        String queryString = "match $x id '" + concept.getId() + "' has gender $g; get;";

        GetQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 1);
    }

    @Test
    public void testFemale() {
        String queryString = "match $x isa person has gender 'female'; get;";
        GetQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 32);
        assertEquals(answers, queryAnswers(qb.<GetQuery>parse(queryString)));
    }

    @Test
    public void testGender() {
        String queryString = "match $x isa person has gender $gender; get;";
        GetQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers, queryAnswers(qb.<GetQuery>parse(queryString)));
        assertEquals(answers.size(), qb.<GetQuery>parse("match $x isa person; get;").execute().size());
    }

    @Test
    public void testName() {
        String queryString = "match $x isa person, has firstname $n; get;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        assertEquals(answers.size(), 60);
        assertEquals(answers, queryAnswers(qb.<GetQuery>parse(queryString)));
    }

    @Test
    public void testMiddleName() {
        String queryString = "match $x has identifier $i has middlename $mn; get;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        assertEquals(answers.size(), 60);
        assertEquals(answers, queryAnswers(qb.<GetQuery>parse(queryString)));
    }

    @Test
    public void testSurName() {
        String queryString = "match $x isa person has surname $srn; get;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        assertEquals(answers.size(), 60);
        assertEquals(answers, queryAnswers(qb.<GetQuery>parse(queryString)));
    }

    @Test
    public void testParentship() {
        String queryString = "match (child: $c, parent: $p) isa parentship; get;";
        GetQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 76);
        assertTrue(!hasDuplicates(answers));
        answers.forEach(answer -> assertEquals(answer.size(), 2));
    }

    @Test
    public void testParentship2() {
        String queryString = "match (child: $x, $y) isa parentship;get $x;";
        String queryString2 = "match (child: $x) isa parentship; get;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        QueryAnswers answers2 = queryAnswers(iqb.parse(queryString2));
        answers.forEach(answer -> assertEquals(answer.size(), 1));
        assertEquals(answers, answers2);
        assertEquals(answers, queryAnswers(qb.<GetQuery>parse(queryString)));
    }

    @Test
    public void testParentship3(){
        String queryString = "match ($x, son: $y) isa parentship; get;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        assertTrue(answers.isEmpty());
    }

    @Test
    public void testComplexQuery(){
        String queryString = "match $a has firstname 'Ann' has surname 'Niesz';" +
                "(wife: $a, husband: $w); (husband: $w, wife: $b) isa marriage;$a != $b; get;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers.size(), 1);
    }

    @Test
    public void testMarriageNotEquals(){
        String queryString = "match ($x, $y) isa marriage; ($y, $z) isa marriage;$x != $z; get;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers.size(), 4);
    }

    //@Ignore
    @Test
    public void testMarriedToThemselves(){
        String queryString = "match (spouse: $x, spouse: $x) isa marriage; get;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        assertTrue(answers.isEmpty());
    }

    @Test
    public void testMarriageType() {
        String queryString = "match $x isa marriage; get;";
        String queryString2 = "match $x($x1, $x2) isa marriage;get $x;";
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);

        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 66);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers2.size(), answers.size());
        assertEquals(answers2.size(), 66);
    }

    @Test
    public void testMarriageType2() {
        String queryString = "match $x isa marriage; get;";
        String queryString2 = "match $x($x1, $x2) isa marriage;get $x;";
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);

        String qs = "match ($x, $y) isa marriage; ($y, $z) isa marriage; get;";
        iqb.parse(qs).execute();

        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers2.size(), answers.size());
        assertEquals(answers2.size(), 66);
    }

    @Test
    public void testMarriageMaterialisation() {
        String queryString = "match $rel ($x, $y) isa marriage; get;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        QueryAnswers answers2 = queryAnswers(qb.parse(queryString));
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers.size(), answers2.size());
        assertEquals(answers.size(), 132);
    }

    //Bug #11149
    @Ignore // TODO: Look into this before merging into master! probably related to `wife/husband` being `sub spouse`
    @Test
    public void testMarriageMaterialisation2() {
        String queryString = "match $rel (spouse: $x, spouse: $y) isa marriage; get;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        QueryAnswers answers2 = queryAnswers(qb.parse(queryString));
        assertEquals(44, answers.size());
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers, answers2);
    }

    //2 relations per wife-husband pair - (spouse: $x, spouse :$y) and (spouse: $y, spouse: $x)
    //wife and husband roles do not sub spouse, spouse
    @Test
    public void testMarriage() {
        String queryString = "match (spouse: $x, spouse: $y) isa marriage; get;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        QueryAnswers answers2 = queryAnswers(qb.parse(queryString));
        assertEquals(answers.size(), 44);
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers, answers2);
    }

    @Test
    public void testMarriage2() {
        String queryString = "match (wife: $x, husband: $y) isa marriage; get;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        QueryAnswers answers2 = queryAnswers(qb.parse(queryString));
        assertEquals(answers.size(), 22);
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers, answers2);
    }

    @Test
    public void testWife(){
        String queryString = "match $r (wife: $x) isa marriage; get;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        List<Answer> answerList = qb.<GetQuery>parse(queryString).execute();
        QueryAnswers requeriedAnswers = queryAnswers(iqb.parse(queryString));
        assertEquals(answers, requeriedAnswers);
        List<Answer> answerList2 = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answerList, answerList2);
    }

    //TODO
    @Ignore
    @Test
    public void testWife2(){
        String queryString = "match ($r: $x) isa marriage;$r label 'wife';get $x;";
        String queryString2 = "match (wife: $x) isa marriage; get;";
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers2, queryAnswers(qb.<GetQuery>parse(queryString2)));
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
        String queryString = "match (sibling:$x, sibling:$y) isa siblings; get;";
        GetQuery query = iqb.materialise(true).parse(queryString);

        QueryAnswers answers = new QueryAnswers(query.stream().collect(Collectors.toSet()));
        assertEquals(answers.size(), 166);
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers, queryAnswers(qb.<GetQuery>parse(queryString)));
    }

    @Test
    public void testCousins() {
        String queryString = "match ($x, $y) isa cousins; get;";
        GetQuery query = iqb.parse(queryString);

        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 192);
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers, queryAnswers(qb.<GetQuery>parse(queryString)));
    }

    @Test
    public void testInLaws() {
        String queryString = "match $x(parent-in-law: $x1, child-in-law: $x2) isa in-laws; get;";
        GetQuery query = iqb.parse(queryString);

        QueryAnswers answers = queryAnswers(query);
        QueryAnswers requeriedAnswers = queryAnswers(query);
        assertEquals(answers.size(), 50);
        assertEquals(answers.size(), requeriedAnswers.size());
    }

    @Test
    public void testInLaws2() {
        String queryString = "match (parent-in-law: $x, child-in-law: $y) isa in-laws;$y has gender 'male'; get;";
        String queryString2 = "match $x isa in-laws; get;";
        String queryString3 = "match (parent-in-law: $x, child-in-law: $y) isa in-laws; get;";
        String queryString4 = "match $x(parent-in-law: $x1, child-in-law: $x2) isa in-laws; get;";
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        GetQuery query3 = iqb.parse(queryString3);
        GetQuery query4 = iqb.parse(queryString4);

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
        String queryString = "match (mother-in-law: $x);$x has gender $g; get;";
        String queryString2 = "match (parent-in-law: $x, child-in-law: $y) isa in-laws;$x has gender $g;$g val 'female'; get $x, $g;";
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers.size(), 8);
        assertEquals(answers, answers2);
        assertTrue(checkResource(answers, "g", "female"));
        assertEquals(answers, queryAnswers(qb.<GetQuery>parse(queryString)));
    }

    @Test
    public void testFatherInLaw() {
        String queryString = "match (father-in-law: $x);$x has gender $g; get;";
        String queryString2 = "match (parent-in-law: $x, child-in-law: $y) isa in-laws;$x has gender $g;$g val'male'; get $x, $g;";
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 = queryAnswers(query2);
        assertEquals(answers.size(), 9);
        assertEquals(answers, answers2);
        assertTrue(checkResource(answers, "g", "male"));
        assertEquals(answers, queryAnswers(qb.<GetQuery>parse(queryString)));
    }

    @Test
    public void testSonInLaw() {
        String queryString = "match (son-in-law: $x);$x has gender $g; get;";
        GetQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 11);
        assertEquals(answers, queryAnswers(qb.<GetQuery>parse(queryString)));
    }

    @Test
    public void testDaughterInLaw() {
        String queryString = "match (daughter-in-law: $x); $x has identifier $id; get;";
        GetQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 14);
        assertEquals(answers, queryAnswers(qb.<GetQuery>parse(queryString)));
    }

    /*
    test for the last rule-file:
    match (mother: $x);
    match (son: $x);
    match (grandfather: $x); (grandparent: $x, grandchild: $y);
    */

    @Test
    public void testSon() {
        String queryString = "match (son: $x);$x has gender $g; get;";
        GetQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 18);
        assertEquals(answers, queryAnswers(qb.<GetQuery>parse(queryString)));
        assertTrue(checkResource(answers, "g", "male"));
    }

    @Test
    public void testDaughter() {
        String queryString = "match (daughter: $x);$x has gender $g; get;";
        GetQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 20);
        assertEquals(answers, queryAnswers(qb.<GetQuery>parse(queryString)));
        assertTrue(checkResource(answers, "g", "female"));
    }

    @Test
    public void testChild() {
        String queryString = "match (child: $x); get;";
        GetQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 38);
        assertEquals(answers, queryAnswers(qb.<GetQuery>parse(queryString)));
    }

    @Test
    public void testFather() {
        String queryString = "match (father: $x); get;";
        GetQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 10);
        assertEquals(answers, queryAnswers(qb.<GetQuery>parse(queryString)));
    }

    @Test
    public void testMother() {
        String queryString = "match (mother: $x); get;";
        GetQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 9);
        assertEquals(answers, queryAnswers(qb.<GetQuery>parse(queryString)));
    }

    @Test
    public void testParent() {
        String queryString = "match (parent: $x); get;";
        GetQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 19);
        assertEquals(answers, queryAnswers(qb.<GetQuery>parse(queryString)));
    }

    @Test
    public void testFemaleFather() {
        String queryString = "match (father: $x) isa parentship; $x has gender $g; $g val 'female'; get;";
        GetQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        QueryAnswers answers2 =  queryAnswers(genealogyKB.tx().graql().infer(true).materialise(true).parse(queryString));
        assertTrue(answers.isEmpty());
        assertEquals(answers, answers2);
    }

    @Test
    public void testGrandMother() {
        String queryString = "match (grandmother: $x) isa grandparentship; $x has gender $g; get;";
        GetQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 4);
        assertTrue(checkResource(answers, "g", "female"));
        assertEquals(answers, queryAnswers(qb.<GetQuery>parse(queryString)));
    }

    @Test
    public void testGrandDaughter(){
        String queryString = "match (granddaughter: $x); $x has gender $g; get;";
        GetQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 18);
        assertTrue(checkResource(answers, "g", "female"));
        assertEquals(answers, queryAnswers(qb.<GetQuery>parse(queryString)));
    }

    @Test
    public void testGrandParentship(){
        String queryString = "match "+
                "(grandchild: $x); (granddaughter: $x);$x has gender $g; get;";
        GetQuery query = iqb.parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        assertEquals(answers.size(), 18);
        assertTrue(checkResource(answers, "g", "female"));
        assertEquals(answers, queryAnswers(qb.<GetQuery>parse(queryString)));
    }

    private boolean checkResource(QueryAnswers answers, String var, String value){
        boolean isOk = true;
        Iterator<Answer> it =  answers.iterator();
        while (it.hasNext() && isOk){
            Concept c = it.next().get(Graql.var(var));
            isOk = c.asAttribute().getValue().equals(value);
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

    private QueryAnswers queryAnswers(GetQuery query) {
        return new QueryAnswers(query.stream().collect(toSet()));
    }
}
