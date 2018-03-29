/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.test.kbs.GenealogyKB;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static ai.grakn.util.GraqlTestUtil.assertCollectionsEqual;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.Matchers.empty;

public class GenealogyTest {

    private static QueryBuilder qb;
    private static QueryBuilder iqb;

    @ClassRule
    public static final SampleKBContext genealogyKB = GenealogyKB.context();

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
    public void matchAllRelationsWithDocumentPlayingARole(){
        String queryString = "match $x isa document; ($x, $y); get;";
        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.toList();
        assertTrue(answers.isEmpty());
    }

    @Test
    public void matchAllRelationsWithDocumentPlayingARole_redundantEntityBound(){
        String queryString = "match $x isa document; ($x, $y); $y isa entity; get;";
        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.toList();
        assertThat(answers, empty());
    }

    @Test
    public void genderOfASpecificPerson(){
        Concept concept = Sets.newHashSet(genealogyKB.tx().graql().infer(false).<GetQuery>parse("match $x isa person; get;"))
                .iterator().next()
                .entrySet()
                .iterator().next().getValue();
        String queryString = "match $x id '" + concept.getId() + "' has gender $g; get;";

        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.toList();
        assertEquals(answers.size(), 1);
    }

    @Test
    public void testSpecificGender() {
        String queryString = "match $x isa person has gender 'female'; get;";
        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.toList();
        assertEquals(answers.size(), 32);
        assertCollectionsEqual(answers, qb.<GetQuery>parse(queryString).toList());
    }

    @Test
    public void testGender() {
        String queryString = "match $x isa person has gender $gender; get;";
        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.toList();
        assertCollectionsEqual(answers, qb.<GetQuery>parse(queryString).toList());
        assertEquals(answers.size(), qb.<GetQuery>parse("match $x isa person; get;").toList().size());
    }

    @Test
    public void testName() {
        String queryString = "match $x isa person, has firstname $n; get;";
        List<Answer> answers = iqb.<GetQuery>parse(queryString).toList();
        assertEquals(answers.size(), 60);
        assertCollectionsEqual(answers, qb.<GetQuery>parse(queryString).toList());
    }

    @Test
    public void testMiddleName() {
        String queryString = "match $x has identifier $i has middlename $mn; get;";
        List<Answer> answers = iqb.<GetQuery>parse(queryString).toList();
        assertEquals(answers.size(), 60);
        assertCollectionsEqual(answers, qb.<GetQuery>parse(queryString).toList());
    }

    @Test
    public void testSurName() {
        String queryString = "match $x isa person has surname $srn; get;";
        List<Answer> answers = iqb.<GetQuery>parse(queryString).toList();
        assertEquals(answers.size(), 60);
        assertCollectionsEqual(answers, qb.<GetQuery>parse(queryString).toList());
    }

    @Test
    public void testParentship() {
        String queryString = "match (child: $c, parent: $p) isa parentship; get;";
        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.toList();
        assertEquals(answers.size(), 76);
        assertTrue(!hasDuplicates(answers));
        answers.forEach(answer -> assertEquals(answer.size(), 2));
    }

    @Test
    public void testParentship_DifferentRoleMappingConfigurations() {
        String queryString = "match (child: $x, $y) isa parentship;get $x;";
        String queryString2 = "match (child: $x) isa parentship; get;";
        String queryString3 = "match ($x, son: $y) isa parentship; get;";
        List<Answer> answers = iqb.<GetQuery>parse(queryString).toList();
        List<Answer> answers2 = iqb.<GetQuery>parse(queryString2).toList();
        List<Answer> answers3 = iqb.<GetQuery>parse(queryString3).toList();
        answers.forEach(answer -> assertEquals(answer.size(), 1));
        assertCollectionsEqual(answers, answers2);
        assertCollectionsEqual(answers, qb.<GetQuery>parse(queryString).toList());
        assertThat(answers3, empty());
    }

    @Test
    public void peopleSharingHusbandOfSpecificPerson(){
        String queryString = "match " +
                "$a has firstname 'Ann' has surname 'Niesz';" +
                "(wife: $a, husband: $w); " +
                "(husband: $w, wife: $b);" +
                "$a != $b;" +
                "get;";
        List<Answer> answers = iqb.<GetQuery>parse(queryString).toList();
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers.size(), 1);
    }

    @Test
    public void peopleSharingSpouse(){
        String queryString = "match " +
                    "($x, $y) isa marriage;" +
                    "($y, $z) isa marriage;" +
                    "$x != $z;" +
                     "get;";
        List<Answer> answers = iqb.<GetQuery>parse(queryString).toList();
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers.size(), 4);
    }

    @Test
    public void marriedToThemselves(){
        String queryString = "match (spouse: $x, spouse: $x) isa marriage; get;";
        List<Answer> answers = iqb.<GetQuery>parse(queryString).toList();
        assertThat(answers, empty());
    }

    @Test
    public void testMarriage_definedViaTypeAndRelationVariable() {
        String queryString = "match $x isa marriage; get;";
        String queryString2 = "match $x($x1, $x2) isa marriage;get $x;";
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);

        //requery
        String qs = "match ($x, $y) isa marriage; ($y, $z) isa marriage; get;";
        iqb.parse(qs).execute();

        List<Answer> answers = query.toList();
        assertEquals(answers.size(), 66);
        List<Answer> answers2 = query2.toList();
        assertCollectionsEqual(answers, answers2);
        assertEquals(answers2.size(), 66);
    }

    //Bug #11149
    @Test
    public void testMarriageMaterialisation() {
        String queryString = "match $rel (spouse: $x, spouse: $y) isa marriage; get;";
        List<Answer> answers = iqb.<GetQuery>parse(queryString).toList();
        List<Answer> answers2 = qb.<GetQuery>parse(queryString).toList();
        assertEquals(132, answers.size());
        assertTrue(!hasDuplicates(answers));
        assertCollectionsEqual(answers, answers2);
    }

    //2 relations per wife-husband pair - (spouse: $x, spouse :$y) and (spouse: $y, spouse: $x)
    //wife and husband roles do not sub spouse, spouse
    @Test
    public void testMarriage() {
        String queryString = "match (spouse: $x, spouse: $y) isa marriage; get;";
        List<Answer> answers = iqb.<GetQuery>parse(queryString).toList();
        List<Answer> answers2 = qb.<GetQuery>parse(queryString).toList();
        assertEquals(answers.size(), 44);
        assertTrue(!hasDuplicates(answers));
        assertCollectionsEqual(answers, answers2);
    }

    @Test
    public void testMarriage_specialisedRoles() {
        String queryString = "match (wife: $x, husband: $y) isa marriage; get;";
        List<Answer> answers = iqb.<GetQuery>parse(queryString).toList();
        List<Answer> answers2 = qb.<GetQuery>parse(queryString).toList();
        assertEquals(answers.size(), 22);
        assertTrue(!hasDuplicates(answers));
        assertCollectionsEqual(answers, answers2);
    }

    @Test
    public void testWife(){
        String queryString = "match $r (wife: $x) isa marriage; get;";
        List<Answer> answers = iqb.<GetQuery>parse(queryString).toList();
        List<Answer> answerList = qb.<GetQuery>parse(queryString).toList();
        List<Answer> requeriedAnswers = iqb.<GetQuery>parse(queryString).toList();
        assertCollectionsEqual(answers, requeriedAnswers);
        List<Answer> answerList2 = qb.<GetQuery>parse(queryString).toList();
        assertCollectionsEqual(answerList, answerList2);
    }

    @Test
    public void testWife_definedViaRoleVariable(){
        String queryString = "match ($r: $x) isa marriage;$r label 'wife';get $x;";
        String queryString2 = "match (wife: $x) isa marriage; get;";
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        List<Answer> answers2 = query2.toList();
        assertCollectionsEqual(answers2, qb.<GetQuery>parse(queryString2).toList());
        List<Answer> answers = query.toList();
        assertTrue(!answers.isEmpty());
        assertCollectionsEqual(answers, answers2);
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

        List<Answer> answers = query.toList();
        assertEquals(answers.size(), 166);
        assertTrue(!hasDuplicates(answers));
        assertCollectionsEqual(answers, qb.<GetQuery>parse(queryString).toList());
    }

    @Test
    public void testCousins() {
        String queryString = "match ($x, $y) isa cousins; get;";
        GetQuery query = iqb.parse(queryString);

        List<Answer> answers = query.toList();
        assertEquals(answers.size(), 192);
        assertTrue(!hasDuplicates(answers));
        assertCollectionsEqual(answers, qb.<GetQuery>parse(queryString).toList());
    }

    @Test
    public void testInLaws() {
        String queryString = "match (parent-in-law: $x, child-in-law: $y) isa in-laws;$y has gender 'male'; get;";
        String queryString2 = "match $x isa in-laws; get;";
        String queryString3 = "match (parent-in-law: $x, child-in-law: $y) isa in-laws; get;";
        String queryString4 = "match $x(parent-in-law: $x1, child-in-law: $x2) isa in-laws; get;";
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        GetQuery query3 = iqb.parse(queryString3);
        GetQuery query4 = iqb.parse(queryString4);

        List<Answer> answers = query.toList();
        List<Answer> answers2 = query2.toList();
        List<Answer> answers3 = query3.toList();
        List<Answer> answers4 = query4.toList();

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
        List<Answer> answers = query.toList();
        List<Answer> answers2 = query2.toList();
        assertEquals(answers.size(), 8);
        assertCollectionsEqual(answers, answers2);
        assertTrue(checkResource(answers, "g", "female"));
        assertCollectionsEqual(answers, qb.<GetQuery>parse(queryString).toList());
    }

    @Test
    public void testFatherInLaw() {
        String queryString = "match (father-in-law: $x);$x has gender $g; get;";
        String queryString2 = "match (parent-in-law: $x, child-in-law: $y) isa in-laws;$x has gender $g;$g val'male'; get $x, $g;";
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        List<Answer> answers = query.toList();
        List<Answer> answers2 = query2.toList();
        assertEquals(answers.size(), 9);
        assertCollectionsEqual(answers, answers2);
        assertTrue(checkResource(answers, "g", "male"));
        assertCollectionsEqual(answers, qb.<GetQuery>parse(queryString).toList());
    }

    @Test
    public void testSonInLaw() {
        String queryString = "match (son-in-law: $x);$x has gender $g; get;";
        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.toList();
        assertEquals(answers.size(), 11);
        assertCollectionsEqual(answers, qb.<GetQuery>parse(queryString).toList());
    }

    @Test
    public void testDaughterInLaw() {
        String queryString = "match (daughter-in-law: $x); $x has identifier $id; get;";
        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.toList();
        assertEquals(answers.size(), 14);
        assertCollectionsEqual(answers, qb.<GetQuery>parse(queryString).toList());
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
        List<Answer> answers = query.toList();
        assertEquals(answers.size(), 18);
        assertCollectionsEqual(answers, qb.<GetQuery>parse(queryString).toList());
        assertTrue(checkResource(answers, "g", "male"));
    }

    @Test
    public void testDaughter() {
        String queryString = "match (daughter: $x);$x has gender $g; get;";
        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.toList();
        assertEquals(answers.size(), 20);
        assertCollectionsEqual(answers, qb.<GetQuery>parse(queryString).toList());
        assertTrue(checkResource(answers, "g", "female"));
    }

    @Test
    public void testChild() {
        String queryString = "match (child: $x); get;";
        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.toList();
        assertEquals(answers.size(), 38);
        assertCollectionsEqual(answers, qb.<GetQuery>parse(queryString).toList());
    }

    @Test
    public void testFather() {
        String queryString = "match (father: $x); get;";
        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.toList();
        assertEquals(answers.size(), 10);
        assertCollectionsEqual(answers, qb.<GetQuery>parse(queryString).toList());
    }

    @Test
    public void testMother() {
        String queryString = "match (mother: $x); get;";
        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.toList();
        assertEquals(answers.size(), 9);
        assertCollectionsEqual(answers, qb.<GetQuery>parse(queryString).toList());
    }

    @Test
    public void testParent() {
        String queryString = "match (parent: $x); get;";
        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.toList();
        assertEquals(answers.size(), 19);
        assertCollectionsEqual(answers, qb.<GetQuery>parse(queryString).toList());
    }

    @Test
    public void testFemaleFather() {
        String queryString = "match (father: $x) isa parentship; $x has gender $g; $g val 'female'; get;";
        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.toList();
        List<Answer> answers2 = genealogyKB.tx().graql().infer(true).materialise(true).<GetQuery>parse(queryString).toList();
        assertThat(answers, empty());
        assertThat(answers2, empty());
    }

    @Test
    public void testGrandMother() {
        String queryString = "match (grandmother: $x) isa grandparentship; $x has gender $g; get;";
        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.toList();
        assertEquals(answers.size(), 4);
        assertTrue(checkResource(answers, "g", "female"));
        assertCollectionsEqual(answers, qb.<GetQuery>parse(queryString).toList());
    }

    @Test
    public void testGrandDaughter(){
        String queryString = "match (granddaughter: $x); $x has gender $g; get;";
        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.toList();
        assertEquals(answers.size(), 18);
        assertTrue(checkResource(answers, "g", "female"));
        assertCollectionsEqual(answers, qb.<GetQuery>parse(queryString).toList());
    }

    @Test
    public void testGrandParentship(){
        String queryString = "match "+
                "(grandchild: $x); (granddaughter: $x);$x has gender $g; get;";
        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.toList();
        assertEquals(answers.size(), 18);
        assertTrue(checkResource(answers, "g", "female"));
        assertCollectionsEqual(answers, qb.<GetQuery>parse(queryString).toList());
    }

    private boolean checkResource(List<Answer> answers, String var, String value){
        boolean isOk = true;
        Iterator<Answer> it =  answers.iterator();
        while (it.hasNext() && isOk){
            Concept c = it.next().get(Graql.var(var));
            isOk = c.asAttribute().getValue().equals(value);
        }
        return isOk;
    }

    private boolean hasDuplicates(List<Answer> answers){
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
}
