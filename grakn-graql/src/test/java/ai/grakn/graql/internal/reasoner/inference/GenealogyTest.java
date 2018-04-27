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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.inference;

import ai.grakn.concept.Concept;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.reasoner.utils.ReasonerUtils;
import ai.grakn.test.rule.SampleKBContext;
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

import static ai.grakn.util.GraqlTestUtil.assertCollectionsEqual;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.Matchers.empty;

public class GenealogyTest {

    private static QueryBuilder iqb;

    private final int noOfPeople = 60;
    private final int noOfMarriages = 22;
    private final int noOfParents = 19;
    private final int noOfChildren = 38;

    //2 relations per wife-husband pair - (spouse: $x, spouse :$y) and (spouse: $y, spouse: $x)
    //wife and husband roles do not sub spouse, spouse
    private final int noOfMarriageRelations = 2 * noOfMarriages;

    @ClassRule
    public static final SampleKBContext genealogyKB = GenealogyKB.context();

    @BeforeClass
    public static void setUpClass() throws Exception {
        iqb = genealogyKB.tx().graql().infer(true);
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
        List<Answer> answers = query.execute();
        assertTrue(answers.isEmpty());
    }

    @Test
    public void matchAllRelationsWithDocumentPlayingARole_redundantEntityBound(){
        String queryString = "match $x isa document; ($x, $y); $y isa entity; get;";
        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.execute();
        assertThat(answers, empty());
    }

    @Test
    public void testGeneratedGenders() {
        String specificGender = "match $x isa person has gender 'female'; get;";
        String generalGender = "match $x isa person has gender $gender; get;";

        Concept concept = Sets.newHashSet(genealogyKB.tx().graql().infer(false).<GetQuery>parse("match $x isa person; get;"))
                .iterator().next()
                .entrySet()
                .iterator().next().getValue();
        String genderOfSpecificPerson = "match $x id '" + concept.getId() + "' has gender $g; get;";

        List<Answer> females = iqb.<GetQuery>parse(specificGender).execute();
        List<Answer> allPeople = iqb.<GetQuery>parse(generalGender).execute();
        List<Answer> specificPerson = iqb.<GetQuery>parse(genderOfSpecificPerson).execute();

        assertEquals(specificPerson.size(), 1);
        assertEquals(females.size(), 32);
        assertEquals(allPeople.size(), noOfPeople);
    }

    @Test
    public void testGeneratedNames() {
        String firstnameString = "match $x isa person, has firstname $n; get;";
        String middlenameString = "match $x has identifier $i has middlename $mn; get;";
        String surnameString = "match $x isa person has surname $srn; get;";
        List<Answer> firstnameAnswers = iqb.<GetQuery>parse(firstnameString).execute();
        List<Answer> middlenameAnswers = iqb.<GetQuery>parse(middlenameString).execute();
        List<Answer> surnameAnswers = iqb.<GetQuery>parse(surnameString).execute();
        assertEquals(firstnameAnswers.size(), noOfPeople);
        assertEquals(middlenameAnswers.size(), noOfPeople);
        assertEquals(surnameAnswers.size(), noOfPeople);
    }

    @Test
    public void testParentship() {
        String queryString = "match (child: $c, parent: $p) isa parentship; get;";
        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.execute();
        assertEquals(answers.size(), 76);
        assertTrue(!hasDuplicates(answers));
        answers.forEach(answer -> assertEquals(answer.size(), 2));
    }

    @Test
    public void testParentship_DifferentRoleMappingConfigurations() {
        String queryString = "match (child: $x, $y) isa parentship;get $x;";
        String queryString2 = "match (child: $x) isa parentship; get;";
        String queryString3 = "match ($x, son: $y) isa parentship; get;";
        List<Answer> answers = iqb.<GetQuery>parse(queryString).execute();
        List<Answer> answers2 = iqb.<GetQuery>parse(queryString2).execute();
        List<Answer> answers3 = iqb.<GetQuery>parse(queryString3).execute();
        answers.forEach(answer -> assertEquals(answer.size(), 1));
        assertCollectionsEqual(answers, answers2);
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
        List<Answer> answers = iqb.<GetQuery>parse(queryString).execute();
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
        List<Answer> answers = iqb.<GetQuery>parse(queryString).execute();
        assertTrue(!hasDuplicates(answers));
        assertEquals(answers.size(), 4);
    }

    @Test
    public void peopleMarriedToThemselves(){
        String queryString = "match (spouse: $x, spouse: $x) isa marriage; get;";
        List<Answer> answers = iqb.<GetQuery>parse(queryString).execute();
        assertThat(answers, empty());
    }

    @Test
    public void testDifferentMarriageVariants() {
        String symmetricRoles= "match (spouse: $x, spouse: $y) isa marriage; get;";
        String specialisedRoles = "match (wife: $x, husband: $y) isa marriage; get;";

        String relationInstances = "match $x isa marriage; get;";
        String relationInstancesWithRPs = "match $x ($x1, $x2) isa marriage;get;";
        String relationInstancesWithRPsandRoles = "match $rel (spouse: $x, spouse: $y) isa marriage; get;";

        //requery
        String chainedMarriages = "match ($x, $y) isa marriage; ($y, $z) isa marriage; get;";
        iqb.parse(chainedMarriages).execute();

        List<Answer> spousePairs = iqb.<GetQuery>parse(symmetricRoles).execute();
        List<Answer> wifeHusbandPairs = iqb.<GetQuery>parse(specialisedRoles).execute();
        List<Answer> marriageInstances = iqb.<GetQuery>parse(relationInstances).execute();
        List<Answer> marriageInstancesWithRPs = iqb.<GetQuery>parse(relationInstancesWithRPs).execute();
        List<Answer> marriageInstancesWithRPsandRoles = iqb.<GetQuery>parse(relationInstancesWithRPsandRoles).execute();

        assertEquals(spousePairs.size(), noOfMarriageRelations);
        assertEquals(wifeHusbandPairs.size(), noOfMarriages);
        assertEquals(marriageInstances.size(), noOfMarriageRelations);
        //for each marriage relation we can swap the roles hence the factor of 2
        assertEquals(marriageInstancesWithRPs.size(), 2* noOfMarriageRelations);
        assertEquals(marriageInstancesWithRPsandRoles.size(), 2 * noOfMarriageRelations);
    }

    /*
    test for the second rule file:
    match ($x, $y) isa cousins;
    match (parent-in-law: $x, child-in-law: $y) isa in-laws;
    */

    @Test
    public void testSiblings() {
        String queryString = "match (sibling:$x, sibling:$y) isa siblings; get;";
        GetQuery query = iqb.parse(queryString);

        List<Answer> answers = query.execute();
        assertEquals(answers.size(), 166);
        assertTrue(!hasDuplicates(answers));
    }

    @Test
    public void testCousins() {
        String queryString = "match ($x, $y) isa cousins; get;";
        GetQuery query = iqb.parse(queryString);

        List<Answer> answers = query.execute();
        assertEquals(answers.size(), 192);
        assertTrue(!hasDuplicates(answers));
    }

    @Test
    public void testDifferentInLawVariants() {
        String queryString = "match (parent-in-law: $x, child-in-law: $y) isa in-laws;$y has gender 'male'; get;";
        String queryString2 = "match $x isa in-laws; get;";
        String queryString3 = "match (parent-in-law: $x, child-in-law: $y) isa in-laws; get;";
        String queryString4 = "match $x (parent-in-law: $x1, child-in-law: $x2) isa in-laws; get;";
        GetQuery query = iqb.parse(queryString);
        GetQuery query2 = iqb.parse(queryString2);
        GetQuery query3 = iqb.parse(queryString3);
        GetQuery query4 = iqb.parse(queryString4);

        List<Answer> answers = query.execute();
        List<Answer> answers2 = query2.execute();
        List<Answer> answers3 = query3.execute();
        List<Answer> answers4 = query4.execute();

        //numbers do not add up because there are duplicate relations with specialised roles
        assertEquals(answers.size(), 22);
        assertEquals(answers2.size(), 92);
        assertEquals(answers3.size(), 50);
        assertEquals(answers3.size(), answers4.size());
    }

    /*
    test for the last rule-file:
    match (mother: $x);
    match (son: $x);
    match (grandfather: $x); (grandparent: $x, grandchild: $y);
    */

    @Test
    public void testParentConsistency() {
        String parentQuery = "match (parent: $x); get;";
        String childQuery = "match (child: $x); get;";
        String fatherQuery = "match (father: $x); get;";
        String motherQuery = "match (mother: $x); get;";
        String sonQuery = "match (son: $x); get;";
        String daughterQuery = "match (daughter: $x); get;";

        List<Answer> parents = iqb.<GetQuery>parse(parentQuery).execute();
        List<Answer> children = iqb.<GetQuery>parse(childQuery).execute();
        List<Answer> fathers = iqb.<GetQuery>parse(fatherQuery).execute();
        List<Answer> mothers = iqb.<GetQuery>parse(motherQuery).execute();
        List<Answer> sons = iqb.<GetQuery>parse(sonQuery).execute();
        List<Answer> daughters = iqb.<GetQuery>parse(daughterQuery).execute();
        assertEquals(parents.size(), noOfParents);
        assertEquals(children.size(), noOfChildren);
        assertCollectionsEqual(ReasonerUtils.listUnion(fathers, mothers), parents);
        assertCollectionsEqual(ReasonerUtils.listUnion(sons, daughters), children);
    }

    @Test
    public void testFemaleFather() {
        String queryString = "match (father: $x) isa parentship; $x has gender $g; $g == 'female'; get;";
        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.execute();
        List<Answer> answers2 =  genealogyKB.tx().graql().infer(true).<GetQuery>parse(queryString).execute();
        assertThat(answers, empty());
        assertThat(answers2, empty());
    }

    //TODO flaky! will fix in another PR
    @Ignore
    @Test
    public void grandChildrenThatAreGrandDaughters(){
        String queryString = "match "+
                "(grandchild: $x);" +
                "(granddaughter: $x);" +
                "$x has gender $g;" +
                "get;";
        GetQuery query = iqb.parse(queryString);
        List<Answer> answers = query.execute();
        assertEquals(answers.size(), 18);
        assertTrue(checkResource(answers, "g", "female"));
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
