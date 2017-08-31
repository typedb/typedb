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

package ai.grakn.graql.internal.reasoner;

import ai.grakn.concept.Label;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.test.SampleKBContext;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.Schema.ImplicitType.HAS;
import static ai.grakn.util.Schema.ImplicitType.HAS_OWNER;
import static ai.grakn.util.Schema.ImplicitType.HAS_VALUE;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Suite of tests checking different meanders and aspects of reasoning - full reasoning cycle is being tested.
 */
public class ReasoningTests {

    @ClassRule
    public static final SampleKBContext testSet1 = SampleKBContext.preLoad("testSet1.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet1b = SampleKBContext.preLoad("testSet1b.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet2 = SampleKBContext.preLoad("testSet2.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet3 = SampleKBContext.preLoad("testSet3.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet4 = SampleKBContext.preLoad("testSet4.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet5 = SampleKBContext.preLoad("testSet5.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet6 = SampleKBContext.preLoad("testSet6.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet7 = SampleKBContext.preLoad("testSet7.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet8 = SampleKBContext.preLoad("testSet8.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet9 = SampleKBContext.preLoad("testSet9.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet10 = SampleKBContext.preLoad("testSet10.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet11 = SampleKBContext.preLoad("testSet11.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet12 = SampleKBContext.preLoad("testSet12.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet13 = SampleKBContext.preLoad("testSet13.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet14 = SampleKBContext.preLoad("testSet14.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet15 = SampleKBContext.preLoad("testSet15.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet16 = SampleKBContext.preLoad("testSet16.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet17 = SampleKBContext.preLoad("testSet17.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet18 = SampleKBContext.preLoad("testSet18.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet19 = SampleKBContext.preLoad("testSet19.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet19recursive = SampleKBContext.preLoad("testSet19-recursive.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet20 = SampleKBContext.preLoad("testSet20.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet21 = SampleKBContext.preLoad("testSet21.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet22 = SampleKBContext.preLoad("testSet22.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet23 = SampleKBContext.preLoad("testSet23.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet24 = SampleKBContext.preLoad("testSet24.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet25 = SampleKBContext.preLoad("testSet25.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet26 = SampleKBContext.preLoad("testSet26.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet27 = SampleKBContext.preLoad("testSet27.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet28 = SampleKBContext.preLoad("testSet28.gql").assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext testSet29 = SampleKBContext.preLoad("testSet29.gql").assumeTrue(GraknTestSetup.usingTinker());

    @Before
    public void onStartup() throws Exception {
        assumeTrue(GraknTestSetup.usingTinker());
    }

    //The tests validate the correctness of the rule reasoning implementation w.r.t. the intended semantics of rules.
    //The ignored tests reveal some bugs in the reasoning algorithm, as they don't return the expected results,
    //as specified in the respective comments below.

    @Test //Expected result: Both queries should return a non-empty result, with $x/$y mapped to a unique entity.
    public void unificationWithVarDuplicates() {
        QueryBuilder qb = testSet1.tx().graql().infer(true);
        String queryString = "match (role1:$x, role2:$x) isa relation1; get;";
        String queryString2 = "match (role1:$x, role2:$y) isa relation1; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        List<Answer> answers2 = qb.<GetQuery>parse(queryString2).execute();

        assertEquals(1, answers.size());
        assertEquals(4, answers2.size());
        assertNotEquals(answers.size() * answers2.size(), 0);
        answers.forEach(x -> assertEquals(x.size(), 1));
        answers2.forEach(x -> assertEquals(x.size(), 2));
    }

    @Test //Expected result: Both queries should return a non-empty result, with $x/$y mapped to a unique entity.
    public void unificationWithVarDuplicates_SymmetricRelation() {
        QueryBuilder qb = testSet1b.tx().graql().infer(true);
        String queryString = "match (symmetricRole: $x, symmetricRole: $x) isa relation1; get;";
        String queryString2 = "match (symmetricRole: $x, symmetricRole: $y) isa relation1; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        List<Answer> answers2 = qb.<GetQuery>parse(queryString2).execute();

        assertEquals(1, answers.size());
        assertEquals(5, answers2.size());
        assertNotEquals(answers.size() * answers2.size(), 0);
        answers.forEach(x -> assertEquals(x.size(), 1));
        answers2.forEach(x -> assertEquals(x.size(), 2));
    }

    @Test //Expected result: The query should return a unique match.
    public void generatingMultipleIsaEdges() {
        QueryBuilder qb = testSet2.tx().graql().infer(true);
        String queryString = "match $x isa entity2; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 1);
    }

    @Test //Expected result: The queries should return different matches, unique per query.
    public void generatingFreshEntity() {
        QueryBuilder qb = testSet3.tx().graql().infer(true);
        String queryString = "match $x isa entity1; get;";
        String queryString2 = "match $x isa entity2; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        List<Answer> answers2 = qb.<GetQuery>parse(queryString2).execute();
        assertEquals(answers.size(), answers2.size());
        assertFalse(answers.containsAll(answers2));
        assertFalse(answers2.containsAll(answers));
    }

    @Test //Expected result: The queries should return the same two matches.
    public void generatingIsaEdge() {
        QueryBuilder qb = testSet4.tx().graql().infer(true);
        String queryString = "match $x isa entity1; get;";
        String queryString2 = "match $x isa entity2; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        List<Answer> answers2 = qb.<GetQuery>parse(queryString2).execute();
        assertEquals(answers.size(), 2);
        assertTrue(answers.containsAll(answers2));
        assertTrue(answers2.containsAll(answers));
    }

    @Test //Expected result: The query should return a unique match (or possibly nothing if we enforce range-restriction).
    public void generatingFreshEntity2() {
        QueryBuilder qb = testSet5.tx().graql().infer(false);
        QueryBuilder iqb = testSet5.tx().graql().infer(true);
        String queryString = "match $x isa entity2; get;";
        String explicitQuery = "match $x isa entity1; get;";
        List<Answer> answers = iqb.<GetQuery>parse(queryString).execute();
        List<Answer> answers2 = qb.<GetQuery>parse(explicitQuery).execute();

        assertEquals(answers2.size(), 3);
        assertTrue(!answers2.containsAll(answers));
    }

    @Test //Expected result: The query should return three different instances of relation1 with unique ids.
    public void generatingFreshRelation() {
        QueryBuilder qb = testSet6.tx().graql().infer(true);
        String queryString = "match $x isa relation1; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 3);
    }

    @Test //Expected result: The query should return 10 unique matches (no duplicates).
    public void distinctLimitedAnswersOfInfinitelyGeneratingRule() {
        QueryBuilder iqb = testSet7.tx().graql().infer(true);
        QueryBuilder qb = testSet7.tx().graql().infer(false);
        String queryString = "match $x isa relation1; limit 10; get;";
        List<Answer> answers = iqb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 10);
        assertEquals(answers.size(), qb.<GetQuery>parse(queryString).execute().size());
    }

    @Test //Expected result: The query should not return any matches (or possibly return a single match with $x=$y)
    public void roleUnificationWithRoleHierarchiesInvolved() {
        QueryBuilder qb = testSet8.tx().graql().infer(true);
        String queryString = "match (role2:$x, role3:$y) isa relation2; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertThat(answers.stream().collect(toSet()), empty());
    }

    @Test //Expected result: The query should not return any matches (or possibly return a single match with $x=$y)
    public void roleUnificationWithRepeatingRoleTypes() {
        QueryBuilder qb = testSet9.tx().graql().infer(true);
        String queryString = "match (role1:$x, role1:$y) isa relation2; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertThat(answers.stream().collect(toSet()), empty());
    }

    @Test //Expected result: The query should return a single match
    public void roleUnificationWithLessRelationPlayersInQueryThanHead() {
        QueryBuilder qb = testSet9.tx().graql().infer(true);
        String queryString = "match (role1:$x) isa relation2; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 1);
    }

    /**
     * recursive relation having same type for different role players
     * tests for handling recursivity and equivalence of queries and relations
     */
    @Test //Expected result: The query should return a unique match
    public void transRelationWithEntityGuardsAtBothEnds() {
        QueryBuilder qb = testSet10.tx().graql().infer(true);
        String queryString = "match (role1: $x, role2: $y) isa relation2; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 1);
    }

    @Test //Expected result: The query should return a unique match
    public void transRelationWithRelationGuardsAtBothEnds() {
        QueryBuilder qb = testSet11.tx().graql().infer(true);
        String queryString = "match (role1:$x, role2:$y) isa relation3; get;";
        assertEquals(qb.<GetQuery>parse(queryString).execute().size(), 1);
    }

    @Test //Expected result: The query should return two unique matches
    public void circularRuleDependencies() {
        QueryBuilder qb = testSet12.tx().graql().infer(true);
        String queryString = "match (role1:$x, role2:$y) isa relation3; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 2);
    }

    @Test //Expected result: The query should return a unique match
    public void rulesInteractingWithTypeHierarchy() {
        QueryBuilder qb = testSet13.tx().graql().infer(true);
        String queryString = "match (role1:$x, role2:$y) isa relation2; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 1);
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources() {
        QueryBuilder qb = testSet14.tx().graql().infer(true);
        String queryString = "match $x isa entity1, has res1 $y; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        String queryString2 = "match $x isa res1; get;";
        QueryAnswers answers2 = queryAnswers(qb.parse(queryString2));

        assertEquals(answers.size(), 2);
        assertEquals(answers2.size(), 1);
    }

    @Test
    public void whenReasoningWithResourcesWithRelationVar_ResultsAreComplete() {
        QueryBuilder qb = testSet14.tx().graql().infer(true);

        VarPattern has = var("x").has(Label.of("res1"), var("y"), var("r"));
        List<Answer> answers = qb.match(has).get().execute();
        assertEquals(answers.size(), 2);
        answers.forEach(a -> assertTrue(a.vars().contains(var("r"))));
    }

    @Test
    public void whenExecutingAQueryWithImplicitTypes_InferenceHasAtLeastAsManyResults() {
        QueryBuilder withInference = testSet14.tx().graql().infer(true);
        QueryBuilder withoutInference = testSet14.tx().graql().infer(false);

        VarPattern owner = label(HAS_OWNER.getLabel("res1"));
        VarPattern value = label(HAS_VALUE.getLabel("res1"));
        VarPattern hasRes = label(HAS.getLabel("res1"));

        Function<QueryBuilder, GetQuery> query = qb -> qb.match(
                var().rel(owner, "x").rel(value, "y").isa(hasRes),
                var("a").has("res1", var("b"))  // This pattern is added only to encourage reasoning to activate
        ).get();

        Set<Answer> resultsWithInference = query.apply(withInference).stream().collect(toSet());
        Set<Answer> resultsWithoutInference = query.apply(withoutInference).stream().collect(toSet());

        assertThat(resultsWithoutInference, not(empty()));
        assertThat(Sets.difference(resultsWithoutInference, resultsWithInference), empty());
    }

    //TODO potentially a graql bug when executing match insert on shared resources
    @Ignore
    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources2() {
        QueryBuilder qb = testSet15.tx().graql().infer(true);
        String queryString = "match $x isa entity1, has res2 $y;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 1);

        String queryString2 = "match $x isa res2;";
        List<Answer> answers2 = qb.<GetQuery>parse(queryString2).execute();
        assertEquals(answers2.size(), 1);
        assertTrue(answers2.iterator().next().get(var("x")).isAttribute());
        String queryString3 = "match $x isa res1; $y isa res2;";
        List<Answer> answers3 = qb.<GetQuery>parse(queryString3).execute();
        assertEquals(answers3.size(), 1);

        assertTrue(answers3.iterator().next().get(var("x")).isAttribute());
        assertTrue(answers3.iterator().next().get(var("y")).isAttribute());
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources3() {
        QueryBuilder qb = testSet16.tx().graql().infer(true);

        String queryString = "match $x isa entity1, has res1 $y; $z isa relation1; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 1);
        answers.forEach(ans ->
                {
                    assertTrue(ans.get(var("x")).isEntity());
                    assertTrue(ans.get(var("y")).isAttribute());
                    assertTrue(ans.get(var("z")).isRelationship());
                }
        );

        String queryString2 = "match $x isa relation1, has res1 $y; get;";
        List<Answer> answers2 = qb.<GetQuery>parse(queryString2).execute();
        assertEquals(answers2.size(), 1);
        answers2.forEach(ans ->
                {
                    assertTrue(ans.get(var("x")).isRelationship());
                    assertTrue(ans.get(var("y")).isAttribute());
                }
        );
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources4() {
        QueryBuilder qb = testSet17.tx().graql().infer(true);
        String queryString = "match $x has res2 $r; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 1);
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void inferringSpecificResourceValue() {
        QueryBuilder qb = testSet18.tx().graql().infer(true);
        String queryString = "match $x has res1 'value'; get;";
        String queryString2 = "match $x has res1 $r; get;";
        GetQuery query = qb.parse(queryString);
        GetQuery query2 = qb.parse(queryString2);
        List<Answer> answers = query.execute();
        List<Answer> answers2 = query2.execute();
        List<Answer> requeriedAnswers = query.execute();
        assertEquals(answers.size(), 2);
        assertEquals(answers.size(), answers2.size());
        assertEquals(answers.size(), requeriedAnswers.size());
        assertTrue(answers.containsAll(requeriedAnswers));
    }

    @Test //Expected result: Two answers obtained only if the rule query containing sub type is correctly executed.
    public void instanceTypeHierarchyRespected_queryHasSuperTypes(){
        QueryBuilder qb = testSet19.tx().graql().infer(true);
        String queryString = "match " +
                "$x isa entity1;" +
                "$y isa entity1;" +
                "(role1: $x, role2: $y) isa relation1;";
        String queryString2 = queryString + "$y has name 'a';";
        List<Answer> answers = qb.<GetQuery>parse(queryString + " get;").execute();
        assertEquals(answers.size(), 2);
        List<Answer> answers2 = qb.<GetQuery>parse(queryString2 + " get;").execute();
        assertEquals(answers2.size(), 2);
    }

    @Test //Expected result: Single answer obtained only if the rule query containing super type is correctly executed.
    public void instanceTypeHierarchyRespected_querySpecialisesType(){
        QueryBuilder qb = testSet19.tx().graql().infer(true);
        String queryString = "match " +
                "$x isa entity1;" +
                "$y isa subEntity1;" +
                "(role1: $x, role2: $y) isa relation1;";
        String queryString2 = queryString + "$y has name 'a';";

        List<Answer> answers = qb.<GetQuery>parse(queryString + " get;").execute();
        assertEquals(answers.size(), 1);
        List<Answer> answers2 = qb.<GetQuery>parse(queryString2 + " get;").execute();
        assertEquals(answers2.size(), 1);
    }

    @Test //Expected result: Single answer obtained only if the rule query containing super type is correctly executed.
    public void instanceTypeHierarchyRespected_queryOverwritesTypes(){
        QueryBuilder qb = testSet19.tx().graql().infer(true);
        String queryString = "match " +
                "$x isa subEntity1;" +
                "$y isa entity1;" +
                "(role1: $x, role2: $y) isa relation1;";
        String queryString2 = queryString + "$y has name 'a';";

        List<Answer> answers = qb.<GetQuery>parse(queryString + "get;").execute();
        assertEquals(answers.size(), 2);
        List<Answer> answers2 = qb.<GetQuery>parse(queryString2 + "get;").execute();
        assertEquals(answers2.size(), 2);
    }

    @Test //Expected result: Two answers obtained only if the rule query containing sub type is correctly executed.
    public void instanceTypeHierarchyRespected_queryHasSuperTypes_recursiveRule(){
        QueryBuilder qb = testSet19recursive.tx().graql().infer(true);
        String queryString = "match " +
                "$x isa entity1;" +
                "$y isa entity1;" +
                "(role1: $x, role2: $y) isa relation1;";
        String queryString2 = queryString + "$y has name 'a';";
        List<Answer> answers = qb.<GetQuery>parse(queryString + " get;").execute();
        assertEquals(answers.size(), 2);
        List<Answer> answers2 = qb.<GetQuery>parse(queryString2 + " get;").execute();
        assertEquals(answers2.size(), 2);
    }

    @Test //Expected result: Single answer obtained only if the rule query containing super type is correctly executed.
    public void instanceTypeHierarchyRespected_querySpecialisesType_recursiveRule(){
        QueryBuilder qb = testSet19recursive.tx().graql().infer(true);
        String queryString = "match " +
                "$x isa entity1;" +
                "$y isa subEntity1;" +
                "(role1: $x, role2: $y) isa relation1;";
        String queryString2 = queryString + "$y has name 'a';";

        List<Answer> answers = qb.<GetQuery>parse(queryString + " get;").execute();
        assertEquals(answers.size(), 1);
        List<Answer> answers2 = qb.<GetQuery>parse(queryString2 + " get;").execute();
        assertEquals(answers2.size(), 1);
    }

    @Test //Expected result: Single answer obtained only if the rule query containing super type is correctly executed.
    public void instanceTypeHierarchyRespected_queryOverwritesTypes_recursiveRule(){
        QueryBuilder qb = testSet19recursive.tx().graql().infer(true);
        String queryString = "match " +
                "$x isa subEntity1;" +
                "$y isa entity1;" +
                "(role1: $x, role2: $y) isa relation1;";
        String queryString2 = queryString + "$y has name 'a';";

        List<Answer> answers = qb.<GetQuery>parse(queryString + " get;").execute();
        assertEquals(answers.size(), 2);
        List<Answer> answers2 = qb.<GetQuery>parse(queryString2 + " get;").execute();
        assertEquals(answers2.size(), 2);
    }

    @Test //Expected result: Both queries should return a single equal match as they trigger the same rule.
    public void reasoningOverRelationHierarchy(){
        QueryBuilder qb = testSet20.tx().graql().infer(true);
        String queryString = "match (role1: $x, role2: $y) isa relation1; get;";
        String queryString2 = "match (role1: $x, role2: $y) isa sub-relation1; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        List<Answer> answers2 = qb.<GetQuery>parse(queryString2).execute();
        assertEquals(answers.size(), 1);
        assertTrue(answers.containsAll(answers2));
        assertTrue(answers2.containsAll(answers));
    }

    @Test //Expected result: Both queries should return a single equal match as they trigger the same rule.
    public void reasoningOverEntityHierarchy(){
        QueryBuilder qb = testSet21.tx().graql().infer(true);
        String queryString = "match $x isa entity1; get;";
        String queryString2 = "match $x isa sub-entity1; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        List<Answer> answers2 = qb.<GetQuery>parse(queryString2).execute();
        assertEquals(answers.size(), 1);
        assertTrue(answers.containsAll(answers2));
        assertTrue(answers2.containsAll(answers));
    }

    @Test //Expected result: Returns db and inferred relations + their inverses and relations with self for all entities
    public void reasoningWithRepeatingRoles(){
        QueryBuilder qb = testSet22.tx().graql().infer(true);
        String queryString = "match (friend:$x1, friend:$x2) isa knows-trans; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 16);
    }

    @Test //Expected result: The same set of results is always returned
    public void reasoningWithLimitHigherThanNumberOfResults_ReturnsConsistentResults(){
        QueryBuilder qb = testSet23.tx().graql().infer(true);
        String queryString = "match (friend1:$x1, friend2:$x2) isa knows-trans;limit 60; get;";
        List<Answer> oldAnswers = qb.<GetQuery>parse(queryString).execute();
        for(int i = 0; i < 5 ; i++) {
            List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
            assertEquals(answers.size(), 6);
            assertTrue(answers.containsAll(oldAnswers));
            assertTrue(oldAnswers.containsAll(answers));
        }
    }

    @Test //Expected result: Relations between all entity instances including relation between each instance and itself
    public void reasoningWithEntityTypes() {
        QueryBuilder qb = testSet24.tx().graql().infer(true);
        QueryBuilder qbm = testSet24.tx().graql().infer(true);
        String queryString = "match (role1:$x1, role2:$x2) isa relation1; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        List<Answer> answers2 = qbm.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 9);
        assertEquals(answers2.size(), 9);
        assertTrue(answers.containsAll(answers2));
        assertTrue(answers2.containsAll(answers));
    }

    @Test //Expected result: Relations between all entity instances including relation between each instance and itself
    public void reasoningWithEntityTypes_WithNeqProperty() {
        QueryBuilder qb = testSet24.tx().graql().infer(true);
        QueryBuilder qbm = testSet24.tx().graql().infer(true).materialise(true);
        String queryString = "match (role1:$x1, role2:$x2) isa relation2; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 6);
        List<Answer> answers2 = qbm.<GetQuery>parse(queryString).execute();
        assertEquals(answers2.size(), 6);
        assertTrue(answers.containsAll(answers2));
        assertTrue(answers2.containsAll(answers));
    }

    @Test //Expected result: Timeline is correctly recognised via applying resource comparisons in the rule body
    public void reasoningWithResourceValueComparison() {
        QueryBuilder qb = testSet25.tx().graql().infer(true);
        String queryString = "match (predecessor:$x1, successor:$x2) isa message-succession; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 10);
    }

    //tests if partial substitutions are propagated correctly - atom disjointness may lead to variable loss (bug #15476)
    @Test //Expected result: 2 relations obtained by correctly finding reified relations
    public void reasoningWithReifiedRelations() {
        QueryBuilder qb = testSet26.tx().graql().infer(true);
        String queryString = "match (role1: $x1, role2: $x2) isa relation2; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 2);

        String queryString2 = "match " +
                "$b isa entity2;" +
                "$b has res1 'value';" +
                "$rel1 has res2 'value1';" +
                "$rel1 (role1: $p, role2: $b) isa relation1;" +
                "$rel2 has res2 'value2';" +
                "$rel2 (role1: $c, role2: $b) isa relation1; get;";
        List<Answer> answers2 = qb.<GetQuery>parse(queryString2).execute();
        assertEquals(answers2.size(), 2);
        Set<Var> vars = Sets.newHashSet(var("b"), var("p"), var("c"), var("rel1"), var("rel2"));
        answers2.forEach(ans -> assertTrue(ans.vars().containsAll(vars)));
    }

    @Test //Expected result: 2 relations obtained by correctly finding reified relations
    public void reasoningWithNeqProperty() {
        QueryBuilder qb = testSet27.tx().graql().infer(true);
        String queryString = "match (related-state: $s) isa holds; get;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        List<Answer> exact = qb.<GetQuery>parse("match $s isa state, has name 's2'; get;").execute();
        assertTrue(answers.containsAll(exact));
        assertTrue(exact.containsAll(answers));
    }

    @Test //Expected result: number of answers equal to specified limit (no duplicates produced)
    public void duplicatesNotProducedWhenResolvingNonResolvableConjunctionsWithoutType(){
        QueryBuilder qb = testSet28.tx().graql().infer(true);
        String queryString = "match " +
                "(role1: $x, role2: $y);" +
                "(role1: $y, role2: $z);" +
                "(role3: $z, role4: $w) isa relation3;" +
                "limit 3; get;";

        assertEquals(qb.<GetQuery>parse(queryString).execute().size(), 3);
    }

    @Test //Expected result: no answers (if types were incorrectly inferred the query would yield answers)
    public void relationTypesAreCorrectlyInferredInConjunctionWhenTypeIsPresent(){
        QueryBuilder qb = testSet28.tx().graql().infer(true);
        String queryString = "match " +
                "(role1: $x, role2: $y) isa relation1;" +
                "(role1: $y, role2: $z) isa relation1;" +
                "(role3: $z, role4: $w) isa relation3; get;";

        assertThat(qb.<GetQuery>parse(queryString).execute(), empty());
    }

    @Test //Expected result: no answers (if types were incorrectly inferred the query would yield answers)
    public void transRelationWithNeqPredicate(){
        QueryBuilder qb = testSet29.tx().graql().infer(true);
        String queryString = "match " +
                "(role1: $x, role2: $y) isa relation1;" +
                "$y has name 'c';" +
                "$x != $y; get;";

        String explicitString = "match " +
                "(role1: $x, role2: $y) isa relation1;" +
                "$y has name 'c';" +
                "{$x has name 'a';} or {$x has name 'b';}; get;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        List<Answer> answers2 = qb.<GetQuery>parse(explicitString).execute();
        assertTrue(answers.containsAll(answers2));
        assertTrue(answers2.containsAll(answers));
    }

    private QueryAnswers queryAnswers(GetQuery query) {
        return new QueryAnswers(query.stream().collect(toSet()));
    }
}
