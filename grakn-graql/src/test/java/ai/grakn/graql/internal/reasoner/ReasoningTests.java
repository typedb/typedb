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

package ai.grakn.graql.internal.reasoner;

/*-
 * #%L
 * grakn-graql
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.GraknTx;
import ai.grakn.concept.Label;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Answer;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.GraknTestUtil;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
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
import org.apache.commons.math3.util.CombinatoricsUtils;

/**
 * Suite of tests checking different meanders and aspects of reasoning - full reasoning cycle is being tested.
 */
public class ReasoningTests {

    @ClassRule
    public static final SampleKBContext testSet1 = SampleKBContext.load("testSet1.gql");

    @ClassRule
    public static final SampleKBContext testSet1b = SampleKBContext.load("testSet1b.gql");

    @ClassRule
    public static final SampleKBContext testSet2 = SampleKBContext.load("testSet2.gql");

    @ClassRule
    public static final SampleKBContext testSet2b = SampleKBContext.load("testSet2b.gql");

    @ClassRule
    public static final SampleKBContext testSet2c = SampleKBContext.load("testSet2c.gql");

    @ClassRule
    public static final SampleKBContext testSet3 = SampleKBContext.load("testSet3.gql");

    @ClassRule
    public static final SampleKBContext testSet4 = SampleKBContext.load("testSet4.gql");

    @ClassRule
    public static final SampleKBContext testSet5 = SampleKBContext.load("testSet5.gql");

    @ClassRule
    public static final SampleKBContext testSet6 = SampleKBContext.load("testSet6.gql");

    @ClassRule
    public static final SampleKBContext testSet7 = SampleKBContext.load("testSet7.gql");

    @ClassRule
    public static final SampleKBContext testSet8 = SampleKBContext.load("testSet8.gql");

    @ClassRule
    public static final SampleKBContext testSet9 = SampleKBContext.load("testSet9.gql");

    @ClassRule
    public static final SampleKBContext testSet10 = SampleKBContext.load("testSet10.gql");

    @ClassRule
    public static final SampleKBContext testSet11 = SampleKBContext.load("testSet11.gql");

    @ClassRule
    public static final SampleKBContext testSet12 = SampleKBContext.load("testSet12.gql");

    @ClassRule
    public static final SampleKBContext testSet13 = SampleKBContext.load("testSet13.gql");

    @ClassRule
    public static final SampleKBContext testSet14 = SampleKBContext.load("testSet14.gql");

    @ClassRule
    public static final SampleKBContext testSet15 = SampleKBContext.load("testSet15.gql");

    @ClassRule
    public static final SampleKBContext testSet16 = SampleKBContext.load("testSet16.gql");

    @ClassRule
    public static final SampleKBContext testSet17 = SampleKBContext.load("testSet17.gql");

    @ClassRule
    public static final SampleKBContext testSet19 = SampleKBContext.load("testSet19.gql");

    @ClassRule
    public static final SampleKBContext testSet19recursive = SampleKBContext.load("testSet19-recursive.gql");

    @ClassRule
    public static final SampleKBContext testSet20 = SampleKBContext.load("testSet20.gql");

    @ClassRule
    public static final SampleKBContext testSet21 = SampleKBContext.load("testSet21.gql");

    @ClassRule
    public static final SampleKBContext testSet22 = SampleKBContext.load("testSet22.gql");

    @ClassRule
    public static final SampleKBContext testSet23 = SampleKBContext.load("testSet23.gql");

    @ClassRule
    public static final SampleKBContext testSet24 = SampleKBContext.load("testSet24.gql");

    @ClassRule
    public static final SampleKBContext testSet25 = SampleKBContext.load("testSet25.gql");

    @ClassRule
    public static final SampleKBContext testSet26 = SampleKBContext.load("testSet26.gql");

    @ClassRule
    public static final SampleKBContext testSet27 = SampleKBContext.load("testSet27.gql");

    @ClassRule
    public static final SampleKBContext testSet28 = SampleKBContext.load("testSet28.gql");

    @ClassRule
    public static final SampleKBContext testSet28b = SampleKBContext.load("testSet28b.gql");

    @ClassRule
    public static final SampleKBContext testSet29 = SampleKBContext.load("testSet29.gql");

    @ClassRule
    public static final SampleKBContext testSet30 = SampleKBContext.load("testSet30.gql");

    @Before
    public void onStartup() throws Exception {
        assumeTrue(GraknTestUtil.usingTinker());
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

        assertEquals(2, answers.size());
        assertEquals(8, answers2.size());
        assertNotEquals(answers.size() * answers2.size(), 0);
        answers.forEach(x -> assertEquals(x.size(), 1));
        answers2.forEach(x -> assertEquals(x.size(), 2));
    }

    @Test //Expected result: The query should return a unique match.
    public void generatingMultipleIsaEdges() {
        QueryBuilder qb = testSet2.tx().graql().infer(true);
        String queryString = "match $x isa derivedEntity; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 1);
    }

    @Test //Expected result: Differentiated behaviour based on directedness of the isa.
    public void generatingIsaEdgesDirectly() {
        QueryBuilder qb = testSet2b.tx().graql().infer(true);
        String queryString = "match $x isa derivedEntity; get;";
        String queryString2 = "match $x isa! derivedEntity; get;";
        String queryString3 = "match $x isa directDerivedEntity; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        List<Answer> answers2 = qb.<GetQuery>parse(queryString2).execute();
        List<Answer> answers3 = qb.<GetQuery>parse(queryString3).execute();
        assertEquals(answers.size(), 2);
        assertEquals(answers2.size(), 2);
        assertEquals(answers3.size(), 1);
    }

    @Test //Expected result: Differentiated behaviour based on directedness of the isa.
    public void generatingIsaEdgesForRelationsDirectly() {
        QueryBuilder qb = testSet2c.tx().graql().infer(true);
        String queryString = "match ($x, $y) isa derivedRelation; get;";
        String queryString2 = "match ($x, $y) isa! derivedRelation; get;";
        String queryString3 = "match ($x, $y) isa directDerivedRelation; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        List<Answer> answers2 = qb.<GetQuery>parse(queryString2).execute();
        List<Answer> answers3 = qb.<GetQuery>parse(queryString3).execute();
        assertEquals(answers.size(), 2);
        assertEquals(answers2.size(), 2);
        assertEquals(answers3.size(), 1);
    }

    @Test //Expected result: The query should return 3 results: one for meta type, one for db, one for inferred type.
    public void queryingForGenericType_ruleDefinesNewType() {
        QueryBuilder qb = testSet2.tx().graql().infer(true);
        String queryString = "match $x isa $type; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 4);
        answers.forEach(ans -> assertEquals(ans.size(), 2));
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

    //TODO: currently disallowed by rule validation
    @Ignore
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

    //TODO: currently disallowed by rule validation
    @Ignore
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

    //TODO: currently disallowed by rule validation
    @Ignore
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
    public void reusingResources_usingExistingResourceToDefineResource() {
        QueryBuilder qb = testSet14.tx().graql().infer(true);

        String queryString = "match $x isa entity1, has resource $y; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        String queryString2 = "match $x isa resource; get;";
        List<Answer> answers2 = qb.<GetQuery>parse(queryString2).execute();

        assertEquals(answers.size(), 2);
        assertEquals(answers2.size(), 1);
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources_queryingForGenericRelation() {
        QueryBuilder qb = testSet14.tx().graql().infer(true);

        String queryString = "match $x isa entity1;($x, $y); get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();

        assertEquals(answers.size(), 3);
        assertEquals(answers.stream().filter(answer -> answer.get("y").isAttribute()).count(), 2);
    }

    //TODO potentially a graql bug when executing match insert on shared resources
    @Ignore
    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources_usingExistingResourceToDefineSubResource() {
        QueryBuilder qb = testSet14.tx().graql().infer(true);
        String queryString = "match $x isa entity1, has subResource $y;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 1);

        String queryString2 = "match $x isa subResource;";
        List<Answer> answers2 = qb.<GetQuery>parse(queryString2).execute();
        assertEquals(answers2.size(), 1);
        assertTrue(answers2.iterator().next().get(var("x")).isAttribute());
        String queryString3 = "match $x isa resource; $y isa subResource;";
        List<Answer> answers3 = qb.<GetQuery>parse(queryString3).execute();
        assertEquals(answers3.size(), 1);

        assertTrue(answers3.iterator().next().get(var("x")).isAttribute());
        assertTrue(answers3.iterator().next().get(var("y")).isAttribute());
    }

    @Test
    public void whenReasoningWithResourcesWithRelationVar_ResultsAreComplete() {
        QueryBuilder qb = testSet14.tx().graql().infer(true);

        VarPattern has = var("x").has(Label.of("resource"), var("y"), var("r"));
        List<Answer> answers = qb.match(has).get().execute();
        assertEquals(answers.size(), 3);
        answers.forEach(a -> assertTrue(a.vars().contains(var("r"))));
    }

    @Test
    public void whenExecutingAQueryWithImplicitTypes_InferenceHasAtLeastAsManyResults() {
        QueryBuilder withInference = testSet14.tx().graql().infer(true);
        QueryBuilder withoutInference = testSet14.tx().graql().infer(false);

        VarPattern owner = label(HAS_OWNER.getLabel("resource"));
        VarPattern value = label(HAS_VALUE.getLabel("resource"));
        VarPattern hasRes = label(HAS.getLabel("resource"));

        Function<QueryBuilder, GetQuery> query = qb -> qb.match(
                var().rel(owner, "x").rel(value, "y").isa(hasRes),
                var("a").has("resource", var("b"))  // This pattern is added only to encourage reasoning to activate
        ).get();


        Set<Answer> resultsWithoutInference = query.apply(withoutInference).stream().collect(toSet());
        Set<Answer> resultsWithInference = query.apply(withInference).stream().collect(toSet());

        assertThat(resultsWithoutInference, not(empty()));
        assertThat(Sets.difference(resultsWithoutInference, resultsWithInference), empty());
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources_attachingExistingResourceToARelation() {
        QueryBuilder qb = testSet14.tx().graql().infer(true);

        String queryString = "match $x isa entity1, has resource $y; $z isa relation; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 2);
        answers.forEach(ans ->
                {
                    assertTrue(ans.get(var("x")).isEntity());
                    assertTrue(ans.get(var("y")).isAttribute());
                    assertTrue(ans.get(var("z")).isRelationship());
                }
        );

        String queryString2 = "match $x isa relation, has resource $y; get;";
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
    public void reusingResources_definingResourceThroughOtherResourceWithConditionalValue() {
        QueryBuilder qb = testSet15.tx().graql().infer(true);
        String queryString = "match $x has boolean-resource $r; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 1);
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void inferringSpecificResourceValue() {
        QueryBuilder qb = testSet16.tx().graql().infer(true);
        String queryString = "match $x has resource 'value'; get;";
        String queryString2 = "match $x has resource $r; get;";
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

    @Test
    public void resourcesAsRolePlayers() {
        QueryBuilder qb = testSet17.tx().graql().infer(true);

        String queryString = "match $x isa resource val 'partial bad flag'; ($x, resource-owner: $y) isa resource-relation; get;";
        String queryString2 = "match $x isa resource val 'partial bad flag 2'; ($x, resource-owner: $y) isa resource-relation; get;";
        String queryString3 = "match $x isa resource val 'bad flag' ; ($x, resource-owner: $y) isa resource-relation; get;";
        String queryString4 = "match $x isa resource val 'no flag' ; ($x, resource-owner: $y) isa resource-relation; get;";
        String queryString5 = "match $x isa resource; ($x, resource-owner: $y) isa resource-relation; get;";
        String queryString6 = "match $x isa resource; $x val contains 'bad flag';($x, resource-owner: $y) isa resource-relation; get;";

        GetQuery query = qb.parse(queryString);
        GetQuery query2 = qb.parse(queryString2);
        GetQuery query3 = qb.parse(queryString3);
        GetQuery query4 = qb.parse(queryString4);
        GetQuery query5 = qb.parse(queryString5);
        GetQuery query6 = qb.parse(queryString6);

        List<Answer> answers = query.execute();
        List<Answer> answers2 = query2.execute();
        List<Answer> answers3 = query3.execute();
        List<Answer> answers4 = query4.execute();
        List<Answer> answers5 = query5.execute();
        List<Answer> answers6 = query6.execute();

        assertEquals(answers.size(), 2);
        assertEquals(answers2.size(), 1);
        assertEquals(answers3.size(), 1);
        assertEquals(answers4.size(), 1);
        assertEquals(answers5.size(), answers.size() + answers2.size() + answers3.size() + answers4.size());
        assertEquals(answers6.size(), answers5.size() - answers4.size());
    }

    @Test
    public void resourcesAsRolePlayers_vpPropagationTest() {
        QueryBuilder qb = testSet17.tx().graql().infer(true);

        String queryString = "match $x isa resource val 'partial bad flag'; ($x, resource-owner: $y) isa another-resource-relation; get;";
        String queryString2 = "match $x isa resource val 'partial bad flag 2'; ($x, resource-owner: $y) isa another-resource-relation; get;";
        String queryString3 = "match $x isa resource val 'bad flag' ; ($x, resource-owner: $y) isa another-resource-relation; get;";
        String queryString4 = "match $x isa resource val 'no flag' ; ($x, resource-owner: $y) isa another-resource-relation; get;";
        String queryString5 = "match $x isa resource; ($x, resource-owner: $y) isa another-resource-relation; get;";
        String queryString6 = "match $x isa resource; $x val contains 'bad flag';($x, resource-owner: $y) isa another-resource-relation; get;";

        GetQuery query = qb.parse(queryString);
        GetQuery query2 = qb.parse(queryString2);
        GetQuery query3 = qb.parse(queryString3);
        GetQuery query4 = qb.parse(queryString4);
        GetQuery query5 = qb.parse(queryString5);
        GetQuery query6 = qb.parse(queryString6);

        List<Answer> answers = query.execute();
        List<Answer> answers2 = query2.execute();
        List<Answer> answers3 = query3.execute();
        List<Answer> answers4 = query4.execute();
        List<Answer> answers5 = query5.execute();
        List<Answer> answers6 = query6.execute();

        assertEquals(answers.size(), 3);
        assertEquals(answers2.size(), 3);
        assertEquals(answers3.size(), 3);
        assertEquals(answers4.size(), 3);
        assertEquals(answers5.size(), answers.size() + answers2.size() + answers3.size() + answers4.size());
        assertEquals(answers6.size(), answers5.size() - answers4.size());
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
    public void relationTypesAreCorrectlyInferredInConjunction_TypeArePresent(){
        QueryBuilder qb = testSet28.tx().graql().infer(true);
        String queryString = "match " +
                "(role1: $x, role2: $y) isa relation1;" +
                "(role1: $y, role2: $z) isa relation1;" +
                "(role3: $z, role4: $w) isa relation3; get;";

        assertThat(qb.<GetQuery>parse(queryString).execute(), empty());
    }

    @Test
    public void relationTypesAreCorrectlyInferredInConjunction_TypesAreAbsent(){
        QueryBuilder qb = testSet28b.tx().graql().infer(true);
        String queryString = "match " +
                "$a isa entity1;" +
                "($a, $b); $b isa entity3;" +
                "($b, $c);" +
                "get;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 4);
        answers.forEach(ans -> assertEquals(ans.size(), 3));
    }

    @Test
    public void relationTypesAreCorrectlyInferredInConjunction_TypesAreAbsent_DisconnectedQuery(){
        QueryBuilder qb = testSet28b.tx().graql().infer(true);

        String pattern = "{$a isa entity1;($a, $b); $b isa entity3;};";
        String pattern2 = "{($c, $d);};";
        String queryString = "match " +
                pattern +
                pattern2 +
                "get;";
        List<Answer> partialAnswers = qb.match(Graql.parser().parsePatterns(pattern)).get().execute();

        //single relation that satisfies the types
        assertEquals(partialAnswers.size(), 1);

        List<Answer> partialAnswers2 = qb.match(Graql.parser().parsePatterns(pattern2)).get().execute();
        //(4 db relations  + 1 inferred + 1 resource) x 2 for variable swap
        assertEquals(partialAnswers2.size(), 12);

        //1 relation satisfying ($a, $b) with types x (4 db relations + 1 inferred + 1 resource) x 2 for var change
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), partialAnswers.size() * partialAnswers2.size());
        answers.forEach(ans -> assertEquals(ans.size(), 4));
    }

    /* Should find the possible relation configurations:
         (x, z) - (z, z1) - (z1, z)
                - (z, z2) - (z2, z)
                - (z, y)  - { (y,z) (y, x) }
                - (z, x)  - { res, (x, y), (x, z) }
         */
    @Test
    public void relationTypesAreCorrectlyInferredInConjunction_TypesAreAbsent_WithRelationWithoutAnyBounds(){
        QueryBuilder qb = testSet28b.tx().graql().infer(true);
        String entryPattern = "{" +
                "$a isa entity1;" +
                "($a, $b);" +
                "};";

        List<Answer> entryAnswers = qb.match(Graql.parser().parsePatterns(entryPattern)).get().execute();
        assertEquals(entryAnswers.size(), 3);

        String partialPattern = "{" +
                "$a isa entity1;" +
                "($a, $b); $b isa entity3;" +
                "($b, $c);" +
                "};";

        List<Answer> partialAnswers = qb.match(Graql.parser().parsePatterns(partialPattern)).get().execute();
        assertEquals(partialAnswers.size(), 4);
        String queryString = "match " +
                partialPattern +
                "($c, $d);" +
                "get;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 7);
        answers.forEach(ans -> assertEquals(ans.size(), 4));
    }

    @Test //tests a query containing a neq predicate bound to a recursive relation
    public void recursiveRelationWithNeqPredicate(){
        QueryBuilder qb = testSet29.tx().graql().infer(true);
        String baseQueryString = "match " +
                "(role1: $x, role2: $y) isa binary-base;" +
                "$x != $y;";
        String queryString = baseQueryString + "$y has name 'c'; get;";

        List<Answer> baseAnswers = qb.<GetQuery>parse(baseQueryString + "get;").execute();
        assertEquals(baseAnswers.size(), 6);
        baseAnswers.forEach(ans -> {
            assertEquals(ans.size(), 2);
            assertNotEquals(ans.get("x"), ans.get("y"));
        });

        String explicitString = "match " +
                "(role1: $x, role2: $y) isa binary-base;" +
                "$y has name 'c';" +
                "{$x has name 'a';} or {$x has name 'b';}; get;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        List<Answer> answers2 = qb.<GetQuery>parse(explicitString).execute();
        assertTrue(answers.containsAll(answers2));
        assertTrue(answers2.containsAll(answers));
    }

    /**
     * Tests a scenario in which the neq predicate binds free variables of two recursive equivalent relations.
     * Corresponds to the following pattern:
     *
     *                     x
     *                   /    \
     *                 /        \
     *               v           v
     *              y     !=      z
     */
    @Test
    public void recursiveRelationsWithSharedNeqPredicate_relationsAreEquivalent(){
        QueryBuilder qb = testSet29.tx().graql().infer(true);
        String baseQueryString = "match " +
                "(role1: $x, role2: $y) isa binary-base;" +
                "(role1: $x, role2: $z) isa binary-base;" +
                "$y != $z;";

        List<Answer> baseAnswers = qb.<GetQuery>parse(baseQueryString + "get;").execute();
        assertEquals(baseAnswers.size(), 18);
        baseAnswers.forEach(ans -> {
            assertEquals(ans.size(), 3);
            assertNotEquals(ans.get("y"), ans.get("z"));
        });

        String queryString = baseQueryString + "$x has name 'a';";
        String explicitString = "match " +
                "$x has name 'a';" +
                "{$y has name 'a';$z has name 'b';} or " +
                "{$y has name 'a';$z has name 'c';} or " +
                "{$y has name 'b';$z has name 'a';} or" +
                "{$y has name 'b';$z has name 'c';} or " +
                "{$y has name 'c';$z has name 'a';} or " +
                "{$y has name 'c';$z has name 'b';};";

        List<Answer> answers = qb.<GetQuery>parse(queryString + "get;").execute();
        List<Answer> answers2 = qb.infer(false).<GetQuery>parse(explicitString + "get;").execute();
        assertTrue(baseAnswers.containsAll(answers));
        assertTrue(answers.containsAll(answers2));
        assertTrue(answers2.containsAll(answers));
    }

    /**
     * Tests a scenario in which the neq predicate prevents loops by binding free variables
     * of two recursive non-equivalent relations. Corresponds to the following pattern:
     *
     *                     y
     *                    ^  \
     *                  /      \
     *                /          v
     *              x     !=      z
     */
    @Test
    public void multipleRecursiveRelationsWithSharedNeqPredicate_neqPredicatePreventsLoops(){
        QueryBuilder qb = testSet29.tx().graql().infer(true);
        String baseQueryString = "match " +
                "(role1: $x, role2: $y) isa binary-base;" +
                "(role1: $y, role2: $z) isa binary-base;" +
                "$x != $z;";

        List<Answer> baseAnswers = qb.<GetQuery>parse(baseQueryString + "get;").execute();
        assertEquals(baseAnswers.size(), 18);
        baseAnswers.forEach(ans -> {
            assertEquals(ans.size(), 3);
            assertNotEquals(ans.get("x"), ans.get("z"));
        });

        String queryString = baseQueryString + "$x has name 'a';";

        String explicitString = "match " +
                "$x has name 'a';" +
                "{$y has name 'a';$z has name 'b';} or " +
                "{$y has name 'a';$z has name 'c';} or " +
                "{$y has name 'b';$z has name 'c';} or " +
                "{$y has name 'b';$z has name 'b';} or " +
                "{$y has name 'c';$z has name 'c';} or " +
                "{$y has name 'c';$z has name 'b';};";

        List<Answer> answers = qb.<GetQuery>parse(queryString + "get;").execute();
        List<Answer> answers2 = qb.infer(false).<GetQuery>parse(explicitString + "get;").execute();
        assertTrue(answers.containsAll(answers2));
        assertTrue(answers2.containsAll(answers));
    }

    /**
     * Tests a scenario in which the multiple neq predicates are present but bind at most single var in a relation.
     * Corresponds to the following pattern:
     *
     *              y       !=      z1
     *               ^              ^
     *                 \           /
     *                   \       /
     *                      x[a]
     *                   /      \
     *                 /          \
     *                v            v
     *              y2     !=      z2
     */
    @Test
    public void multipleRecursiveRelationsWithMultipleSharedNeqPredicates_symmetricPattern(){
        QueryBuilder qb = testSet29.tx().graql().infer(true);
        String baseQueryString = "match " +
                "(role1: $x, role2: $y1) isa binary-base;" +
                "(role1: $x, role2: $z1) isa binary-base;" +
                "(role1: $x, role2: $y2) isa binary-base;" +
                "(role1: $x, role2: $z2) isa binary-base;" +

                "$y1 != $z1;" +
                "$y2 != $z2;";

        List<Answer> baseAnswers = qb.<GetQuery>parse(baseQueryString + "get;").execute();
        assertEquals(baseAnswers.size(), 108);
        baseAnswers.forEach(ans -> {
            assertEquals(ans.size(), 5);
            assertNotEquals(ans.get("y1"), ans.get("z1"));
            assertNotEquals(ans.get("y2"), ans.get("z2"));
        });

        String queryString = baseQueryString + "$x has name 'a';";

        List<Answer> answers = qb.<GetQuery>parse(queryString + "get;").execute();
        assertEquals(answers.size(), 36);
        answers.forEach(ans -> {
            assertEquals(ans.size(), 5);
            assertNotEquals(ans.get("y1"), ans.get("z1"));
            assertNotEquals(ans.get("y2"), ans.get("z2"));
        });
    }

    /**
     * Tests a scenario in which a single relation has both variables bound with two different neq predicates.
     * Corresponds to the following pattern:
     *
     *                  x[a]  - != - >  z1
     *                  |
     *                  |
     *                  v
     *                  y     - != - >  z2
     */
    @Test
    public void multipleRecursiveRelationsWithMultipleSharedNeqPredicates(){
        QueryBuilder qb = testSet29.tx().graql().infer(true);
        String baseQueryString = "match " +
                "(role1: $x, role2: $y) isa binary-base;" +
                "$x != $z1;" +
                "$y != $z2;" +
                "(role1: $x, role2: $z1) isa binary-base;" +
                "(role1: $y, role2: $z2) isa binary-base;";

        List<Answer> baseAnswers = qb.<GetQuery>parse(baseQueryString + "get;").execute();
        assertEquals(baseAnswers.size(), 36);
        baseAnswers.forEach(ans -> {
            assertEquals(ans.size(), 4);
            assertNotEquals(ans.get("x"), ans.get("z1"));
            assertNotEquals(ans.get("y"), ans.get("z2"));
        });

        String queryString = baseQueryString + "$x has name 'a';";

        List<Answer> answers = qb.<GetQuery>parse(queryString + "get;").execute();
        assertEquals(answers.size(), 12);
        answers.forEach(ans -> {
            assertEquals(ans.size(), 4);
            assertNotEquals(ans.get("x"), ans.get("z1"));
            assertNotEquals(ans.get("y"), ans.get("z2"));
        });
    }

    @Test //tests whether shared resources are recognised correctly
    public void inferrableRelationWithRolePlayersSharingResource(){
        QueryBuilder qb = testSet29.tx().graql().infer(true);
        String queryString = "match " +
                "(role1: $x, role2: $y) isa binary-base;" +
                "$x has name $n;" +
                "$y has name $n;" +
                "get;";

        String queryString2 = "match " +
                "(role1: $x, role2: $y) isa binary-base;" +
                "$x has name $n;" +
                "$y has name $n;" +
                "$n val 'a';" +
                "get;";

        String queryString3 = "match " +
                "(role1: $x, role2: $y) isa binary-base;" +
                "$x has name 'a';" +
                "$y has name 'a';" +
                "get;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        List<Answer> answers2 = qb.<GetQuery>parse(queryString2).execute();
        List<Answer> answers3 = qb.<GetQuery>parse(queryString3).execute();

        assertEquals(answers.size(), 3);
        answers.forEach(ans -> {
            assertEquals(ans.size(), 3);
            assertEquals(ans.get("x"), ans.get("y"));
        });

        assertEquals(answers2.size(), 1);

        assertEquals(answers3.size(), 1);
        answers2.stream()
                .map(a -> a.project(Sets.newHashSet(var("x"), var("y"))))
                .forEach(a -> assertTrue(answers3.contains(a)));
    }

    @Test
    public void ternaryRelationsRequiryingDifferentMultiunifiers(){
        QueryBuilder qb = testSet29.tx().graql().infer(true);

        String queryString = "match " +
                "(role1: $a, role2: $b, role3: $c) isa ternary-base;" +
                "get;";

        String queryString2 = "match " +
                "(role: $a, role2: $b, role: $c) isa ternary-base;" +
                "$b has name 'b';" +
                "get;";

        String queryString3 = "match " +
                "($r: $a) isa ternary-base;" +
                "get;";

        String queryString4 = "match " +
                "($r: $b) isa ternary-base;" +
                "$b has name 'b';" +
                "get;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 27);

        List<Answer> answers2 = qb.<GetQuery>parse(queryString2).execute();
        assertEquals(answers2.size(), 9);

        List<Answer> answers3 = qb.<GetQuery>parse(queryString3).execute();
        assertEquals(answers3.size(), 12);

        List<Answer> answers4 = qb.<GetQuery>parse(queryString4).execute();
        assertEquals(answers4.size(), 4);
    }

    @Test
    public void binaryRelationWithDifferentVariantsOfVariableRoles(){
        QueryBuilder qb = testSet29.tx().graql().infer(true);

        //9 binary-base instances with {role, role2} = 2 roles for r2 -> 18 answers
        String queryString = "match " +
                "(role1: $a, $r2: $b) isa binary-base;" +
                "get;";

        String equivalentQueryString = "match " +
                "($r1: $a, $r2: $b) isa binary-base;" +
                "$r1 label 'role1';" +
                "get $a, $b, $r2;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        List<Answer> equivalentAnswers = qb.<GetQuery>parse(equivalentQueryString).execute();
        assertEquals(answers.size(), 18);
        assertTrue(CollectionUtils.isEqualCollection(answers, equivalentAnswers));

        //9 binary-base instances with {role, role1, role2} = 3 roles for r2 -> 27 answers
        String queryString2 = "match " +
                "(role: $a, $r2: $b) isa binary-base;" +
                "get;";

        String equivalentQueryString2 = "match " +
                "($r1: $a, $r2: $b) isa binary-base;" +
                "$r1 label 'role';" +
                "get $a, $b, $r2;";

        List<Answer> answers2 = qb.<GetQuery>parse(queryString2).execute();
        List<Answer> equivalentAnswers2 = qb.<GetQuery>parse(equivalentQueryString2).execute();
        assertEquals(answers2.size(), 27);
        assertTrue(CollectionUtils.isEqualCollection(answers2, equivalentAnswers2));

        //role variables bound hence should return original 9 instances
        String queryString3 = "match " +
                "($r1: $a, $r2: $b) isa binary-base;" +
                "$r1 label 'role';" +
                "$r2 label 'role2';" +
                "get $a, $b;";

        String equivalentQueryString3 = "match " +
                "(role1: $a, role2: $b) isa binary-base;" +
                "get;";

        List<Answer> answers3 = qb.<GetQuery>parse(queryString3).execute();
        List<Answer> equivalentAnswers3 = qb.<GetQuery>parse(equivalentQueryString3).execute();
        assertEquals(answers3.size(), 9);
        assertTrue(CollectionUtils.isEqualCollection(answers3, equivalentAnswers3));

        //9 relation instances with 7 possible permutations for each - 63 answers
        String queryString4 = "match " +
                "($r1: $a, $r2: $b) isa binary-base;" +
                "get;";

        List<Answer> answers4 = qb.<GetQuery>parse(queryString4).execute();
        assertEquals(answers4.size(), 63);
    }

    @Test
    public void binaryRelationWithVariableRoles_basicSet(){
        final int conceptDOF = 2;
        ternaryNaryRelationWithVariableRoles("binary", conceptDOF);
    }

    @Test
    public void binaryRelationWithVariableRoles_extendedSet(){
        final int conceptDOF = 3;
        ternaryNaryRelationWithVariableRoles("binary-base", conceptDOF);
    }

    @Test
    public void ternaryRelationWithVariableRoles_basicSet(){
        /*
        As each vertex is a starting point for {a, b, c} x {a, b c} = 9 relations, starting with a we have:

        (r1: a, r2: a, r3: a), (r1: a, r2: a, r3: b), (r1: a, r2: a, r3: c)
        (r1: a, r2: b, r3: a), (r1: a, r2: b, r3: b), (r1: a, r2: b, r3: c)
        (r1: a, r2: c, r3: a), (r1: a, r2: c, r3: b), (r1: a. r2: c, r3: c)

        If we generify two roles each of these produces 7 answers, taking (r1: a, r2: b, r3:c) we have:

        (a, r2: b, r3: c)
        (a, r: b, r3: c)
        (a, r2: b, r: c)
        (a, r3: c, r2: b)
        (a, r3: c, r: b)
        (a, r: c, r2: b)
        (a, r: b, r: c)

        plus
        (a, r: c, r: b) but this one is counted in (r1: a, r2: c, r3:b)
        hence 7 answers per single relation.
        */
        final int conceptDOF = 2;
        ternaryNaryRelationWithVariableRoles("ternary", conceptDOF);
    }

    @Test
    public void ternaryRelationWithVariableRoles_extendedSet(){
        final int conceptDOF = 3;
        ternaryNaryRelationWithVariableRoles("ternary-base", conceptDOF);
    }

    @Test
    public void quaternaryRelationWithVariableRoles_basicSet(){
        final int conceptDOF = 2;
        ternaryNaryRelationWithVariableRoles("quaternary", conceptDOF);
    }

    @Test
    public void quaternaryRelationWithVariableRoles2_extendedSet(){
        final int conceptDOF = 3;
        ternaryNaryRelationWithVariableRoles("quaternary-base", conceptDOF);
    }

    private void ternaryNaryRelationWithVariableRoles(String label, int conceptDOF){
        GraknTx graph = testSet29.tx();
        QueryBuilder qb = graph.graql().infer(true);
        final int arity = (int) graph.getRelationshipType(label).relates().count();

        VarPattern resourcePattern = var("a1").has("name", "a");

        //This query generalises all roles but the first one.
        VarPattern pattern = var().rel("role1", "a1");
        for(int i = 2; i <= arity ; i++) pattern = pattern.rel(var("r" + i), "a" + i);
        pattern = pattern.isa(label);

        List<Answer> answers = qb.match(pattern.and(resourcePattern)).get().execute();
        assertEquals(answers.size(), answerCombinations(arity-1, conceptDOF));

        //We get extra conceptDOF degrees of freedom by removing the resource constraint on $a1 and the set is symmetric.
        List<Answer> answers2 = qb.match(pattern).get().execute();
        assertEquals(answers2.size(), answerCombinations(arity-1, conceptDOF) * conceptDOF);


        //The general case of mapping all available Rps
        VarPattern generalPattern = var();
        for(int i = 1; i <= arity ; i++) generalPattern = generalPattern.rel(var("r" + i), "a" + i);
        generalPattern = generalPattern.isa(label);

        List<Answer> answers3 = qb.match(generalPattern).get().execute();
        assertEquals(answers3.size(), answerCombinations(arity, conceptDOF));
    }

    /**
     *Each role player variable can be mapped to either of the conceptDOF concepts and these can repeat.
     *Each role variable can be mapped to either of RPs roles and only meta roles can repeat.

     *For the case of conceptDOF = 3, roleDOF = 3.
     *We start by considering the number of meta roles we allow.
     *If we consider only non-meta roles, considering each relation player we get:
     *C^3_0 x 3.3 x 3.2 x 3 = 162 combinations
     *
     *If we consider single metarole - C^3_1 = 3 possibilities of assigning them:
     *C^3_1 x 3.3 x 3.2 x 3 = 486 combinations
     *
     *Two metaroles - again C^3_2 = 3 possibilities of assigning them:
     *C^3_2 x 3.3 x 3   x 3 = 243 combinations
     *
     *Three metaroles, C^3_3 = 1 possiblity of assignment:
     *C^3_3 x 3   x 3   x 3 = 81 combinations
     *
     *-> Total = 918 different answers
     *In general, for i allowed meta roles we have:
     *C^{RP}_i PRODUCT_{j = RP-i}{ (conceptDOF)x(roleDOF-j) } x PRODUCT_i{ conceptDOF} } answers.
     *
     *So total number of answers is:
     *SUM_i{ C^{RP}_i PRODUCT_{j = RP-i}{ (conceptDOF)x(roleDOF-j) } x PRODUCT_i{ conceptDOF} }
     *
     * @param RPS number of relation players available
     * @param conceptDOF number of concept degrees of freedom
     * @return number of answer combinations
     */
    private int answerCombinations(int RPS, int conceptDOF) {
        int answers = 0;
        //i is the number of meta roles
        for (int i = 0; i <= RPS; i++) {
            int RPProduct = 1;
            //rps with non-meta roles
            for (int j = 0; j < RPS - i; j++) RPProduct *= conceptDOF * (RPS - j);
            //rps with meta roles
            for (int k = 0; k < i; k++) RPProduct *= conceptDOF;
            answers += CombinatoricsUtils.binomialCoefficient(RPS, i) * RPProduct;
        }
        return answers;
    }

    @Test //tests scenario where rules define mutually recursive relation and resource and we query for an attributed type corresponding to the relation
    public void mutuallyRecursiveRelationAndResource_queryForAttributedType(){
        QueryBuilder qb = testSet30.tx().graql().infer(true);

        String queryString = "match $p isa pair, has name 'ff'; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 16);
    }
}
