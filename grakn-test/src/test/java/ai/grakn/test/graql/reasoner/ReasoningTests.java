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

package ai.grakn.test.graql.reasoner;

import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.reasoner.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.test.GraphContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;


import static ai.grakn.test.GraknTestEnv.usingTinker;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assume.assumeTrue;

/**
 * Suite of tests checking different meanders and aspects of reasoning - full reasoning cycle is being tested.
 */
public class ReasoningTests {

    @ClassRule
    public static final GraphContext testSet1 = GraphContext.preLoad("testSet1.gql");

    @ClassRule
    public static final GraphContext testSet2 = GraphContext.preLoad("testSet2.gql");

    @ClassRule
    public static final GraphContext testSet3 = GraphContext.preLoad("testSet3.gql");

    @ClassRule
    public static final GraphContext testSet4 = GraphContext.preLoad("testSet4.gql");

    @ClassRule
    public static final GraphContext testSet5 = GraphContext.preLoad("testSet5.gql");

    @ClassRule
    public static final GraphContext testSet6 = GraphContext.preLoad("testSet6.gql");

    @ClassRule
    public static final GraphContext testSet7 = GraphContext.preLoad("testSet7.gql");

    @ClassRule
    public static final GraphContext testSet8 = GraphContext.preLoad("testSet8.gql");

    @ClassRule
    public static final GraphContext testSet9 = GraphContext.preLoad("testSet9.gql");

    @ClassRule
    public static final GraphContext testSet10 = GraphContext.preLoad("testSet10.gql");

    @ClassRule
    public static final GraphContext testSet11 = GraphContext.preLoad("testSet11.gql");

    @ClassRule
    public static final GraphContext testSet12 = GraphContext.preLoad("testSet12.gql");

    @ClassRule
    public static final GraphContext testSet13 = GraphContext.preLoad("testSet13.gql");

    @ClassRule
    public static final GraphContext testSet14 = GraphContext.preLoad("testSet14.gql");

    @ClassRule
    public static final GraphContext testSet15 = GraphContext.preLoad("testSet15.gql");

    @ClassRule
    public static final GraphContext testSet16 = GraphContext.preLoad("testSet16.gql");

    @ClassRule
    public static final GraphContext testSet17 = GraphContext.preLoad("testSet17.gql");

    @ClassRule
    public static final GraphContext testSet18 = GraphContext.preLoad("testSet18.gql");

    @Before
    public void onStartup() throws Exception {
        assumeTrue(usingTinker());
    }

    //The tests validate the correctness of the rule reasoning implementation w.r.t. the intended semantics of rules.
    //The ignored tests reveal some bugs in the reasoning algorithm, as they don't return the expected results,
    //as specified in the respective comments below.

    @Ignore
    @Test //Expected result: Both queries should return a non-empty result, with $x/$y mapped to a unique entity.
    public void unificationWithVarDuplicates() {
        QueryBuilder qb = testSet1.graph().graql().infer(true);
        String query1String = "match (role1:$x, role2:$x);";
        String query2String = "match (role1:$x, role2:$y);";
        QueryAnswers answers1 = queryAnswers(qb.parse(query1String));
        QueryAnswers answers2 = queryAnswers(qb.parse(query2String));

        assertNotEquals(answers1.size() * answers2.size(), 0);
        answers1.forEach(x -> Assert.assertTrue(x.keySet().size() ==1));
        answers2.forEach(x -> answers1.forEach(y -> Assert.assertTrue(x.values().containsAll(y.values()))));
        answers2.forEach(x -> Assert.assertTrue(x.keySet().size() ==2));

    }

    @Test //Expected result: The query should return a unique match.
    public void generatingMultipleIsaEdges() {
        QueryBuilder qb = testSet2.graph().graql().infer(true);
        String query1String = "match $x isa entity2;";
        QueryAnswers answers1 = queryAnswers(qb.parse(query1String));
        assertEquals(answers1.size(), 1);
    }

    @Test //Expected result: The queries should return different matches, unique per query.
    public void generatingFreshEntity() {
        QueryBuilder qb = testSet3.graph().graql().infer(true);
        String query1String = "match $x isa entity1;";
        String query2String = "match $x isa entity2;";
        QueryAnswers answers1 = queryAnswers(qb.parse(query1String));
        QueryAnswers answers2 = queryAnswers(qb.parse(query2String));
        Assert.assertEquals(answers1.size(), answers2.size());
        assertNotEquals(answers1, answers2);
    }

    @Test //Expected result: The queries should return the same two matches.
    public void generatingIsaEdge() {
        QueryBuilder qb = testSet4.graph().graql().infer(true);
        String query1String = "match $x isa entity1;";
        String query2String = "match $x isa entity2;";
        QueryAnswers answers1 = queryAnswers(qb.parse(query1String));
        QueryAnswers answers2 = queryAnswers(qb.parse(query2String));
        Assert.assertEquals(answers1, answers2);
    }

    @Test //Expected result: The query should return a unique match (or possibly nothing if we enforce range-restriction).
    public void generatingFreshEntity2() {
        QueryBuilder qb = testSet5.graph().graql().infer(false);
        QueryBuilder iqb = testSet5.graph().graql().infer(true);
        String queryString = "match $x isa entity2;";
        String explicitQuery = "match $x isa entity1;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        QueryAnswers answers2 = queryAnswers(qb.parse(explicitQuery));

        Assert.assertTrue(!answers2.containsAll(answers));
        Assert.assertTrue(!answers.isEmpty());
        assertEquals(answers2.size(), 3);
    }

    @Test //Expected result: The query should return three different instances of relation1 with unique ids.
    public void generatingFreshRelation() {
        QueryBuilder qb = testSet6.graph().graql().infer(true);
        String queryString = "match $x isa relation1;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        assertEquals(answers.size(), 3);
    }

    @Test //Expected result: The query should return 10 unique matches (no duplicates).
    public void distinctLimitedAnswersOfInfinitelyGeneratingRule() {
        QueryBuilder iqb = testSet7.graph().graql().infer(true);
        QueryBuilder qb = testSet7.graph().graql().infer(true);
        String queryString = "match $x isa relation1; limit 10;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        assertEquals(answers.size(), 10);
        assertEquals(answers.size(), queryAnswers(qb.parse(queryString)).size());

    }

    @Test //Expected result: The query should not return any matches (or possibly return a single match with $x=$y)
    public void roleUnificationWithRoleHierarchiesInvolved1() {
        QueryBuilder qb = testSet8.graph().graql().infer(true);
        String queryString = "match (role2:$x, role3:$y) isa relation2;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        answers.forEach(y -> Assert.assertTrue(y.values().size()<=1));
    }

    @Test //Expected result: The query should not return any matches (or possibly return a single match with $x=$y)
    public void roleUnificationWithRoleHierarchiesInvolved2() {
        QueryBuilder qb = testSet9.graph().graql().infer(true);
        String queryString = "match (role1:$x, role1:$y) isa relation2;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        answers.forEach(y -> Assert.assertTrue(y.values().size()<=1));
    }

    @Test //Expected result: The query should return a single match
    public void roleUnificationWithRoleHierarchiesInvolved3() {
        QueryBuilder qb = testSet9.graph().graql().infer(true);
        String queryString = "match (role1:$x) isa relation2;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        assertEquals(answers.size(), 1);
    }

    /**
     * recursive relation having same type for different role players
     * tests for handling recursivity and equivalence of queries and relations
     */
    @Test //Expected result: The query should return a unique match
    public void transRelationWithEntityGuardsAtBothEnds() {
        QueryBuilder iqb = testSet10.graph().graql().infer(true);
        String queryString = "match (role1: $x, role2: $y) isa relation2;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        assertEquals(answers.size(), 1);
    }

    @Test //Expected result: The query should return a unique match
    public void transRelationWithRelationGuardsAtBothEnds() {
        QueryBuilder qb = testSet11.graph().graql().infer(true);
        String queryString = "match (role1:$x, role2:$y) isa relation3;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        assertEquals(answers.size(), 1);
    }

    @Test //Expected result: The query should return two unique matches
    public void circularRuleDependencies() {
        QueryBuilder qb = testSet12.graph().graql().infer(true);
        String queryString = "match (role1:$x, role2:$y) isa relation3;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        assertEquals(answers.size(), 2);
    }

    @Ignore
    @Test //Expected result: The query should return a unique match
    public void rulesInteractingWithTypeHierarchy() {
        QueryBuilder qb = testSet13.graph().graql().infer(true);
        String queryString = "match (role1:$x, role2:$y) isa relation2;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        assertEquals(answers.size(), 1);
    }

    //TODO
    @Ignore
    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources1() {
        QueryBuilder qb = testSet14.graph().graql().infer(true);
        String queryString1 = "match $x isa entity1, has res1 $y;";
        QueryAnswers answers1 = queryAnswers(qb.parse(queryString1));
        String queryString2 = "match $x isa res1;";
        QueryAnswers answers2 = queryAnswers(qb.parse(queryString2));

        assertEquals(answers1.size(), 2);
        assertEquals(answers2.size(), 1);
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources2() {
        QueryBuilder qb = testSet15.graph().graql().infer(true);
        String queryString1 = "match $x isa entity1, has res2 $y;";
        QueryAnswers answers1 = queryAnswers(qb.parse(queryString1));
        assertEquals(answers1.size(), 1);
        String queryString2 = "match $x isa res2;";
        QueryAnswers answers2 = queryAnswers(qb.parse(queryString2));
        assertEquals(answers2.size(), 1);
        Assert.assertTrue(answers2.iterator().next().get(VarName.of("x")).isResource());
        String queryString3 = "match $x isa res1; $y isa res2;";
        QueryAnswers answers3 = queryAnswers(qb.parse(queryString3));
        assertEquals(answers3.size(), 1);

        Assert.assertTrue(answers3.iterator().next().get(VarName.of("x")).isResource());
        Assert.assertTrue(answers3.iterator().next().get(VarName.of("y")).isResource());
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources3() {
        QueryBuilder qb = testSet16.graph().graql().infer(true);
        String queryString1 = "match $x isa entity1, has res1 $y; $z isa relation1;";
        QueryAnswers answers1 = queryAnswers(qb.parse(queryString1));
        assertEquals(answers1.size(), 1);
        answers1.forEach(ans ->
                {
                    Assert.assertTrue(ans.get(VarName.of("x")).isEntity());
                    Assert.assertTrue(ans.get(VarName.of("y")).isResource());
                    Assert.assertTrue(ans.get(VarName.of("z")).isRelation());
                }
        );
        String queryString2 = "match $x isa relation1, has res1 $y;";
        QueryAnswers answers2 = queryAnswers(qb.parse(queryString2));
        assertEquals(answers2.size(), 1);
        answers2.forEach(ans ->
                {
                    Assert.assertTrue(ans.get(VarName.of("x")).isRelation());
                    Assert.assertTrue(ans.get(VarName.of("y")).isResource());
                }
        );
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources4() {
        QueryBuilder qb = testSet17.graph().graql().infer(true);
        String queryString1 = "match $x isa general-entity has res2 $r;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString1));
        assertEquals(answers.size(), 1);
    }

    @Ignore
    @Test //Expected result: Two answers with obtained only if checked the instance type compatible with the one specified with rule body.
    public void instanceTypeHierarchyRespected(){
        QueryBuilder qb = testSet18.graph().graql().infer(true);
        String queryString1 = "match $x isa entity1;(role1: $x, role2: $y) isa relation1;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString1));
        assertEquals(answers.size(), 2);
    }

    private QueryAnswers queryAnswers(MatchQuery query) {
        return new QueryAnswers(query.admin().streamWithVarNames().map(QueryAnswer::new).collect(toSet()));
    }
}
