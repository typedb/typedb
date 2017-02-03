package ai.grakn.test.graql.reasoner;

import ai.grakn.concept.Concept;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.test.GraphContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.reasoner.Utility.printAnswers;
import static ai.grakn.test.GraknTestEnv.usingTinker;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class Tests{

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

    @Rule
    public final GraphContext graphContext = GraphContext.empty();

    @Before
    public void onStartup() throws Exception {
        assumeTrue(usingTinker());
    }

    @Test
    public void unificationWithVarDuplicates() {
        QueryBuilder qb = testSet1.graph().graql().infer(true);
        String query1String = "match (role1:$x, role2:$x);";
        String query2String = "match (role1:$x, role2:$y);";
        QueryAnswers answers1 = queryAnswers(qb.parse(query1String));
        QueryAnswers answers2 = queryAnswers(qb.parse(query2String));

        answers2.forEach(x -> answers1.forEach(y -> Assert.assertTrue(x.values().containsAll(y.values()))));
        answers2.forEach(x -> Assert.assertTrue(x.keySet().size() ==2));
        answers1.forEach(x -> Assert.assertTrue(x.keySet().size() ==1));
    }

    @Test
    public void generatingMultipleIsaEdges() {
        QueryBuilder qb = testSet2.graph().graql().infer(true);
        String query1String = "match $x isa entity2;";
        QueryAnswers answers1 = queryAnswers(qb.parse(query1String));

        Assert.assertEquals(answers1.size(), 1);
    }

    @Test
    public void generatingFreshEntity() {
        QueryBuilder qb = testSet3.graph().graql().infer(true);
        String query1String = "match $x isa entity1;";
        String query2String = "match $x isa entity2;";
        QueryAnswers answers1 = queryAnswers(qb.parse(query1String));
        QueryAnswers answers2 = queryAnswers(qb.parse(query2String));
        Assert.assertTrue(!(answers1.containsAll(answers2)&&answers2.containsAll(answers1)));
    }

    @Test
    public void generatingIsaEdge() {
        QueryBuilder qb = testSet4.graph().graql().infer(true);
        String query1String = "match $x isa entity1;";
        String query2String = "match $x isa entity2;";
        QueryAnswers answers1 = queryAnswers(qb.parse(query1String));
        QueryAnswers answers2 = queryAnswers(qb.parse(query2String));
        Assert.assertTrue(answers1.containsAll(answers2)&&answers2.containsAll(answers1));
    }


    @Test
    public void generatingFreshEntity2() {
        QueryBuilder qb = testSet5.graph().graql().infer(false);
        QueryBuilder iqb = testSet5.graph().graql().infer(true);
        String queryString = "match $x isa entity2;";
        String explicitQuery = "match $x isa entity1;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));
        QueryAnswers answers2 = queryAnswers(qb.parse(explicitQuery));

        Assert.assertTrue(!answers2.containsAll(answers));
        Assert.assertTrue(!answers.isEmpty());
        Assert.assertEquals(answers2.size(), 3);
    }

    @Test
    public void generatingFreshRelation() {
        QueryBuilder qb = testSet6.graph().graql().infer(true);
        String queryString = "match $x isa relation1;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));

        Assert.assertEquals(answers.size(), 3);
    }

    @Test
    public void distinctLimitedAnswersOfInfinitelyGeneratingRule() {
        QueryBuilder qb = testSet7.graph().graql().infer(true);
        String queryString = "match $x isa relation1; limit 50;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        Assert.assertEquals(answers.size(), 50);
    }

    @Test
    public void roleUnificationWithRoleHierarchiesInvolved1() {
        QueryBuilder qb = testSet8.graph().graql().infer(true);
        String queryString = "match (role2:$x, role3:$y) isa relation2;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        answers.forEach(y -> Assert.assertTrue(y.values().size()<=1));
    }

    @Test
    public void roleUnificationWithRoleHierarchiesInvolved2() {
        QueryBuilder qb = testSet9.graph().graql().infer(true);
        String queryString = "match (role1:$x, role1:$y) isa relation1;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        answers.forEach(y -> Assert.assertTrue(y.values().size()<=1));
    }

    @Test
    public void transRelationWithEntityGuardsAtBothEnds() {
        QueryBuilder iqb = testSet10.graph().graql().infer(true);
        String queryString = "match (role1: $x, role2: $y) isa relation2;";
        QueryAnswers answers = queryAnswers(iqb.parse(queryString));

        Assert.assertTrue(!answers.isEmpty());
    }

    @Test
    public void transRelationWithRelationGuardsAtBothEnds() {
        QueryBuilder qb = testSet11.graph().graql().infer(true);
        String queryString = "match (role1:$x, role2:$y) isa relation3;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        Assert.assertEquals(answers.size(), 1);
    }

    @Test
    public void circularRuleDependencies() {
        QueryBuilder qb = testSet12.graph().graql().infer(true);
        String queryString = "match (role1:$x, role2:$y) isa relation3;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        Assert.assertEquals(answers.size(), 2);
    }

    @Test
    public void rulesInteractingWithTypeHierarchy() {
        QueryBuilder qb = testSet13.graph().graql().infer(true);
        String queryString = "match (role1:$x, role2:$y) isa relation2;";
        QueryAnswers answers = queryAnswers(qb.parse(queryString));
        Assert.assertEquals(answers.size(), 1);
    }

    private QueryAnswers queryAnswers(MatchQuery query) {
        return new QueryAnswers(query.admin().results());
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(q1.stream().collect(Collectors.toSet()), q2.stream().collect(Collectors.toSet()));
    }
}
