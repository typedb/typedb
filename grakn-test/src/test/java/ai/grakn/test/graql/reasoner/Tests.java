package ai.grakn.test.graql.reasoner;

import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.test.GraphContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.stream.Collectors;

import static ai.grakn.test.GraknTestEnv.usingTinker;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class Tests{


    @ClassRule
    public static final GraphContext testSet5 = GraphContext.preLoad("testSet5.gql");

    @Rule
    public final GraphContext graphContext = GraphContext.empty();

    @Before
    public void onStartup() throws Exception {
        assumeTrue(usingTinker());
    }
    
    @Test
    public void testSet5() {
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

    private QueryAnswers queryAnswers(MatchQuery query) {
        return new QueryAnswers(query.admin().results());
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(q1.stream().collect(Collectors.toSet()), q2.stream().collect(Collectors.toSet()));
    }
}
