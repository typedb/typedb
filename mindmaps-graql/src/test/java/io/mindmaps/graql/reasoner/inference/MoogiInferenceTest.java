package io.mindmaps.graql.reasoner.inference;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.Reasoner;
import io.mindmaps.graql.internal.reasoner.query.QueryAnswers;
import io.mindmaps.graql.reasoner.graphs.GenericGraph;
import io.mindmaps.graql.reasoner.graphs.SNBGraph;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.mindmaps.graql.internal.reasoner.Utility.printAnswers;
import static io.mindmaps.graql.internal.reasoner.Utility.printMatchQueryResults;
import static org.junit.Assert.assertEquals;

public class MoogiInferenceTest{

    private static Reasoner reasoner;
    private static QueryBuilder qb;
    private static String dataDir = "femtomoogi/";
    private static String schemaFile = dataDir + "schema.gql";
    private static String entityFile = dataDir + "entities-new.gql";
    private static String assertionFile = dataDir + "assertions_2-new.gql";
    private static String assertionFile2 = dataDir + "assertions_3-new.gql";
    private static String ruleFile = dataDir + "rules.gql";

    @BeforeClass
    public static void setUpClass() {
        MindmapsGraph graph = GenericGraph.getGraph(schemaFile, entityFile, assertionFile2, assertionFile, ruleFile);
        reasoner = new Reasoner(graph);
        qb = Graql.withGraph(graph);
    }

    @Test
    public void test() {

        String queryString = "match $z isa genre; $z has description 'science fiction'";
        MatchQuery query = qb.parseMatch(queryString);

        QueryAnswers answers = reasoner.resolve(query);

        String testQuery = "match (production-with-genre: $x, genre-of-production: $y) isa has-genre;"+
                            "$y has description 'Sci-Fi';$x isa movie; select $x(has title)";
        printMatchQueryResults(qb.parseMatch(testQuery));

        printAnswers(answers);

        /*
        String explicitQuery = "match " +
                "$x isa university;$x id 'University of Cambridge';$y isa country;$y id 'UK'";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
           */
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }


}
