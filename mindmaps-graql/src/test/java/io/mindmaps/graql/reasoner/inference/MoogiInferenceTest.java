package io.mindmaps.graql.reasoner.inference;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.Reasoner;

import io.mindmaps.graql.reasoner.graphs.GenericGraph;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MoogiInferenceTest {

    private static MindmapsGraph graph;
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
        graph = GenericGraph.getGraph(schemaFile, entityFile, assertionFile2, assertionFile, ruleFile);
        reasoner = new Reasoner(graph);
        qb = Graql.withGraph(graph);
    }

    @Test
    public void test() {

        String queryString = "match (production-with-genre: $x, genre-of-production: $y) isa has-genre;" +
                             "$x isa movie; $y isa genre; $y has description 'science fiction'; select $x";
        MatchQuery query = qb.parseMatch(queryString);

        String explicitQuery = "match (production-with-genre: $x, genre-of-production: $y) isa has-genre;"+
                            "$y has description 'Sci-Fi' or $y has description 'science fiction' or $y has description 'Science Fiction';" +
                            "$x isa movie; select $x";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
    }

    @Test
    public void testActorDirector(){
        String queryString = "match $x has status 'actor-director';";
        String explicitQuery = "match (actor: $x) isa has-cast;(director: $x) isa production-crew;";

        MatchQuery query = qb.parseMatch(queryString);

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parseMatch(explicitQuery));
    }

    @Test
    public void testPerson(){
        String queryString = "match $x isa person has name;";
        MatchQuery query = qb.parseMatch(queryString);
        MatchQuery mq = reasoner.resolveToQuery(query);
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }


}
