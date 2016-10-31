package io.grakn.test.graql.reasoner.inference;

import com.google.common.collect.Sets;
import io.grakn.MindmapsGraph;
import io.grakn.graql.Graql;
import io.grakn.graql.MatchQuery;
import io.grakn.graql.QueryBuilder;
import io.grakn.graql.Reasoner;
import io.grakn.graql.internal.reasoner.query.Query;
import io.grakn.test.graql.reasoner.graphs.GenericGraph;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MoogiInferenceTest {

    private static MindmapsGraph graph;
    private static Reasoner reasoner;
    private static QueryBuilder qb;
    private final static String dataDir = "femtomoogi/";
    private final static String schemaFile = dataDir + "schema.gql";
    private final static String entityFile = dataDir + "entities-new.gql";
    private final static String assertionFile = dataDir + "assertions_2-new.gql";
    private final static String assertionFile2 = dataDir + "assertions_3-new.gql";
    private final static String ruleFile = dataDir + "rules.gql";

    @BeforeClass
    public static void setUpClass() {
        graph = GenericGraph.getGraph(schemaFile, entityFile, assertionFile2, assertionFile, ruleFile);
        reasoner = new Reasoner(graph);
        qb = Graql.withGraph(graph);
    }

    @Test
    public void test() {

        String queryString = "match (production-with-genre: $x, genre-of-production: $y) isa has-genre;" +
                             "$x isa movie; $y isa genre; $y has description 'science fiction'; select $x;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match (production-with-genre: $x, genre-of-production: $y) isa has-genre;"+
                            "$y has description 'Sci-Fi' or $y has description 'science fiction' or $y has description 'Science Fiction';" +
                            "$x isa movie; select $x;";

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    @Test
    public void testDecentPopularMovie(){
        String queryString = "match $x has status 'decent popular movie';";
        String explicitQuery = "match $x isa movie;"+
                                "{$x has tmdb-vote-count > 1000.0;} or {$x has rotten-tomatoes-user-total-votes > 25000;};" +
                                "$x has rotten-tomatoes-user-rating >= 3.0;";
        MatchQuery query = qb.parse(queryString);
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        }

    @Test
    public void testBadPopularMovie(){
        String queryString = "match $x has status 'bad popular movie';";
        String explicitQuery = "match $x isa movie;$x has tmdb-vote-count > 1000.0;$x has tmdb-vote-average < 4.0;";
        MatchQuery query = qb.parse(queryString);
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        }

    @Test
    public void testActorDirector(){
        String queryString = "match $x has status 'actor-director';";
        String explicitQuery = "match (actor: $x) isa has-cast;(director: $x) isa production-crew;";
        Query query = new Query(queryString, graph);

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    @Test
    public void testPopularActor(){
        String queryString = "match $x has status 'popular actor';";
        String explicitQuery = "match $y has rotten-tomatoes-user-total-votes > 25000;" +
                               "(actor: $x, production-with-cast: $y) isa has-cast; select $x;";
        MatchQuery query = qb.parse(queryString);

        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    @Test
    public void testPerson(){
        String queryString = "match $x isa person has name;";
        MatchQuery query = qb.parse(queryString);
        MatchQuery mq = reasoner.resolveToQuery(query);
    }

    @Test
    public void testRelated(){
        String queryString = "match $x isa movie;$x has status 'decent movie';($x, director: $y);";
        String explicitQuery = "match $x isa movie;$x has rotten-tomatoes-user-rating >= 3.0;($x, director: $y);";

        MatchQuery query = qb.parse(queryString);
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));
        assertQueriesEqual(reasoner.resolveToQuery(query), qb.parse(explicitQuery));
    }

    private void assertQueriesEqual(MatchQuery q1, MatchQuery q2) {
        assertEquals(Sets.newHashSet(q1), Sets.newHashSet(q2));
    }


}
