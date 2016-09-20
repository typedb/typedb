package io.mindmaps.graql.reasoner.inference;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.Reasoner;
<<<<<<< HEAD
import io.mindmaps.graql.internal.reasoner.query.QueryAnswers;
import io.mindmaps.graql.reasoner.graphs.GenericGraph;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static io.mindmaps.graql.internal.reasoner.Utility.printAnswers;
import static io.mindmaps.graql.internal.reasoner.Utility.printMatchQueryResults;
import static org.junit.Assert.assertEquals;

public class MoogiInferenceTest{
=======
import io.mindmaps.graql.reasoner.graphs.GenericGraph;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MoogiInferenceTest {
>>>>>>> 7067d4a7520f36db8e4b5e8c13c1e80d80d1cf7b

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
<<<<<<< HEAD
        graph = GenericGraph.getGraph(schemaFile/*, entityFile, assertionFile2, assertionFile, ruleFile*/);
=======
        graph = GenericGraph.getGraph(schemaFile, entityFile, assertionFile2, assertionFile, ruleFile);
>>>>>>> 7067d4a7520f36db8e4b5e8c13c1e80d80d1cf7b
        reasoner = new Reasoner(graph);
        qb = Graql.withGraph(graph);
    }

    @Test
    public void test() {

        String queryString = "match (production-with-genre: $x, genre-of-production: $y) isa has-genre;" +
                             "$x isa movie; $y isa genre; $y has description 'science fiction'; select $x";
        MatchQuery query = qb.parseMatch(queryString);

<<<<<<< HEAD
        QueryAnswers answers = reasoner.resolve(query);

=======
>>>>>>> 7067d4a7520f36db8e4b5e8c13c1e80d80d1cf7b
        String explicitQuery = "match (production-with-genre: $x, genre-of-production: $y) isa has-genre;"+
                            "$y has description 'Sci-Fi' or $y has description 'science fiction' or $y has description 'Science Fiction';" +
                            "$x isa movie; select $x";

<<<<<<< HEAD
        assertEquals(answers, Sets.newHashSet(qb.parseMatch(explicitQuery)));
=======
        assertEquals(reasoner.resolve(query), Sets.newHashSet(qb.parseMatch(explicitQuery)));
>>>>>>> 7067d4a7520f36db8e4b5e8c13c1e80d80d1cf7b
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
