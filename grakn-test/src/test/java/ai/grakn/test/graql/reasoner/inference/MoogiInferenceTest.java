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

package ai.grakn.test.graql.reasoner.inference;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Reasoner;
import ai.grakn.graql.internal.reasoner.query.Query;
import ai.grakn.test.graql.reasoner.graphs.TestGraph;
import ai.grakn.graql.QueryBuilder;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

@Ignore
public class MoogiInferenceTest {

    private static GraknGraph graph;
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
        graph = TestGraph.getGraph("name", schemaFile, entityFile, assertionFile2, assertionFile, ruleFile);
        reasoner = new Reasoner(graph);
        qb = graph.graql();
    }

    @Test
    public void test() {

        String queryString = "match (production-with-genre: $x, genre-of-production: $y) isa has-genre;" +
                             "$x isa movie; $y isa genre; $y has description 'science fiction'; select $x;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match (production-with-genre: $x, genre-of-production: $y) isa has-genre;"+
                            "$y has description 'Sci-Fi' or $y has description 'science fiction' or $y has description 'Science Fiction';" +
                            "$x isa movie; select $x;";

        assertQueriesEqual(reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    @Test
    public void testDecentPopularMovie(){
        String queryString = "match $x has status 'decent popular movie';";
        String explicitQuery = "match $x isa movie;"+
                                "{$x has tmdb-vote-count > 1000.0;} or {$x has rotten-tomatoes-user-total-votes > 25000;};" +
                                "$x has rotten-tomatoes-user-rating >= 3.0;";
        MatchQuery query = qb.parse(queryString);
        assertQueriesEqual(reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        }

    @Test
    public void testBadPopularMovie(){
        String queryString = "match $x has status 'bad popular movie';";
        String explicitQuery = "match $x isa movie;$x has tmdb-vote-count > 1000.0;$x has tmdb-vote-average < 4.0;";
        MatchQuery query = qb.parse(queryString);
        assertQueriesEqual(reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        }

    @Test
    public void testActorDirector(){
        String queryString = "match $x has status 'actor-director';";
        String explicitQuery = "match (actor: $x) isa has-cast;(director: $x) isa production-crew;";
        Query query = new Query(queryString, graph);
        assertQueriesEqual(reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    @Test
    public void testPopularActor(){
        String queryString = "match $x has status 'popular actor';";
        String explicitQuery = "match $y has rotten-tomatoes-user-total-votes > 25000;" +
                               "(actor: $x, production-with-cast: $y) isa has-cast; select $x;";
        MatchQuery query = qb.parse(queryString);
        assertQueriesEqual(reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    @Test
    public void testPerson(){
        String queryString = "match $x isa person has name;";
        MatchQuery query = qb.parse(queryString);
        reasoner.resolve(query, true);
    }

    @Test
    public void testRelated(){
        String queryString = "match $x isa movie;$x has status 'decent movie';($x, director: $y);";
        String explicitQuery = "match $x isa movie;$x has rotten-tomatoes-user-rating >= 3.0;($x, director: $y);";

        MatchQuery query = qb.parse(queryString);
        assertQueriesEqual(reasoner.resolve(query, false), qb.<MatchQuery>parse(explicitQuery).stream());
        assertQueriesEqual(reasoner.resolve(query, true), qb.<MatchQuery>parse(explicitQuery).stream());
    }

    private void assertQueriesEqual(Stream<Map<String, Concept>> s1, Stream<Map<String, Concept>> s2) {
        assertEquals(s1.collect(Collectors.toSet()), s2.collect(Collectors.toSet()));
    }
}
