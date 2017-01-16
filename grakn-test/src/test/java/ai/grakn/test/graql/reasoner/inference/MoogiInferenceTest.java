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

import ai.grakn.concept.Concept;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.internal.reasoner.Reasoner;
import ai.grakn.graql.VarName;
import ai.grakn.test.GraphContext;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

@Ignore
public class MoogiInferenceTest {

    private static QueryBuilder qb;
    private final static String dataDir = "femtomoogi/";
    private final static String schemaFile = dataDir + "schema.gql";
    private final static String entityFile = dataDir + "entities-new.gql";
    private final static String assertionFile = dataDir + "assertions_2-new.gql";
    private final static String assertionFile2 = dataDir + "assertions_3-new.gql";
    private final static String ruleFile = dataDir + "rules.gql";

    @ClassRule
    public static final GraphContext moogiGraph = GraphContext.preLoad(schemaFile, entityFile, assertionFile2, assertionFile, ruleFile);

    @BeforeClass
    public static void setUpClass() {
        qb = moogiGraph.graph().graql();
    }

    @Test
    public void test() {

        String queryString = "match (production-with-genre: $x, genre-of-production: $y) isa has-genre;" +
                             "$x isa movie; $y isa genre; $y has description 'science fiction'; select $x;";
        MatchQuery query = qb.parse(queryString);

        String explicitQuery = "match (production-with-genre: $x, genre-of-production: $y) isa has-genre;"+
                            "$y has description 'Sci-Fi' or $y has description 'science fiction' or $y has description 'Science Fiction';" +
                            "$x isa movie; select $x;";

        assertQueriesEqual(Reasoner.resolve(query, false), qb.parse(explicitQuery));
        assertQueriesEqual(Reasoner.resolve(query, true), qb.parse(explicitQuery));
    }

    @Test
    public void testDecentPopularMovie(){
        String queryString = "match $x has status 'decent popular movie';";
        String explicitQuery = "match $x isa movie;"+
                                "{$x has tmdb-vote-count > 1000.0;} or {$x has rotten-tomatoes-user-total-votes > 25000;};" +
                                "$x has rotten-tomatoes-user-rating >= 3.0;";
        MatchQuery query = qb.parse(queryString);
        assertQueriesEqual(Reasoner.resolve(query, false), qb.parse(explicitQuery));
        }

    @Test
    public void testBadPopularMovie(){
        String queryString = "match $x has status 'bad popular movie';";
        String explicitQuery = "match $x isa movie;$x has tmdb-vote-count > 1000.0;$x has tmdb-vote-average < 4.0;";
        MatchQuery query = qb.parse(queryString);
        assertQueriesEqual(Reasoner.resolve(query, false), qb.parse(explicitQuery));
        }

    @Test
    public void testActorDirector(){
        String queryString = "match $x has status 'actor-director';";
        String explicitQuery = "match (actor: $x) isa has-cast;(director: $x) isa production-crew;";
        MatchQuery query = qb.parse(queryString);
        assertQueriesEqual(Reasoner.resolve(query, false), qb.parse(explicitQuery));
        assertQueriesEqual(Reasoner.resolve(query, true), qb.parse(explicitQuery));
    }

    @Test
    public void testPopularActor(){
        String queryString = "match $x has status 'popular actor';";
        String explicitQuery = "match $y has rotten-tomatoes-user-total-votes > 25000;" +
                               "(actor: $x, production-with-cast: $y) isa has-cast; select $x;";
        MatchQuery query = qb.parse(queryString);
        assertQueriesEqual(Reasoner.resolve(query, false), qb.parse(explicitQuery));
        assertQueriesEqual(Reasoner.resolve(query, true), qb.parse(explicitQuery));
    }

    @Test
    public void testPerson(){
        String queryString = "match $x isa person has name;";
        MatchQuery query = qb.parse(queryString);
        Reasoner.resolve(query, true);
    }

    @Test
    public void testRelated(){
        String queryString = "match $x isa movie;$x has status 'decent movie';($x, director: $y);";
        String explicitQuery = "match $x isa movie;$x has rotten-tomatoes-user-rating >= 3.0;($x, director: $y);";

        MatchQuery query = qb.parse(queryString);
        assertQueriesEqual(Reasoner.resolve(query, false), qb.parse(explicitQuery));
        assertQueriesEqual(Reasoner.resolve(query, true), qb.parse(explicitQuery));
    }

    private void assertQueriesEqual(Stream<Map<VarName, Concept>> s1, MatchQuery s2) {
        assertEquals(s1.collect(Collectors.toSet()), s2.admin().streamWithVarNames().collect(Collectors.toSet()));
    }
}
