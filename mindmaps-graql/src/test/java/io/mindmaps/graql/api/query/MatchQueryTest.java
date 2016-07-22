/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.api.query;

import com.google.common.collect.Lists;
import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.implementation.Data;
import io.mindmaps.core.model.Concept;
import io.mindmaps.example.MovieGraphFactory;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static io.mindmaps.core.implementation.DataType.ConceptMeta.*;
import static io.mindmaps.graql.api.query.QueryBuilder.*;
import static io.mindmaps.graql.api.query.ValuePredicate.*;
import static org.junit.Assert.*;

public class MatchQueryTest {

    private static MindmapsTransaction transaction;
    private QueryBuilder qb;

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);

    @BeforeClass
    public static void setUpClass() {
        MindmapsGraph mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
        MovieGraphFactory.loadGraph(mindmapsGraph);
        transaction = mindmapsGraph.newTransaction();
    }

    @Before
    public void setUp() {
        qb = QueryBuilder.build(transaction);
    }

    @Test
    public void testMovieQuery() {
        MatchQuery query = qb.match(var("x").isa("movie"));

        QueryUtil.assertResultsMatch(query, "x", "movie", QueryUtil.movies);
    }

    @Test
    public void testProductionQuery() {
        MatchQuery query = qb.match(var("x").isa("production"));

        QueryUtil.assertResultsMatch(query, "x", "movie", QueryUtil.movies);
    }

    @Test
    public void testValueQuery() {
        MatchQuery query = qb.match(var("the-crime-genre").value("crime"));
        List<Map<String, Concept>> results = Lists.newArrayList(query);

        assertEquals(1, results.size());

        Map<String, Concept> result = results.get(0);
        Concept crime = result.get("the-crime-genre");

        assertEquals("genre", crime.type().getId());
        assertEquals("crime", crime.getValue());
        assertEquals("crime", crime.getId());
    }

    @Test
    public void testRoleOnlyQuery() {
        MatchQuery query = qb.match(var().rel("actor", "x"));

        QueryUtil.assertResultsMatch(
                query, "x", "person",
                "Marlon-Brando", "Al-Pacino", "Miss-Piggy", "Kermit-The-Frog", "Martin-Sheen", "Robert-de-Niro",
                "Jude-Law", "Miranda-Heart", "Bette-Midler", "Sarah-Jessica-Parker"
        );
    }

    @Test
    public void testPredicateQuery1() {
        MatchQuery query = qb.match(
                var("x").isa("movie")
                        .value(any(lt("Juno").and(gt("Godfather")), eq("Apocalypse Now"), eq("Spy")).and(neq("Apocalypse Now")))
        );

        QueryUtil.assertResultsMatch(query, "x", "movie", "Hocus-Pocus", "Heat", "Spy");
    }

    @Test
    public void testPredicateQuery2() {
        MatchQuery query = qb.match(
                var("x").isa("movie").value(all(lte("Juno"), gte("Godfather"), neq("Heat")).or(eq("The Muppets")))
        );

        QueryUtil.assertResultsMatch(query, "x", "movie", "Hocus-Pocus", "Godfather", "The-Muppets");
    }

    @Test
    public void testRegexQuery() {
        MatchQuery query = qb.match(
                var("x").isa("genre").value(regex("^f.*y$"))
        );

        QueryUtil.assertResultsMatch(query, "x", "genre", "family", "fantasy");
    }

    @Test
    public void testContainsQuery() {
        MatchQuery query = qb.match(
                var("x").isa("character").value(contains("ar"))
        );

        QueryUtil.assertResultsMatch(query, "x", "character", "Sarah", "Benjamin-L-Willard", "Harry");
    }

    @Test
    public void testOntologyQuery() {
        MatchQuery query = qb.match(
                var("type").playsRole("character-being-played")
        );

        QueryUtil.assertResultsMatch(query, "type", ENTITY_TYPE.getId(), "character", "person");
    }

    @Test
    public void testRelationshipQuery() {
        MatchQuery query = qb.match(
                var("x").isa("movie"),
                var("y").isa("person"),
                var("z").isa("character").value("Don Vito Corleone"),
                var().rel("x").rel("y").rel("z")
        ).select("x", "y");
        List<Map<String, Concept>> results = Lists.newArrayList(query);

        assertEquals(1, results.size());

        Map<String, Concept> result = results.get(0);
        assertEquals("Godfather", result.get("x").getValue());
        assertEquals("Marlon Brando", result.get("y").getValue());
    }

    @Test
    public void testIdQuery() {
        MatchQuery query = qb.match(or(var("x").id("character"), var("x").id("person")));

        QueryUtil.assertResultsMatch(query, "x", ENTITY_TYPE.getId(), "character", "person");
    }

    @Test
    public void testKnowledgeQuery() {
        MatchQuery query = qb.match(
                var("x").isa("person"),
                var().rel("x").rel("y"),
                var("y").isa("movie"),
                var().rel("y").rel("z"),
                var("z").isa("person").id("Marlon-Brando")
        ).select("x");

        QueryUtil.assertResultsMatch(query, "x", "person", "Marlon-Brando", "Al-Pacino", "Martin-Sheen");
    }

    @Test
    public void testRoleQuery() {
        MatchQuery query = qb.match(
                var().rel("actor", "x").rel("y"),
                var("y").id("Apocalypse-Now")
        ).select("x");

        QueryUtil.assertResultsMatch(query, "x", "person", "Marlon-Brando", "Martin-Sheen");
    }

    @Test
    public void testResourceMatchQuery() throws ParseException {
        MatchQuery query = qb.match(
                var("x").has("release-date", DATE_FORMAT.parse("Mon Mar 03 00:00:00 BST 1986").getTime())
        );

        QueryUtil.assertResultsMatch(query, "x", "movie", "Spy");
    }

    @Test
    public void testNameQuery() {
        MatchQuery query = qb.match(var("x").has("title", "The Godfather"));
        QueryUtil.assertResultsMatch(query, "x", "movie", "Godfather");
    }


    @Test
    public void testIntPredicateQuery() {
        MatchQuery query = qb.match(
                var("x").has("tmdb-vote-count", lte(400))
        );

        QueryUtil.assertResultsMatch(query, "x", "movie", "Apocalypse-Now", "The-Muppets", "Chinese-Coffee");
    }

    @Test
    public void testDoublePredicateQuery() {
        MatchQuery query = qb.match(
                var("x").has("tmdb-vote-average", gt(7.8))
        );

        QueryUtil.assertResultsMatch(query, "x", "movie", "Apocalypse-Now", "Godfather");
    }

    @Test
    public void testDatePredicateQuery() throws ParseException {
        MatchQuery query = qb.match(
                var("x").has("release-date", gte(DATE_FORMAT.parse("Tue Jun 23 12:34:56 GMT 1984").getTime()))
        );

        QueryUtil.assertResultsMatch(query, "x", "movie", "Spy", "The-Muppets", "Chinese-Coffee");
    }

    @Test
    public void testGlobalPredicateQuery() {
        Streamable<Concept> query = qb.match(
                var("x").value(gt(500L).and(lt(1000000L)))
        ).get("x");

        // Results will contain any numbers greater than 500, but no strings
        List<Concept> results = Lists.newArrayList(query);

        assertEquals(1, results.size());
        Concept result = results.get(0);
        assertEquals(1000L, result.getValue());
        assertEquals("tmdb-vote-count", result.type().getId());
    }

    @Test
    public void testAssertionQuery() {
        MatchQuery query = qb.match(
                var("a").rel("production-with-cast", "x").rel("y"),
                var("y").value("Miss Piggy"),
                var("a").isa("has-cast")
        ).select("x");

        QueryUtil.assertResultsMatch(query, "x", "movie", "The-Muppets");
    }

    @Test
    public void testAndOrPattern() {
        MatchQuery query = qb.match(
                var("x").isa("movie"),
                or(
                        and(var("y").isa("genre").value("drama"), var().rel("x").rel("y")),
                        var("x").value("The Muppets")
                )
        ).select("x");

        QueryUtil.assertResultsMatch(query, "x", "movie", "Godfather", "Apocalypse-Now", "Heat", "The-Muppets", "Chinese-Coffee");
    }

    @Test
    public void testTypeAsVariable() {
        MatchQuery query = qb.match(id("genre").playsRole(var("x")));
        QueryUtil.assertResultsMatch(query, "x", null, "genre-of-production");
    }

    @Test
    public void testVariableAsRoleType() {
        MatchQuery query = qb.match(var().rel(var().id("genre-of-production"), "y"));
        QueryUtil.assertResultsMatch(
                query, "y", null,
                "crime", "drama", "war", "action", "comedy", "family", "musical", "comedy", "fantasy"
        );
    }

    @Test
    public void testVariableAsRoleplayer() {
        MatchQuery query = qb.match(
                var().rel(var("x").isa("movie")).rel("genre-of-production", var().value("crime"))
        );

        QueryUtil.assertResultsMatch(query, "x", null, "Godfather", "Heat");
    }

    @Test
    public void testVariablesEverywhere() {
        MatchQuery query = qb.match(
                var()
                        .rel(id("production-with-genre"), var("x").isa(var().ako(id("production"))))
                        .rel(var().value("crime"))
        );

        QueryUtil.assertResultsMatch(query, "x", null, "Godfather", "Heat");
    }

    @Test
    public void testAkoSelf() {
        MatchQuery query = qb.match(id("movie").ako(var("x")));

        QueryUtil.assertResultsMatch(query, "x", ENTITY_TYPE.getId(), "movie", "production");
    }

    @Test
    public void testHasValue() {
        MatchQuery query = qb.match(var("x").value()).limit(10);
        assertEquals(10, query.stream().count());
        assertTrue(query.stream().allMatch(results -> results.get("x").getValue() != null));
    }

    @Test
    public void testHasReleaseDate() {
        MatchQuery query = qb.match(var("x").has("release-date"));
        assertEquals(4, query.stream().count());
        assertTrue(query.stream().map(results -> results.get("x")).allMatch(
                x -> x.asEntity().resources().stream().anyMatch(
                        resource -> resource.type().getId().equals("release-date")
                )
        ));
    }

    @Test
    public void testRobertDeNiroNotRelatedToSelf() {
        MatchQuery query = qb.match(
                var().rel("x").rel("y"),
                var("y").id("Robert-de-Niro")
        ).select("x");

        QueryUtil.assertResultsMatch(query, "x", null, "Heat", "Neil-McCauley");
    }

    @Test
    public void testKermitIsRelatedToSelf() {
        MatchQuery query = qb.match(
                var().rel("x").rel("y"),
                var("y").id("Kermit-The-Frog")
        ).select("x");

        QueryUtil.assertResultsMatch(query, "x", null, "The-Muppets", "Kermit-The-Frog");
    }

    @Test
    public void testGettingNullValue() {
        MatchQuery query = qb.match(var("x").isa("has-cast"));
        Concept result = query.iterator().next().get("x");
        assertNull(result.getValue());
    }

    @Test
    public void testMatchDataType() {
        MatchQuery query = qb.match(var("x").datatype(Data.DOUBLE));
        QueryUtil.assertResultsMatch(query, "x", RESOURCE_TYPE.getId(), "tmdb-vote-average");

        query = qb.match(var("x").datatype(Data.LONG));
        QueryUtil.assertResultsMatch(query, "x", RESOURCE_TYPE.getId(), "tmdb-vote-count", "runtime", "release-date");

        query = qb.match(var("x").datatype(Data.BOOLEAN));
        assertEquals(0, query.stream().count());

        query = qb.match(var("x").datatype(Data.STRING));
        QueryUtil.assertResultsMatch(query, "x", RESOURCE_TYPE.getId(), "title", "gender", "real-name");
    }

    @Test
    public void testSelectRuleTypes() {
        MatchQuery query = qb.match(var("x").isa(RULE_TYPE.getId()));
        QueryUtil.assertResultsMatch(query, "x", RULE_TYPE.getId(), "a-rule-type", "inference-rule", "constraint-rule");
    }

    @Test
    public void testMatchRuleRightHandSide() {
        MatchQuery query = qb.match(var("x").lhs("expect-lhs").rhs("expect-rhs"));
        QueryUtil.assertResultsMatch(query, "x", "a-rule-type", "expectation-rule");
        assertTrue(query.iterator().next().get("x").asRule().getExpectation());
    }

    @Test
    public void testDisconnectedQuery() {
        MatchQuery query = qb.match(var("x").isa("movie"), var("y").isa("person"));
        int numPeople = 10;
        assertEquals(QueryUtil.movies.length * numPeople, query.stream().count());
    }
}