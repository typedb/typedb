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

package io.mindmaps.graql.query;

import com.google.common.collect.Lists;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.example.MovieGraphFactory;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static io.mindmaps.graql.Graql.all;
import static io.mindmaps.graql.Graql.and;
import static io.mindmaps.graql.Graql.any;
import static io.mindmaps.graql.Graql.contains;
import static io.mindmaps.graql.Graql.eq;
import static io.mindmaps.graql.Graql.gt;
import static io.mindmaps.graql.Graql.gte;
import static io.mindmaps.graql.Graql.id;
import static io.mindmaps.graql.Graql.lt;
import static io.mindmaps.graql.Graql.lte;
import static io.mindmaps.graql.Graql.neq;
import static io.mindmaps.graql.Graql.or;
import static io.mindmaps.graql.Graql.regex;
import static io.mindmaps.graql.Graql.var;
import static io.mindmaps.graql.Graql.withGraph;
import static io.mindmaps.util.Schema.MetaType.ENTITY_TYPE;
import static io.mindmaps.util.Schema.MetaType.RESOURCE_TYPE;
import static io.mindmaps.util.Schema.MetaType.RULE_TYPE;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MatchQueryTest {

    private static MindmapsGraph mindmapsGraph;
    private QueryBuilder qb;

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);

    @BeforeClass
    public static void setUpClass() {
        mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
        MovieGraphFactory.loadGraph(mindmapsGraph);
    }

    @Before
    public void setUp() {
        qb = Graql.withGraph(mindmapsGraph);
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
        MatchQuery query = qb.match(var("tgf").value("Godfather"));
        List<Map<String, Concept>> results = Lists.newArrayList(query);

        assertEquals(1, results.size());

        Map<String, Concept> result = results.get(0);
        Concept tgf = result.get("tgf");

        assertEquals("title", tgf.type().getId());
        assertEquals("Godfather", tgf.asResource().getValue());
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
                        .has("title", any(lt("Juno").and(gt("Godfather")), eq("Apocalypse Now"), eq("Spy")).and(neq("Apocalypse Now")))
        );

        QueryUtil.assertResultsMatch(query, "x", "movie", "Hocus-Pocus", "Heat", "Spy");
    }

    @Test
    public void testPredicateQuery2() {
        MatchQuery query = qb.match(
                var("x").isa("movie").has("title", all(lte("Juno"), gte("Godfather"), neq("Heat")).or(eq("The Muppets")))
        );

        QueryUtil.assertResultsMatch(query, "x", "movie", "Hocus-Pocus", "Godfather", "The-Muppets");
    }

    @Test
    public void testRegexQuery() {
        MatchQuery query = qb.match(
                var("x").isa("genre").has("name", regex("^f.*y$"))
        );

        QueryUtil.assertResultsMatch(query, "x", "genre", "family", "fantasy");
    }

    @Test
    public void testContainsQuery() {
        MatchQuery query = qb.match(
                var("x").isa("character").has("name", contains("ar"))
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
                var("z").isa("character").id("Don-Vito-Corleone"),
                var().rel("x").rel("y").rel("z")
        ).select("x", "y");
        List<Map<String, Concept>> results = Lists.newArrayList(query);

        assertEquals(1, results.size());

        Map<String, Concept> result = results.get(0);
        assertEquals("Godfather", result.get("x").getId());
        assertEquals("Marlon-Brando", result.get("y").getId());
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
        MatchQuery query = qb.match(var("x").has("title", "Godfather"));
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
        Stream<Concept> query = qb.match(
                var("x").value(gt(500L).and(lt(1000000L)))
        ).get("x");

        // Results will contain any numbers greater than 500, but no strings
        List<Concept> results = query.collect(toList());

        assertEquals(1, results.size());
        Concept result = results.get(0);
        assertEquals(1000L, result.asResource().getValue());
        assertEquals("tmdb-vote-count", result.type().getId());
    }

    @Test
    public void testAssertionQuery() {
        MatchQuery query = qb.match(
                var("a").rel("production-with-cast", "x").rel("y"),
                var("y").id("Miss-Piggy"),
                var("a").isa("has-cast")
        ).select("x");

        QueryUtil.assertResultsMatch(query, "x", "movie", "The-Muppets");
    }

    @Test
    public void testAndOrPattern() {
        MatchQuery query = qb.match(
                var("x").isa("movie"),
                or(
                        and(var("y").isa("genre").has("name", "drama"), var().rel("x").rel("y")),
                        var("x").has("title", "The Muppets")
                )
        );

        QueryUtil.assertResultsMatch(query, "x", "movie", "Godfather", "Apocalypse-Now", "Heat", "The-Muppets", "Chinese-Coffee");
    }

    @Test
    public void testTypeAsVariable() {
        MatchQuery query = qb.match(id("genre").playsRole(var("x")));
        QueryUtil.assertResultsMatch(query, "x", null, "genre-of-production", "has-name-owner");
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
                var().rel(var("x").isa("movie")).rel("genre-of-production", var().has("name", "crime"))
        );

        QueryUtil.assertResultsMatch(query, "x", null, "Godfather", "Heat");
    }

    @Test
    public void testVariablesEverywhere() {
        MatchQuery query = qb.match(
                var()
                        .rel(id("production-with-genre"), var("x").isa(var().ako(id("production"))))
                        .rel(var().has("name", "crime"))
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
        assertTrue(query.stream().allMatch(results -> results.get("x").asResource().getValue() != null));
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
    public void testAllowedToReferToNonExistentRoleplayer() {
        long count = qb.match(var().rel("actor", id("doesnt-exist"))).stream().count();
        assertEquals(0, count);
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
    public void testMatchDataType() {
        MatchQuery query = qb.match(var("x").datatype(ResourceType.DataType.DOUBLE));
        QueryUtil.assertResultsMatch(query, "x", RESOURCE_TYPE.getId(), "tmdb-vote-average");

        query = qb.match(var("x").datatype(ResourceType.DataType.LONG));
        QueryUtil.assertResultsMatch(query, "x", RESOURCE_TYPE.getId(), "tmdb-vote-count", "runtime", "release-date");

        query = qb.match(var("x").datatype(ResourceType.DataType.BOOLEAN));
        assertEquals(0, query.stream().count());

        query = qb.match(var("x").datatype(ResourceType.DataType.STRING));
        QueryUtil.assertResultsMatch(query, "x", RESOURCE_TYPE.getId(), "title", "gender", "real-name", "name");
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

    @Test
    public void testAkoRelationType() {
        MindmapsGraph graph = MindmapsTestGraphFactory.newEmptyGraph();
        QueryBuilder qb = withGraph(graph);

        qb.insert(
                id("ownership").isa("relation-type").hasRole("owner").hasRole("possession"),
                id("organization-with-shares").ako("possession"),
                id("possession").isa("role-type"),

                id("share-ownership").ako("ownership").hasRole("shareholder").hasRole("organization-with-shares"),
                id("shareholder").ako("owner"),
                id("owner").isa("role-type"),

                id("person").isa("entity-type").playsRole("shareholder"),
                id("company").isa("entity-type").playsRole("organization-with-shares"),

                id("apple").isa("company"),
                id("bob").isa("person"),

                var().rel("organization-with-shares", id("apple")).rel("shareholder", id("bob")).isa("share-ownership")
        ).execute();

        // This should work despite akos
        qb.match(var().rel("x").rel("shareholder", "y").isa("ownership")).stream().count();
    }
}