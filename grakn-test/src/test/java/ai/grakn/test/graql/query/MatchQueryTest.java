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

package ai.grakn.test.graql.query;

import ai.grakn.concept.Concept;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.internal.pattern.property.LhsProperty;
import ai.grakn.test.AbstractMovieGraphTest;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.graql.Graql.contains;
import static ai.grakn.graql.Graql.eq;
import static ai.grakn.graql.Graql.gt;
import static ai.grakn.graql.Graql.gte;
import static ai.grakn.graql.Graql.lt;
import static ai.grakn.graql.Graql.lte;
import static ai.grakn.graql.Graql.name;
import static ai.grakn.graql.Graql.neq;
import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.regex;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.Schema.MetaSchema.ENTITY_TYPE;
import static ai.grakn.util.Schema.MetaSchema.RESOURCE_TYPE;
import static ai.grakn.util.Schema.MetaSchema.RULE_TYPE;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class MatchQueryTest extends AbstractMovieGraphTest {

    private QueryBuilder qb;

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() {
        qb = graph.graql();
    }

    @Test
    public void testMovieQuery() {
        MatchQuery query = qb.match(var("x").isa("movie"));

        QueryUtil.assertResultsMatch(query, "x", "movie", graph.getResourceType("title"), QueryUtil.movies);
    }

    @Test
    public void testProductionQuery() {
        MatchQuery query = qb.match(var("x").isa("production"));

        QueryUtil.assertResultsMatch(query, "x", "movie", graph.getResourceType("title"), QueryUtil.movies);
    }

    @Test
    public void testValueQuery() {
        MatchQuery query = qb.match(var("tgf").value("Godfather"));
        List<Map<String, Concept>> results = Lists.newArrayList(query);

        assertEquals(1, results.size());

        Map<String, Concept> result = results.get(0);
        Concept tgf = result.get("tgf");

        assertEquals("title", tgf.type().getName());
        assertEquals("Godfather", tgf.asResource().getValue());
    }

    @Test
    public void testRoleOnlyQuery() {
        MatchQuery query = qb.match(var().rel("actor", "x"));

        QueryUtil.assertResultsMatch(
                query, "x", "person", graph.getResourceType("name"),
                "Marlon Brando", "Al Pacino", "Miss Piggy", "Kermit The Frog", "Martin Sheen", "Robert de Niro",
                "Jude Law", "Miranda Heart", "Bette Midler", "Sarah Jessica Parker"
        );
    }

    @Test
    public void testPredicateQuery1() {
        MatchQuery query = qb.match(
                var("x").isa("movie").has("title", var("t")),
                or(
                        var("t").value(eq("Apocalypse Now")),
                        and(var("t").value(lt("Juno")), var("t").value(gt("Godfather"))),
                        var("t").value(eq("Spy"))
                ),
                var("t").value(neq("Apocalypse Now"))
        );

        QueryUtil.assertResultsMatch(query, "x", "movie", graph.getResourceType("title"), "Hocus Pocus", "Heat", "Spy");
    }

    @Test
    public void testPredicateQuery2() {
        MatchQuery query = qb.match(
                var("x").isa("movie").has("title", var("t")),
                or(
                        and(var("t").value(lte("Juno")), var("t").value(gte("Godfather")), var("t").value(neq("Heat"))),
                        var("t").value("The Muppets")
                )
        );

        QueryUtil.assertResultsMatch(query, "x", "movie", graph.getResourceType("title"), "Hocus Pocus", "Godfather", "The Muppets");
    }

    @Test
    public void testRegexQuery() {
        MatchQuery query = qb.match(
                var("x").isa("genre").has("name", regex("^f.*y$"))
        );

        QueryUtil.assertResultsMatch(query, "x", "genre", graph.getResourceType("name"), "family", "fantasy");
    }

    @Test
    public void testContainsQuery() {
        MatchQuery query = qb.match(
                var("x").isa("character").has("name", contains("ar"))
        );

        QueryUtil.assertResultsMatch(query, "x", "character", graph.getResourceType("name"), "Sarah", "Benjamin L. Willard", "Harry");
    }

    @Test
    public void testOntologyQuery() {
        MatchQuery query = qb.match(
                var("type").playsRole("character-being-played")
        );

        QueryUtil.assertResultsMatch(query, "type", ENTITY_TYPE.getId(), graph.getResourceType("title"), "character", "person");
    }

    @Test
    public void testRelationshipQuery() {
        MatchQuery query = qb.match(
                var("x").isa("movie"),
                var("y").isa("person"),
                var("z").isa("character").has("name", "Don Vito Corleone"),
                var().rel("x").rel("y").rel("z")
        ).select("x", "y");
        List<Map<String, Concept>> results = Lists.newArrayList(query);

        assertEquals(1, results.size());

        Map<String, Concept> result = results.get(0);
        assertEquals("Godfather", result.get("x").asEntity().resources(graph.getResourceType("title")).iterator().next().getValue());
        assertEquals("Marlon Brando", result.get("y").asEntity().resources(graph.getResourceType("name")).iterator().next().getValue());
    }

    @Test
    public void testTypeNameQuery() {
        MatchQuery query = qb.match(or(var("x").name("character"), var("x").name("person")));

        QueryUtil.assertResultsMatch(query, "x", ENTITY_TYPE.getId(), graph.getResourceType("title"),  "character", "person");
    }

    @Test
    public void testKnowledgeQuery() {
        MatchQuery query = qb.match(
                var("x").isa("person"),
                var().rel("x").rel("y"),
                var("y").isa("movie"),
                var().rel("y").rel("z"),
                var("z").isa("person").has("name", "Marlon Brando")
        ).select("x");

        QueryUtil.assertResultsMatch(query, "x", "person", graph.getResourceType("name"), "Marlon Brando", "Al Pacino", "Martin Sheen");
    }

    @Test
    public void testRoleQuery() {
        MatchQuery query = qb.match(
                var().rel("actor", "x").rel("y"),
                var("y").has("title", "Apocalypse Now")
        ).select("x");

        QueryUtil.assertResultsMatch(query, "x", "person", graph.getResourceType("name"), "Marlon Brando", "Martin Sheen");
    }

    @Test
    public void testResourceMatchQuery() throws ParseException {
        MatchQuery query = qb.match(
                var("x").has("release-date", DATE_FORMAT.parse("Mon Mar 03 00:00:00 BST 1986").getTime())
        );

        QueryUtil.assertResultsMatch(query, "x", "movie", graph.getResourceType("title"), "Spy");
    }

    @Test
    public void testNameQuery() {
        MatchQuery query = qb.match(var("x").has("title", "Godfather"));
        QueryUtil.assertResultsMatch(query, "x", "movie", graph.getResourceType("title"),  "Godfather");
    }


    @Test
    public void testIntPredicateQuery() {
        MatchQuery query = qb.match(
                var("x").has("tmdb-vote-count", lte(400))
        );

        QueryUtil.assertResultsMatch(query, "x", "movie", graph.getResourceType("title"), "Apocalypse Now", "The Muppets", "Chinese Coffee");
    }

    @Test
    public void testDoublePredicateQuery() {
        MatchQuery query = qb.match(
                var("x").has("tmdb-vote-average", gt(7.8))
        );

        QueryUtil.assertResultsMatch(query, "x", "movie", graph.getResourceType("title"), "Apocalypse Now", "Godfather");
    }

    @Test
    public void testDatePredicateQuery() throws ParseException {
        MatchQuery query = qb.match(
                var("x").has("release-date", gte(DATE_FORMAT.parse("Tue Jun 23 12:34:56 GMT 1984").getTime()))
        );

        QueryUtil.assertResultsMatch(query, "x", "movie", graph.getResourceType("title"), "Spy", "The Muppets", "Chinese Coffee");
    }

    @Test
    public void testGlobalPredicateQuery() {
        Stream<Concept> query = qb.match(
                var("x").value(gt(500L)),
                var("x").value(lt(1000000L))
        ).get("x");

        // Results will contain any numbers greater than 500, but no strings
        List<Concept> results = query.collect(toList());

        assertEquals(1, results.size());
        Concept result = results.get(0);
        assertEquals(1000L, result.asResource().getValue());
        assertEquals("tmdb-vote-count", result.type().getName());
    }

    @Test
    public void testAssertionQuery() {
        MatchQuery query = qb.match(
                var("a").rel("production-with-cast", "x").rel("y"),
                var("y").has("name", "Miss Piggy"),
                var("a").isa("has-cast")
        ).select("x");

        QueryUtil.assertResultsMatch(query, "x", "movie", graph.getResourceType("title"), "The Muppets");
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

        QueryUtil.assertResultsMatch(query, "x", "movie", graph.getResourceType("title"), "Godfather", "Apocalypse Now", "Heat", "The Muppets", "Chinese Coffee");
    }

    @Test
    public void testTypeAsVariable() {
        MatchQuery query = qb.match(name("genre").playsRole(var("x")));
        QueryUtil.assertResultsMatch(query, "x", null, graph.getResourceType("title"), "genre-of-production", "has-name-owner");
    }

    @Test
    public void testVariableAsRoleType() {
        MatchQuery query = qb.match(var().rel(var().name("genre-of-production"), "y"));
        QueryUtil.assertResultsMatch(
                query, "y", null, graph.getResourceType("name"),
                "crime", "drama", "war", "action", "comedy", "family", "musical", "comedy", "fantasy"
        );
    }

    @Test
    public void testVariableAsRoleplayer() {
        MatchQuery query = qb.match(
                var().rel(var("x").isa("movie")).rel("genre-of-production", var().has("name", "crime"))
        );

        QueryUtil.assertResultsMatch(query, "x", null, graph.getResourceType("title"),  "Godfather", "Heat");
    }

    @Test
    public void testVariablesEverywhere() {
        MatchQuery query = qb.match(
                var()
                        .rel(name("production-with-genre"), var("x").isa(var().sub(name("production"))))
                        .rel(var().has("name", "crime"))
        );

        QueryUtil.assertResultsMatch(query, "x", null, graph.getResourceType("title"),  "Godfather", "Heat");
    }

    @Test
    public void testSubSelf() {
        MatchQuery query = qb.match(name("movie").sub(var("x")));

        QueryUtil.assertResultsMatch(query, "x", ENTITY_TYPE.getId(), graph.getResourceType("title"),  "movie", "production");
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
                        resource -> resource.type().getName().equals("release-date")
                )
        ));
    }

    @Test
    public void testAllowedToReferToNonExistentRoleplayer() {
        long count = qb.match(var().rel("actor", name("doesnt-exist"))).stream().count();
        assertEquals(0, count);
    }

    @Test
    public void testRobertDeNiroNotRelatedToSelf() {
        MatchQuery query = qb.match(
                var().rel("x").rel("y").isa("has-cast"),
                var("y").has("name", "Robert de Niro")
        ).select("x");

        List<ResourceType> resourceTypes = Arrays.asList(graph.getResourceType("name"), graph.getResourceType("title"));
        QueryUtil.assertResultsMatch(query, "x", null, resourceTypes, "Heat", "Neil McCauley");
    }

    @Test
    public void testKermitIsRelatedToSelf() {
        MatchQuery query = qb.match(
                var().rel("x").rel("y").isa("has-cast"),
                var("y").has("name", "Kermit The Frog")
        ).select("x");

        List<ResourceType> resourceTypes = Arrays.asList(graph.getResourceType("title"), graph.getResourceType("name"));
        QueryUtil.assertResultsMatch(query, "x", null, resourceTypes,  "The Muppets", "Kermit The Frog");
    }

    @Test
    public void testMatchDataType() {
        MatchQuery query = qb.match(var("x").datatype(ResourceType.DataType.DOUBLE));
        QueryUtil.assertResultsMatch(query, "x", RESOURCE_TYPE.getId(), graph.getResourceType("title"), "tmdb-vote-average");

        query = qb.match(var("x").datatype(ResourceType.DataType.LONG));
        QueryUtil.assertResultsMatch(query, "x", RESOURCE_TYPE.getId(), graph.getResourceType("title"), "tmdb-vote-count", "runtime", "release-date");

        query = qb.match(var("x").datatype(ResourceType.DataType.BOOLEAN));
        assertEquals(0, query.stream().count());

        query = qb.match(var("x").datatype(ResourceType.DataType.STRING));
        QueryUtil.assertResultsMatch(query, "x", RESOURCE_TYPE.getId(), graph.getResourceType("title"), "title", "gender", "real-name", "name");
    }

    @Test
    public void testSelectRuleTypes() {
        MatchQuery query = qb.match(var("x").isa(RULE_TYPE.getId()));
        QueryUtil.assertResultsMatch(query, "x", RULE_TYPE.getId(), graph.getResourceType("title"), "a-rule-type", "inference-rule", "constraint-rule");
    }

    @Test
    public void testMatchRuleRightHandSide() {
        MatchQuery query = qb.match(var("x").lhs(qb.parsePattern("$x id 'expect-lhs'")).rhs(qb.parsePattern("$x id 'expect-rhs'")));

        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.MATCH_INVALID.getMessage(LhsProperty.class.getName()))
        ));

        query.forEach(r -> {});
    }

    @Test
    public void testDisconnectedQuery() {
        MatchQuery query = qb.match(var("x").isa("movie"), var("y").isa("person"));
        int numPeople = 10;
        assertEquals(QueryUtil.movies.length * numPeople, query.stream().count());
    }

    @Test
    public void testSubRelationType() {
        // Work with a fresh graph for this test
        rollbackGraph();

        qb.insert(
                name("ownership").isa("relation-type").hasRole("owner").hasRole("possession"),
                name("organization-with-shares").sub("possession"),
                name("possession").isa("role-type"),

                name("share-ownership").sub("ownership").hasRole("shareholder").hasRole("organization-with-shares"),
                name("shareholder").sub("owner"),
                name("owner").isa("role-type"),

                name("person").isa("entity-type").playsRole("shareholder"),
                name("company").isa("entity-type").playsRole("organization-with-shares"),

                var("apple").isa("company"),
                var("bob").isa("person"),

                var().rel("organization-with-shares", "apple").rel("shareholder", "bob").isa("share-ownership")
        ).execute();

        // This should work despite subs
        qb.match(var().rel("x").rel("shareholder", "y").isa("ownership")).stream().count();
    }

    @Test
    public void testHasVariable() {
        MatchQuery query = qb.match(var().has("title", "Godfather").has("tmdb-vote-count", var("x")));
        assertEquals(1000L, query.get("x").findFirst().get().asResource().getValue());
    }

    @Test
    public void testRegexResourceType() {
        MatchQuery query = qb.match(var("x").regex("(fe)?male"));
        assertEquals(1, query.stream().count());
        assertEquals("gender", query.get("x").findFirst().get().asType().getName());
    }

    @Test
    public void testPlaysRoleSub() {
        qb.insert(
                name("c").sub(name("b").sub(name("a").isa("entity-type"))),
                name("f").sub(name("e").sub(name("d").isa("role-type"))),
                name("b").playsRole("e")
        ).execute();

        // Make sure SUBs are followed correctly...
        assertTrue(qb.match(name("b").playsRole("e")).ask().execute());
        assertTrue(qb.match(name("b").playsRole("f")).ask().execute());
        assertTrue(qb.match(name("c").playsRole("e")).ask().execute());
        assertTrue(qb.match(name("c").playsRole("f")).ask().execute());

        // ...and not incorrectly
        assertFalse(qb.match(name("a").playsRole("d")).ask().execute());
        assertFalse(qb.match(name("a").playsRole("e")).ask().execute());
        assertFalse(qb.match(name("a").playsRole("f")).ask().execute());
        assertFalse(qb.match(name("b").playsRole("d")).ask().execute());
        assertFalse(qb.match(name("c").playsRole("d")).ask().execute());
    }

    @Test
    public void testMatchQueryExecuteAndParallelStream() {
        MatchQuery query = qb.match(var("x").isa("movie"));
        List<Map<String, Concept>> list = query.execute();
        assertEquals(list, query.parallelStream().collect(toList()));
    }

    @Test
    public void testDistinctRoleplayers() {
        MatchQuery query = qb.match(var().rel("x").rel("y").rel("z").isa("has-cast"));

        assertNotEquals(0, query.stream().count());

        // Make sure none of the resulting relationships have 3 role-players all the same
        query.forEach(result -> {
            Concept x = result.get("x");
            Concept y = result.get("y");
            Concept z = result.get("z");
            assertFalse(x + " = " + y + " = " + z, x.equals(y) && x.equals(z));
        });
    }

    @Test
    public void testRelatedToSelf() {
        MatchQuery query = qb.match(var().rel("x").rel("x").rel("x"));
        
        assertEquals(0, query.stream().count());
    }

    @Test
    public void testMatchAll() {
        MatchQuery query = qb.match(var("x"));

        // Make sure there a reasonable number of results
        assertTrue(query.stream().count() > 10);

        query.get("x").forEach(concept -> {
            // Make sure results never contain castings
            if (concept.type() != null) {
                assertFalse(concept.type().isRoleType());
            }
        });
    }

    @Test
    public void testMatchAllPairs() {
        long numConcepts = qb.match(var("x")).stream().count();
        MatchQuery pairs = qb.match(var("x"), var("y"));

        // We expect there to be a result for every pair of concepts
        assertEquals(numConcepts * numConcepts, pairs.stream().count());
    }

    @Test
    public void testMatchAllDistinctPairs() {
        long numConcepts = qb.match(var("x")).stream().count();
        MatchQuery pairs = qb.match(var("x").neq("y"));

        // Make sure there are no castings in results
        assertFalse(pairs.stream()
                .flatMap(result -> result.values().stream())
                .anyMatch(concept -> concept.getId().startsWith("CASTING-"))
        );

        // We expect there to be a result for every distinct pair of concepts
        assertEquals(numConcepts * (numConcepts - 1), pairs.stream().count());
    }

    @Test
    public void testAllGreaterThanResources() {
        MatchQuery query = qb.match(var("x").value(gt(var("y"))));

        List<Map<String, Concept>> results = query.execute();

        assertTrue(String.valueOf(results.size()), results.size() > 10);

        results.forEach(result -> {
            //noinspection unchecked
            Comparable<Comparable<?>> x = (Comparable<Comparable<?>>) result.get("x").asResource().getValue();
            Comparable<?> y = (Comparable<?>) result.get("y").asResource().getValue();
            assertTrue(x.toString() + " <= " + y.toString(), x.compareTo(y) > 0);
        });
    }

    @Test
    public void testMoviesReleasedBeforeTheMuppets() {
        MatchQuery query = qb.match(
                var("x").has("release-date", lt(var("r"))),
                var().has("title", "The Muppets").has("release-date", var("r"))
        );

        QueryUtil.assertResultsMatch(query, "x", "movie", graph.getResourceType("title"), "Godfather");
    }

    @Test
    public void testMatchAllResources() {
        MatchQuery query = qb.match(var().has("title", "Godfather").has(var("x")));

        Instance godfather = graph.getResource("Godfather", graph.getResourceType("title")).owner();
        Set<Resource<?>> expected = Sets.newHashSet(godfather.resources());

        Set<Resource<?>> results = query.get("x").map(Concept::asResource).collect(toSet());

        assertEquals(expected, results);
    }

    @Test
    public void testNoInstancesOfRoleType() {
        MatchQuery query = qb.match(var("x").isa(var("y")), var("y").name("actor"));
        assertEquals(0, query.stream().count());
    }

    @Test
    public void testNoInstancesOfRoleTypeUnselectedVariable() {
        MatchQuery query = qb.match(var().isa(var("y")), var("y").name("actor"));
        assertEquals(0, query.stream().count());
    }

    @Test
    public void testNoInstancesOfRoleTypeStartingFromCasting() {
        MatchQuery query = qb.match(var("x").isa(var("y")));

        query.get("y").forEach(concept -> {
            assertFalse(concept.isRoleType());
        });
    }

    @Test
    public void testCannotLookUpCastingById() {
        String castingId = graph.getTinkerTraversal()
                .hasLabel(Schema.BaseType.CASTING.name()).id().next().toString();

        MatchQuery query = qb.match(var("x").id(castingId));
        assertEquals(0, query.stream().count());
    }

    @Test
    public void testLookupResourcesOnId() {
        Instance godfather = graph.getResource("Godfather", graph.getResourceType("title")).owner();
        String id = godfather.getId();
        MatchQuery query = qb.match(var().id(id).has("title", var("x")));

        assertEquals("Godfather", query.get("x").findAny().get().asResource().getValue());
    }

    @Test
    public void testQueryDoesNotCrash() {
        qb.parse("match $m isa movie; (actor: $a1, $m); (actor: $a2, $m); select $a1, $a2;").execute();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMatchEmpty() {
        qb.match().execute();
    }
}