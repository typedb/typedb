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

import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.graphs.MovieGraph;
import ai.grakn.graql.Graql;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.internal.pattern.property.LhsProperty;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.test.GraphContext;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.graql.Graql.contains;
import static ai.grakn.graql.Graql.eq;
import static ai.grakn.graql.Graql.gt;
import static ai.grakn.graql.Graql.gte;
import static ai.grakn.graql.Graql.lt;
import static ai.grakn.graql.Graql.lte;
import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.neq;
import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.regex;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.test.matcher.GraknMatchers.allVariables;
import static ai.grakn.test.matcher.GraknMatchers.concept;
import static ai.grakn.test.matcher.GraknMatchers.constraintRule;
import static ai.grakn.test.matcher.GraknMatchers.entity;
import static ai.grakn.test.matcher.GraknMatchers.hasType;
import static ai.grakn.test.matcher.GraknMatchers.hasValue;
import static ai.grakn.test.matcher.GraknMatchers.inferenceRule;
import static ai.grakn.test.matcher.GraknMatchers.isCasting;
import static ai.grakn.test.matcher.GraknMatchers.isInstance;
import static ai.grakn.test.matcher.GraknMatchers.resource;
import static ai.grakn.test.matcher.GraknMatchers.results;
import static ai.grakn.test.matcher.GraknMatchers.rule;
import static ai.grakn.test.matcher.GraknMatchers.variable;
import static ai.grakn.test.matcher.MovieMatchers.aRuleType;
import static ai.grakn.test.matcher.MovieMatchers.action;
import static ai.grakn.test.matcher.MovieMatchers.alPacino;
import static ai.grakn.test.matcher.MovieMatchers.apocalypseNow;
import static ai.grakn.test.matcher.MovieMatchers.benjaminLWillard;
import static ai.grakn.test.matcher.MovieMatchers.betteMidler;
import static ai.grakn.test.matcher.MovieMatchers.character;
import static ai.grakn.test.matcher.MovieMatchers.chineseCoffee;
import static ai.grakn.test.matcher.MovieMatchers.cluster;
import static ai.grakn.test.matcher.MovieMatchers.comedy;
import static ai.grakn.test.matcher.MovieMatchers.containsAllMovies;
import static ai.grakn.test.matcher.MovieMatchers.crime;
import static ai.grakn.test.matcher.MovieMatchers.drama;
import static ai.grakn.test.matcher.MovieMatchers.family;
import static ai.grakn.test.matcher.MovieMatchers.fantasy;
import static ai.grakn.test.matcher.MovieMatchers.gender;
import static ai.grakn.test.matcher.MovieMatchers.genre;
import static ai.grakn.test.matcher.MovieMatchers.genreOfProduction;
import static ai.grakn.test.matcher.MovieMatchers.godfather;
import static ai.grakn.test.matcher.MovieMatchers.harry;
import static ai.grakn.test.matcher.MovieMatchers.hasTitle;
import static ai.grakn.test.matcher.MovieMatchers.heat;
import static ai.grakn.test.matcher.MovieMatchers.hocusPocus;
import static ai.grakn.test.matcher.MovieMatchers.judeLaw;
import static ai.grakn.test.matcher.MovieMatchers.kermitTheFrog;
import static ai.grakn.test.matcher.MovieMatchers.language;
import static ai.grakn.test.matcher.MovieMatchers.marlonBrando;
import static ai.grakn.test.matcher.MovieMatchers.martinSheen;
import static ai.grakn.test.matcher.MovieMatchers.mirandaHeart;
import static ai.grakn.test.matcher.MovieMatchers.missPiggy;
import static ai.grakn.test.matcher.MovieMatchers.movie;
import static ai.grakn.test.matcher.MovieMatchers.movies;
import static ai.grakn.test.matcher.MovieMatchers.musical;
import static ai.grakn.test.matcher.MovieMatchers.name;
import static ai.grakn.test.matcher.MovieMatchers.neilMcCauley;
import static ai.grakn.test.matcher.MovieMatchers.person;
import static ai.grakn.test.matcher.MovieMatchers.production;
import static ai.grakn.test.matcher.MovieMatchers.realName;
import static ai.grakn.test.matcher.MovieMatchers.releaseDate;
import static ai.grakn.test.matcher.MovieMatchers.robertDeNiro;
import static ai.grakn.test.matcher.MovieMatchers.runtime;
import static ai.grakn.test.matcher.MovieMatchers.sarah;
import static ai.grakn.test.matcher.MovieMatchers.sarahJessicaParker;
import static ai.grakn.test.matcher.MovieMatchers.spy;
import static ai.grakn.test.matcher.MovieMatchers.theMuppets;
import static ai.grakn.test.matcher.MovieMatchers.title;
import static ai.grakn.test.matcher.MovieMatchers.tmdbVoteAverage;
import static ai.grakn.test.matcher.MovieMatchers.tmdbVoteCount;
import static ai.grakn.test.matcher.MovieMatchers.war;
import static ai.grakn.util.ErrorMessage.MATCH_INVALID;
import static ai.grakn.util.Schema.ImplicitType.HAS;
import static ai.grakn.util.Schema.MetaSchema.RULE;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

@SuppressWarnings({"OptionalGetWithoutIsPresent", "unchecked"})
public class MatchQueryTest {

    private QueryBuilder qb;

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);

    @ClassRule
    public static final GraphContext movieGraph = GraphContext.preLoad(MovieGraph.get());

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() {
        qb = movieGraph.graph().graql();
    }

    @After
    public void tearDown() {
        if (movieGraph.graph() != null) movieGraph.graph().showImplicitConcepts(false);
    }

    @Test
    public void testMovieQuery() {
        MatchQuery query = qb.match(var("x").isa("movie"));
        assertThat(query, variable("x", containsAllMovies));
    }

    @Test
    public void testProductionQuery() {
        MatchQuery query = qb.match(var("x").isa("production"));
        assertThat(query, variable("x", containsAllMovies));
    }

    @Test
    public void testValueQuery() {
        MatchQuery query = qb.match(var("tgf").val("Godfather"));
        assertThat(query, variable("tgf", contains(both(hasValue("Godfather")).and(hasType(title)))));
    }

    @Test
    public void testRoleOnlyQuery() {
        MatchQuery query = qb.match(var().rel("actor", "x")).distinct();

        assertThat(query, variable("x", containsInAnyOrder(
                marlonBrando, alPacino, missPiggy, kermitTheFrog, martinSheen, robertDeNiro, judeLaw, mirandaHeart,
                betteMidler, sarahJessicaParker
        )));
    }

    @Test
    public void testPredicateQuery1() {
        MatchQuery query = qb.match(
                var("x").isa("movie").has("title", var("t")),
                or(
                        var("t").val(eq("Apocalypse Now")),
                        and(var("t").val(lt("Juno")), var("t").val(gt("Godfather"))),
                        var("t").val(eq("Spy"))
                ),
                var("t").val(neq("Apocalypse Now"))
        );

        assertThat(query, variable("x", containsInAnyOrder(hocusPocus, heat, spy)));
    }

    @Test
    public void testPredicateQuery2() {
        MatchQuery query = qb.match(
                var("x").isa("movie").has("title", var("t")),
                or(
                        and(var("t").val(lte("Juno")), var("t").val(gte("Godfather")), var("t").val(neq("Heat"))),
                        var("t").val("The Muppets")
                )
        );

        assertThat(query, variable("x", containsInAnyOrder(hocusPocus, godfather, theMuppets)));
    }

    @Test
    public void testValueEqualsVarQuery() {
        MatchQuery query = qb.match(var("x").val(var("y")));

        assertThat(query.execute(), hasSize(greaterThan(10)));

        query.forEach(result -> {
            Concept x = result.get("x");
            Concept y = result.get("y");
            assertEquals(x.asResource().getValue(), y.asResource().getValue());
        });
    }

    @Test
    public void testRegexQuery() {
        MatchQuery query = qb.match(
                var("x").isa("genre").has("name", regex("^f.*y$"))
        );

        assertThat(query, variable("x", containsInAnyOrder(family, fantasy)));
    }

    @Test
    public void testContainsQuery() {
        MatchQuery query = qb.match(
                var("x").isa("character").has("name", contains("ar"))
        );

        assertThat(query, variable("x", containsInAnyOrder(sarah, benjaminLWillard, harry)));
    }

    @Test
    public void testOntologyQuery() {
        MatchQuery query = qb.match(
                var("type").plays("character-being-played")
        );

        assertThat(query, variable("type", containsInAnyOrder(character, person)));
    }

    @Test
    public void testRelationshipQuery() {
        MatchQuery query = qb.match(
                var("x").isa("movie"),
                var("y").isa("person"),
                var("z").isa("character").has("name", "Don Vito Corleone"),
                var().rel("x").rel("y").rel("z")
        ).select("x", "y");

        assertThat(query, allOf(variable("x", contains(godfather)), variable("y", contains(marlonBrando))));
    }

    @Test
    public void testTypeLabelQuery() {
        MatchQuery query = qb.match(or(var("x").label("character"), var("x").label("person")));

        assertThat(query, variable("x", containsInAnyOrder(character, person)));
    }

    @Test
    public void testKnowledgeQuery() {
        MatchQuery query = qb.match(
                var("x").isa("person"),
                var().rel("x").rel("y"),
                var("y").isa("movie"),
                var().rel("y").rel("z"),
                var("z").isa("person").has("name", "Marlon Brando")
        ).select("x").distinct();

        assertThat(query, variable("x", containsInAnyOrder(marlonBrando, alPacino, martinSheen)));
    }

    @Test
    public void testRoleQuery() {
        MatchQuery query = qb.match(
                var().rel("actor", "x").rel("y"),
                var("y").has("title", "Apocalypse Now")
        ).select("x");

        assertThat(query, variable("x", containsInAnyOrder(marlonBrando, martinSheen)));
    }

    @Test
    public void testResourceMatchQuery() throws ParseException {
        MatchQuery query = qb.match(
                var("x").has("release-date", DATE_FORMAT.parse("Mon Mar 03 00:00:00 BST 1986").getTime())
        );

        assertThat(query, variable("x", contains(spy)));
    }

    @Test
    public void testNameQuery() {
        MatchQuery query = qb.match(var("x").has("title", "Godfather"));

        assertThat(query, variable("x", contains(godfather)));
    }


    @Test
    public void testIntPredicateQuery() {
        MatchQuery query = qb.match(
                var("x").has("tmdb-vote-count", lte(400))
        );

        assertThat(query, variable("x", containsInAnyOrder(apocalypseNow, theMuppets, chineseCoffee)));
    }

    @Test
    public void testDoublePredicateQuery() {
        MatchQuery query = qb.match(
                var("x").has("tmdb-vote-average", gt(7.8))
        );

        assertThat(query, variable("x", containsInAnyOrder(apocalypseNow, godfather)));
    }

    @Test
    public void testDatePredicateQuery() throws ParseException {
        MatchQuery query = qb.match(
                var("x").has("release-date", gte(DATE_FORMAT.parse("Tue Jun 23 12:34:56 GMT 1984").getTime()))
        );

        assertThat(query, variable("x", containsInAnyOrder(spy, theMuppets, chineseCoffee)));
    }

    @Test
    public void testGlobalPredicateQuery() {
        MatchQuery query = qb.match(
                var("x").val(gt(500L)),
                var("x").val(lt(1000000L))
        );

        assertThat(query, variable("x", contains(both(hasValue(1000L)).and(hasType(tmdbVoteCount)))));
    }

    @Test
    public void testAssertionQuery() {
        MatchQuery query = qb.match(
                var("a").rel("production-with-cast", "x").rel("y"),
                var("y").has("name", "Miss Piggy"),
                var("a").isa("has-cast")
        ).select("x");

        // There are two results because Miss Piggy plays both actor and character roles in 'The Muppets' cast
        assertThat(query, variable("x", contains(theMuppets, theMuppets)));
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

        assertThat(query, variable("x", containsInAnyOrder(godfather, apocalypseNow, heat, theMuppets, chineseCoffee)));
    }

    @Test
    public void testTypeAsVariable() {
        MatchQuery query = qb.match(label("genre").plays(var("x")));
        assertThat(query, variable("x", contains(genreOfProduction)));
    }

    @Test
    public void testVariableAsRoleType() {
        MatchQuery query = qb.match(var().rel(var().label("genre-of-production"), "y")).distinct();

        assertThat(query, variable("y", containsInAnyOrder(
                crime, drama, war, action, comedy, family, musical, fantasy
        )));
    }

    @Test
    public void testVariableAsRoleplayer() {
        MatchQuery query = qb.match(
                var().rel(var("x").isa("movie")).rel("genre-of-production", var().has("name", "crime"))
        );

        assertThat(query, variable("x", containsInAnyOrder(godfather, heat)));
    }

    @Test
    public void testVariablesEverywhere() {
        MatchQuery query = qb.match(
                var()
                        .rel(label("production-with-genre"), var("x").isa(var("y").sub(label("production"))))
                        .rel(var().has("name", "crime"))
        );

        assertThat(query, results(containsInAnyOrder(
                allOf(hasEntry(is("x"), godfather), hasEntry(is("y"), production)),
                allOf(hasEntry(is("x"), godfather), hasEntry(is("y"), movie)),
                allOf(hasEntry(is("x"), heat),      hasEntry(is("y"), production)),
                allOf(hasEntry(is("x"), heat),      hasEntry(is("y"), movie))
        )));
    }

    @Test
    public void testSubSelf() {
        MatchQuery query = qb.match(label("movie").sub(var("x")));

        assertThat(query, variable("x", containsInAnyOrder(movie, production, entity, concept)));
    }

    @Test
    public void testIsResource() {
        MatchQuery query = qb.match(var("x").isa("resource")).limit(10);

        assertThat(query.execute(), hasSize(10));
        assertThat(query, variable("x", everyItem(hasType(resource))));
    }

    @Test
    public void testHasReleaseDate() {
        MatchQuery query = qb.match(var("x").has("release-date", var("y")));
        assertThat(query, variable("x", containsInAnyOrder(godfather, theMuppets, spy, chineseCoffee)));
    }

    @Test
    public void testAllowedToReferToNonExistentRoleplayer() {
        MatchQuery query = qb.match(var().rel("actor", var().id(ConceptId.of("999999999999999999"))));
        assertThat(query.execute(), empty());
    }

    @Test
    public void testRobertDeNiroNotRelatedToSelf() {
        MatchQuery query = qb.match(
                var().rel("x").rel("y").isa("has-cast"),
                var("y").has("name", "Robert de Niro")
        ).select("x");

        assertThat(query, variable("x", containsInAnyOrder(heat, neilMcCauley)));
    }

    @Test
    public void testKermitIsRelatedToSelf() {
        MatchQuery query = qb.match(
                var().rel("x").rel("y").isa("has-cast"),
                var("y").has("name", "Kermit The Frog")
        ).select("x");

        assertThat(query, variable("x", (Matcher) hasItem(kermitTheFrog)));
    }

    @Test
    public void testMatchDataType() {
        MatchQuery query = qb.match(var("x").datatype(ResourceType.DataType.DOUBLE));
        assertThat(query, variable("x", containsInAnyOrder(tmdbVoteAverage)));

        query = qb.match(var("x").datatype(ResourceType.DataType.LONG));
        assertThat(query, variable("x", containsInAnyOrder(tmdbVoteCount, runtime, releaseDate)));

        query = qb.match(var("x").datatype(ResourceType.DataType.BOOLEAN));
        assertThat(query, variable("x", empty()));

        query = qb.match(var("x").datatype(ResourceType.DataType.STRING));
        assertThat(query, variable("x", containsInAnyOrder(title, gender, realName, name)));
    }

    @Test
    public void testSelectRuleTypes() {
        MatchQuery query = qb.match(var("x").sub(RULE.getLabel().getValue()));
        assertThat(query, variable("x", containsInAnyOrder(
                rule, aRuleType, inferenceRule, constraintRule
        )));
    }

    @Test
    public void testMatchRuleRightHandSide() {
        MatchQuery query = qb.match(var("x").lhs(qb.parsePattern("$x id 'expect-lhs'")).rhs(qb.parsePattern("$x id 'expect-rhs'")));

        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(MATCH_INVALID.getMessage(LhsProperty.class.getName()));

        query.forEach(r -> {});
    }

    @Test
    public void testDisconnectedQuery() {
        MatchQuery query = qb.match(var("x").isa("movie"), var("y").isa("person"));
        int numPeople = 10;
        assertThat(query.execute(), hasSize(movies.size() * numPeople));
    }

    @Test
    public void testSubRelationType() {
        qb.insert(
                label("ownership").sub("relation").relates("owner").relates("possession"),
                label("organization-with-shares").sub("possession"),
                label("possession").sub("role"),

                label("share-ownership").sub("ownership").relates("shareholder").relates("organization-with-shares"),
                label("shareholder").sub("owner"),
                label("owner").sub("role"),

                label("person").sub("entity").plays("shareholder"),
                label("company").sub("entity").plays("organization-with-shares"),

                var("apple").isa("company"),
                var("bob").isa("person"),

                var().rel("organization-with-shares", "apple").rel("shareholder", "bob").isa("share-ownership")
        ).execute();

        // This method should work despite subs
        //noinspection ResultOfMethodCallIgnored
        qb.match(var().rel("x").rel("shareholder", "y").isa("ownership")).stream().count();

        movieGraph.rollback();
    }

    @Test
    public void testHasVariable() {
        MatchQuery query = qb.match(var().has("title", "Godfather").has("tmdb-vote-count", var("x")));
        assertThat(query, variable("x", contains(hasValue(1000L))));
    }

    @Test
    public void testRegexResourceType() {
        MatchQuery query = qb.match(var("x").regex("(fe)?male"));
        assertThat(query, variable("x", contains(gender)));
    }

    @Test
    public void testGraqlPlaysSemanticsMatchGraphAPI() {
        TypeLabel a = TypeLabel.of("a");
        TypeLabel b = TypeLabel.of("b");
        TypeLabel c = TypeLabel.of("c");
        TypeLabel d = TypeLabel.of("d");
        TypeLabel e = TypeLabel.of("e");
        TypeLabel f = TypeLabel.of("f");

        qb.insert(
                Graql.label(c).sub(Graql.label(b).sub(Graql.label(a).sub("entity"))),
                Graql.label(f).sub(Graql.label(e).sub(Graql.label(d).sub("role"))),
                Graql.label(b).plays(Graql.label(e))
        ).execute();

        GraknGraph graph = movieGraph.graph();

        Stream.of(a, b, c, d, e, f).forEach(type -> {
            Set<Concept> graqlPlays = qb.match(Graql.label(type).plays(var("x"))).get("x").collect(Collectors.toSet());
            Collection<RoleType> graphAPIPlays = new HashSet<>(graph.getType(type).plays());

            assertEquals(graqlPlays, graphAPIPlays);
        });

        Stream.of(d, e, f).forEach(type -> {
            Set<Concept> graqlPlayedBy = qb.match(var("x").plays(Graql.label(type))).get("x").collect(toSet());
            Collection<Type> graphAPIPlayedBy = new HashSet<>(graph.<RoleType>getType(type).playedByTypes());

            assertEquals(graqlPlayedBy, graphAPIPlayedBy);
        });

        movieGraph.rollback();
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
            assertThat(x, not(allOf(is(y), is(z))));
        });
    }

    @Test
    public void testRelatedToSelf() {
        MatchQuery query = qb.match(var().rel("x").rel("x").rel("x"));
        assertThat(query.execute(), empty());
    }

    @Test
    public void testMatchAll() {
        MatchQuery query = qb.match(var("x"));

        // Make sure there a reasonable number of results
        assertThat(query.execute(), hasSize(greaterThan(10)));

        // Make sure results never contain castings
        assertThat(query, variable("x", everyItem(not(isCasting()))));
    }

    @Test
    public void testMatchAllInstances() {
        MatchQuery query = qb.match(var("x").isa(Schema.MetaSchema.CONCEPT.getLabel().getValue()));

        // Make sure there a reasonable number of results
        assertThat(query.execute(), hasSize(greaterThan(10)));

        assertThat(query, variable("x", everyItem(both(isInstance()).and(not(isCasting())))));
    }

    @Test
    public void testMatchAllPairs() {
        int numConcepts = (int) qb.match(var("x")).stream().count();
        MatchQuery pairs = qb.match(var("x"), var("y"));

        // We expect there to be a result for every pair of concepts
        assertThat(pairs.execute(), hasSize(numConcepts * numConcepts));
    }

    @Test
    public void testMatchAllDistinctPairs() {
        int numConcepts = (int) qb.match(var("x")).stream().count();
        MatchQuery pairs = qb.match(var("x").neq("y"));

        // Make sure there are no castings in results
        assertThat(pairs, allVariables(everyItem(not(isCasting()))));

        // We expect there to be a result for every distinct pair of concepts
        assertThat(pairs.execute(), hasSize(numConcepts * (numConcepts - 1)));
    }

    @Test
    public void testAllGreaterThanResources() {
        MatchQuery query = qb.match(var("x").val(gt(var("y"))));

        List<Map<String, Concept>> results = query.execute();

        assertThat(results, hasSize(greaterThan(10)));

        results.forEach(result -> {
            Comparable x = (Comparable) result.get("x").asResource().getValue();
            Comparable y = (Comparable) result.get("y").asResource().getValue();
            assertThat(x, greaterThan(y));
        });
    }

    @Test
    public void testMoviesReleasedBeforeTheMuppets() {
        MatchQuery query = qb.match(
                var("x").has("release-date", lt(var("r"))),
                var().has("title", "The Muppets").has("release-date", var("r"))
        );

        assertThat(query, variable("x", contains(godfather)));
    }

    @Test
    public void testAllLessThanAttachedResource() {
        MatchQuery query = qb.match(
                var("p").has("release-date", var("x")),
                var("x").val(lte(var("y")))
        );

        List<Map<String, Concept>> results = query.execute();

        assertThat(results, hasSize(greaterThan(5)));

        results.forEach(result -> {
            Comparable x = (Comparable) result.get("x").asResource().getValue();
            Comparable y = (Comparable) result.get("y").asResource().getValue();
            assertThat(x, lessThanOrEqualTo(y));
        });
    }

    @Test
    public void testMatchAllResourcesUsingResourceName() {
        MatchQuery query = qb.match(var().has("title", "Godfather").has("resource", var("x")));

        Instance godfather = movieGraph.graph().getResourceType("title").getResource("Godfather").owner();
        Set<Resource<?>> expected = Sets.newHashSet(godfather.resources());

        Set<Resource<?>> results = query.get("x").map(Concept::asResource).collect(toSet());

        assertEquals(expected, results);
    }

    @Test
    public void testNoInstancesOfRoleType() {
        MatchQuery query = qb.match(var("x").isa(var("y")), var("y").label("actor"));
        assertThat(query.execute(), empty());
    }

    @Test
    public void testNoInstancesOfRoleTypeUnselectedVariable() {
        MatchQuery query = qb.match(var().isa(var("y")), var("y").label("actor"));
        assertThat(query.execute(), empty());
    }

    @Test
    public void testNoInstancesOfRoleTypeStartingFromCasting() {
        MatchQuery query = qb.match(var("x").isa(var("y")));
        assertThat(query, variable("y", everyItem(not(isCasting()))));
    }

    @Test
    public void testCannotLookUpCastingById() {
        String castingId = movieGraph.graph().admin().getTinkerTraversal()
                .hasLabel(Schema.BaseType.CASTING.name()).id().next().toString();

        MatchQuery query = qb.match(var("x").id(ConceptId.of(castingId)));
        assertThat(query.execute(), empty());
    }

    @Test
    public void testLookupResourcesOnId() {
        Instance godfather = movieGraph.graph().getResourceType("title").getResource("Godfather").owner();
        ConceptId id = godfather.getId();
        MatchQuery query = qb.match(var().id(id).has("title", var("x")));

        assertThat(query, variable("x", contains(hasValue("Godfather"))));
    }

    @Test
    public void testResultsString() {
        MatchQuery query = qb.match(var("x").isa("movie"));
        List<String> resultsString = query.resultsString(Printers.graql()).collect(toList());
        assertThat(resultsString, everyItem(allOf(containsString("$x"), containsString("movie"), containsString(";"))));
    }

    @Test
    public void testQueryDoesNotCrash() {
        qb.parse("match $m isa movie; (actor: $a1, $m); (actor: $a2, $m); select $a1, $a2;").execute();
    }

    @Test
    public void testMatchHas() {
        MatchQuery query = qb.match(var("x").has("name"));
        assertThat(query, variable("x", containsInAnyOrder(
                person, language, genre, aRuleType, cluster, character
        )));
    }

    @Test
    public void whenMatchingHas_ThenTheResultOnlyContainsTheExpectedVariables() {
        MatchQuery query = qb.match(var("x").has("name"));
        for (Map<String, Concept> result : query) {
            assertEquals(result.keySet(), ImmutableSet.of("x"));
        }
    }

    @Test
    public void testMatchKey() {
        MatchQuery query = qb.match(var("x").key("name"));
        assertThat(query, variable("x", contains(genre)));
    }

    @Test
    public void testHideImplicitTypes() {
        MatchQuery query = qb.match(var("x").sub("concept"));
        assertThat(query, variable("x", allOf((Matcher) hasItem(movie), not((Matcher) hasItem(hasTitle)))));
    }

    @Test
    public void testDontHideImplicitTypesIfExplicitlyMentioned() {
        MatchQuery query = qb.match(var("x").sub("concept").label(HAS.getLabel("title")));
        assertThat(query, variable("x", (Matcher) hasItem(hasTitle)));
    }

    @Test
    public void testDontHideImplicitTypesIfImplicitTypesOn() {
        movieGraph.graph().showImplicitConcepts(true);
        MatchQuery query = qb.match(var("x").sub("concept"));
        assertThat(query, variable("x", hasItems(movie, hasTitle)));
    }

    @Test
    public void testHideImplicitTypesTwice() {
        MatchQuery query1 = qb.match(var("x").sub("concept"));
        assertThat(query1, variable("x", allOf((Matcher) hasItem(movie), not((Matcher) hasItem(hasTitle)))));
        List<Map<String, Concept>> results1 = query1.execute();

        String keyspace = movieGraph.graph().getKeyspace();
        movieGraph.graph().close();
        GraknGraph graph2 = EngineGraknGraphFactory.getInstance().getGraph(keyspace, GraknTxType.WRITE);

        MatchQuery query2 = graph2.graql().match(var("x").sub("concept"));
        assertEquals(results1, query2.execute());
    }

    @Test
    public void testQueryNoVariables() {
        MatchQuery query = qb.match(var().isa("movie"));
        List<Map<String, Concept>> results = query.execute();
        assertThat(results, allOf(
                (Matcher) everyItem(not(hasKey(anything()))),
                hasSize(movies.size())
        ));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMatchEmpty() {
        qb.match().execute();
    }

    @Test
    public void whenQueryDoesNotSpecifyRole_ResultIsTheSameAsSpecifyingMetaRole() {
        Set<Map<String, Concept>> withoutRole = qb.match(var().rel("x").isa("has-cast")).stream().collect(toSet());
        Set<Map<String, Concept>> withRole = qb.match(var().rel("role", "x").isa("has-cast")).stream().collect(toSet());

        assertEquals(withoutRole, withRole);
    }

    @Test
    public void whenQueryingForSameRoleTwice_ReturnResultsWithMultipleRolePlayers() {
        MatchQuery query = qb.match(
                var().rel("production-with-cluster", "x").rel("production-with-cluster", "y").rel("z"),
                var("z").has("name", "1")
        );

        assertThat(query, results(containsInAnyOrder(
                allOf(hasEntry(is("x"), hocusPocus), hasEntry(is("y"), theMuppets)),
                allOf(hasEntry(is("x"), theMuppets), hasEntry(is("y"), hocusPocus))
        )));
    }

    @Test
    public void whenQueryingForSameRoleTwiceWhenItIsPlayedOnce_ReturnNoResults() {
        MatchQuery query = qb.match(var().rel("actor", "x").rel("actor", "y"));

        assertThat(query.execute(), empty());
    }

    @Test
    public void whenQueryingForSameRoleTwice_DoNotReturnDuplicateRolePlayers() {
        MatchQuery query = qb.match(var().rel("cluster-of-production", "x").rel("cluster-of-production", "y"));

        query.forEach(result -> {
            assertNotEquals(result.get("x"), result.get("y"));
        });
    }
}