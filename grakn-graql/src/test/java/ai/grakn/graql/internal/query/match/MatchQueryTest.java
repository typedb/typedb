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

package ai.grakn.graql.internal.query.match;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.AskQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Order;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.matcher.MatchableConcept;
import ai.grakn.test.GraphContext;
import ai.grakn.test.graphs.MovieGraph;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.graql.Graql.contains;
import static ai.grakn.graql.Graql.eq;
import static ai.grakn.graql.Graql.gt;
import static ai.grakn.graql.Graql.gte;
import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.lt;
import static ai.grakn.graql.Graql.lte;
import static ai.grakn.graql.Graql.neq;
import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.regex;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.matcher.GraknMatchers.concept;
import static ai.grakn.matcher.GraknMatchers.constraintRule;
import static ai.grakn.matcher.GraknMatchers.entity;
import static ai.grakn.matcher.GraknMatchers.hasType;
import static ai.grakn.matcher.GraknMatchers.hasValue;
import static ai.grakn.matcher.GraknMatchers.inferenceRule;
import static ai.grakn.matcher.GraknMatchers.isInstance;
import static ai.grakn.matcher.GraknMatchers.isShard;
import static ai.grakn.matcher.GraknMatchers.resource;
import static ai.grakn.matcher.GraknMatchers.results;
import static ai.grakn.matcher.GraknMatchers.role;
import static ai.grakn.matcher.GraknMatchers.rule;
import static ai.grakn.matcher.GraknMatchers.variable;
import static ai.grakn.matcher.MovieMatchers.aRuleType;
import static ai.grakn.matcher.MovieMatchers.action;
import static ai.grakn.matcher.MovieMatchers.alPacino;
import static ai.grakn.matcher.MovieMatchers.apocalypseNow;
import static ai.grakn.matcher.MovieMatchers.benjaminLWillard;
import static ai.grakn.matcher.MovieMatchers.betteMidler;
import static ai.grakn.matcher.MovieMatchers.character;
import static ai.grakn.matcher.MovieMatchers.chineseCoffee;
import static ai.grakn.matcher.MovieMatchers.cluster;
import static ai.grakn.matcher.MovieMatchers.comedy;
import static ai.grakn.matcher.MovieMatchers.containsAllMovies;
import static ai.grakn.matcher.MovieMatchers.crime;
import static ai.grakn.matcher.MovieMatchers.drama;
import static ai.grakn.matcher.MovieMatchers.family;
import static ai.grakn.matcher.MovieMatchers.fantasy;
import static ai.grakn.matcher.MovieMatchers.gender;
import static ai.grakn.matcher.MovieMatchers.genre;
import static ai.grakn.matcher.MovieMatchers.genreOfProduction;
import static ai.grakn.matcher.MovieMatchers.godfather;
import static ai.grakn.matcher.MovieMatchers.harry;
import static ai.grakn.matcher.MovieMatchers.hasTitle;
import static ai.grakn.matcher.MovieMatchers.heat;
import static ai.grakn.matcher.MovieMatchers.hocusPocus;
import static ai.grakn.matcher.MovieMatchers.judeLaw;
import static ai.grakn.matcher.MovieMatchers.kermitTheFrog;
import static ai.grakn.matcher.MovieMatchers.keyNameOwner;
import static ai.grakn.matcher.MovieMatchers.language;
import static ai.grakn.matcher.MovieMatchers.marlonBrando;
import static ai.grakn.matcher.MovieMatchers.martinSheen;
import static ai.grakn.matcher.MovieMatchers.mirandaHeart;
import static ai.grakn.matcher.MovieMatchers.missPiggy;
import static ai.grakn.matcher.MovieMatchers.movie;
import static ai.grakn.matcher.MovieMatchers.movies;
import static ai.grakn.matcher.MovieMatchers.musical;
import static ai.grakn.matcher.MovieMatchers.name;
import static ai.grakn.matcher.MovieMatchers.neilMcCauley;
import static ai.grakn.matcher.MovieMatchers.person;
import static ai.grakn.matcher.MovieMatchers.production;
import static ai.grakn.matcher.MovieMatchers.realName;
import static ai.grakn.matcher.MovieMatchers.releaseDate;
import static ai.grakn.matcher.MovieMatchers.robertDeNiro;
import static ai.grakn.matcher.MovieMatchers.runtime;
import static ai.grakn.matcher.MovieMatchers.sarah;
import static ai.grakn.matcher.MovieMatchers.sarahJessicaParker;
import static ai.grakn.matcher.MovieMatchers.spy;
import static ai.grakn.matcher.MovieMatchers.theMuppets;
import static ai.grakn.matcher.MovieMatchers.title;
import static ai.grakn.matcher.MovieMatchers.tmdbVoteAverage;
import static ai.grakn.matcher.MovieMatchers.tmdbVoteCount;
import static ai.grakn.matcher.MovieMatchers.war;
import static ai.grakn.util.ErrorMessage.MATCH_INVALID;
import static ai.grakn.util.ErrorMessage.NEGATIVE_OFFSET;
import static ai.grakn.util.ErrorMessage.NON_POSITIVE_LIMIT;
import static ai.grakn.util.ErrorMessage.VARIABLE_NOT_IN_QUERY;
import static ai.grakn.util.Schema.ImplicitType.HAS;
import static ai.grakn.util.Schema.ImplicitType.HAS_OWNER;
import static ai.grakn.util.Schema.ImplicitType.HAS_VALUE;
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
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"OptionalGetWithoutIsPresent", "unchecked"})
public class MatchQueryTest {

    private static final Var x = var("x");
    private static final Var y = var("y");
    public static final Var z = var("z");
    public static final Var t = var("t");
    public static final Var r = var("r");

    private QueryBuilder qb;

    @ClassRule
    public static final GraphContext movieGraph = GraphContext.preLoad(MovieGraph.get());

    // This is a graph to contain unusual edge cases
    @ClassRule
    public static final GraphContext weirdGraph = GraphContext.preLoad(graph -> {
        ResourceType<String> weirdLoopType = graph.putResourceType("name", ResourceType.DataType.STRING);
        weirdLoopType.resource(weirdLoopType);
        Resource<String> weird = weirdLoopType.putResource("weird");
        weird.resource(weird);
    });

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() {
        qb = movieGraph.graph().graql();
    }

    @Test
    public void testMovieQuery() {
        MatchQuery query = qb.match(x.isa("movie"));
        assertThat(query, variable("x", containsAllMovies));
    }

    @Test
    public void testProductionQuery() {
        MatchQuery query = qb.match(x.isa("production"));
        assertThat(query, variable("x", containsAllMovies));
    }

    @Test
    public void testValueQuery() {
        MatchQuery query = qb.match(var("tgf").val("Godfather"));
        assertThat(query, variable("tgf", contains(both(hasValue("Godfather")).and(hasType(title)))));
    }

    @Test
    public void testRoleOnlyQuery() {
        MatchQuery query = qb.match(var().rel("actor", x)).distinct();

        assertThat(query, variable("x", containsInAnyOrder(
                marlonBrando, alPacino, missPiggy, kermitTheFrog, martinSheen, robertDeNiro, judeLaw, mirandaHeart,
                betteMidler, sarahJessicaParker
        )));
    }

    @Test
    public void whenQueryingForRole_ResultContainsAllValidRoles() {
        MatchQuery query = qb.match(var().rel(x, var().has("name", "Michael Corleone"))).distinct();

        assertThat(query, variable("x", containsInAnyOrder(
                role("role"), role("character-being-played"),
                role("has-name-owner")
        )));
    }

    @Test
    public void testPredicateQuery1() {
        MatchQuery query = qb.match(
                x.isa("movie").has("title", t),
                or(
                        t.val(eq("Apocalypse Now")),
                        and(t.val(lt("Juno")), t.val(gt("Godfather"))),
                        t.val(eq("Spy"))
                ),
                t.val(neq("Apocalypse Now"))
        );

        assertThat(query, variable("x", containsInAnyOrder(hocusPocus, heat, spy)));
    }

    @Test
    public void testPredicateQuery2() {
        MatchQuery query = qb.match(
                x.isa("movie").has("title", t),
                or(
                        and(t.val(lte("Juno")), t.val(gte("Godfather")), t.val(neq("Heat"))),
                        t.val("The Muppets")
                )
        );

        assertThat(query, variable("x", containsInAnyOrder(hocusPocus, godfather, theMuppets)));
    }

    @Test
    public void whenQueryingForResourcesWithEqualValues_ResultsAreCorrect() {
        MatchQuery query = qb.match(x.val(y));

        assertThat(query.execute(), hasSize(greaterThan(10)));

        query.forEach(result -> {
            Concept cx = result.get(x);
            Concept cy = result.get(y);
            assertEquals(cx.asResource().getValue(), cy.asResource().getValue());
        });
    }

    @Test
    public void whenQueryingForTitlesWithEqualValues_ResultsAreCorrect() {
        // This is an edge-case which fooled the resource-index optimiser
        MatchQuery query = qb.match(var("x").isa("title").val(var("y")));

        assertThat(query.execute(), hasSize(greaterThan(3)));

        query.forEach(result -> {
            Concept x = result.get("x");
            Concept y = result.get("y");
            assertEquals(x.asResource().getValue(), y.asResource().getValue());
        });
    }

    @Test
    public void testRegexQuery() {
        MatchQuery query = qb.match(
                x.isa("genre").has("name", regex("^f.*y$"))
        );

        assertThat(query, variable("x", containsInAnyOrder(family, fantasy)));
    }

    @Test
    public void testContainsQuery() {
        MatchQuery query = qb.match(
                x.isa("character").has("name", contains("ar"))
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
                x.isa("movie"),
                y.isa("person"),
                z.isa("character").has("name", "Don Vito Corleone"),
                var().rel(x).rel(y).rel(z)
        ).select("x", "y");

        assertThat(query, allOf(variable("x", contains(godfather)), variable("y", contains(marlonBrando))));
    }

    @Test
    public void testTypeLabelQuery() {
        MatchQuery query = qb.match(or(x.label("character"), x.label("person")));

        assertThat(query, variable("x", containsInAnyOrder(character, person)));
    }

    @Test
    public void testKnowledgeQuery() {
        MatchQuery query = qb.match(
                x.isa("person"),
                var().rel(x).rel(y),
                y.isa("movie"),
                var().rel(y).rel(z),
                z.isa("person").has("name", "Marlon Brando")
        ).select("x").distinct();

        assertThat(query, variable("x", containsInAnyOrder(marlonBrando, alPacino, martinSheen)));
    }

    @Test
    public void testRoleQuery() {
        MatchQuery query = qb.match(
                var().rel("actor", x).rel(y),
                y.has("title", "Apocalypse Now")
        ).select("x");

        assertThat(query, variable("x", containsInAnyOrder(marlonBrando, martinSheen)));
    }

    @Test
    public void testResourceMatchQuery() throws ParseException {
        MatchQuery query = qb.match(
                x.has("release-date", LocalDate.of(1986, 3, 3).atStartOfDay())
        );

        assertThat(query, variable("x", contains(spy)));
    }

    @Test
    public void testNameQuery() {
        MatchQuery query = qb.match(x.has("title", "Godfather"));

        assertThat(query, variable("x", contains(godfather)));
    }


    @Test
    public void testIntPredicateQuery() {
        MatchQuery query = qb.match(
                x.has("tmdb-vote-count", lte(400))
        );

        assertThat(query, variable("x", containsInAnyOrder(apocalypseNow, theMuppets, chineseCoffee)));
    }

    @Test
    public void testDoublePredicateQuery() {
        MatchQuery query = qb.match(
                x.has("tmdb-vote-average", gt(7.8))
        );

        assertThat(query, variable("x", containsInAnyOrder(apocalypseNow, godfather)));
    }

    @Test
    public void testDatePredicateQuery() throws ParseException {
        MatchQuery query = qb.match(
                x.has("release-date", gte(LocalDateTime.of(1984, 6, 23, 12, 34, 56)))
        );

        assertThat(query, variable("x", containsInAnyOrder(spy, theMuppets, chineseCoffee)));
    }

    @Test
    public void testGlobalPredicateQuery() {
        MatchQuery query = qb.match(
                x.val(gt(500L)),
                x.val(lt(1000000L))
        );

        assertThat(query, variable("x", contains(both(hasValue(1000L)).and(hasType(tmdbVoteCount)))));
    }

    @Test
    public void testAssertionQuery() {
        MatchQuery query = qb.match(
                var("a").rel("production-with-cast", x).rel(y),
                y.has("name", "Miss Piggy"),
                var("a").isa("has-cast")
        ).select("x");

        // There are two results because Miss Piggy plays both actor and character roles in 'The Muppets' cast
        assertThat(query, variable("x", contains(theMuppets, theMuppets)));
    }

    @Test
    public void testAndOrPattern() {
        MatchQuery query = qb.match(
                x.isa("movie"),
                or(
                        and(y.isa("genre").has("name", "drama"), var().rel(x).rel(y)),
                        x.has("title", "The Muppets")
                )
        );

        assertThat(query, variable("x", containsInAnyOrder(godfather, apocalypseNow, heat, theMuppets, chineseCoffee)));
    }

    @Test
    public void testTypeAsVariable() {
        MatchQuery query = qb.match(label("genre").plays(x));
        assertThat(query, variable("x", containsInAnyOrder(genreOfProduction, keyNameOwner)));
    }

    @Test
    public void testVariableAsRoleType() {
        MatchQuery query = qb.match(var().rel(var().label("genre-of-production"), y)).distinct();

        assertThat(query, variable("y", containsInAnyOrder(
                crime, drama, war, action, comedy, family, musical, fantasy
        )));
    }

    @Test
    public void testVariableAsRoleplayer() {
        MatchQuery query = qb.match(
                var().rel(x.isa("movie")).rel("genre-of-production", var().has("name", "crime"))
        );

        assertThat(query, variable("x", containsInAnyOrder(godfather, heat)));
    }

    @Test
    public void testVariablesEverywhere() {
        MatchQuery query = qb.match(
                var()
                        .rel(label("production-with-genre"), x.isa(y.sub(label("production"))))
                        .rel(var().has("name", "crime"))
        );

        assertThat(query, results(containsInAnyOrder(
                allOf(hasEntry(is(x), godfather), hasEntry(is(y), production)),
                allOf(hasEntry(is(x), godfather), hasEntry(is(y), movie)),
                allOf(hasEntry(is(x), heat), hasEntry(is(y), production)),
                allOf(hasEntry(is(x), heat), hasEntry(is(y), movie))
        )));
    }

    @Test
    public void testSubSelf() {
        MatchQuery query = qb.match(label("movie").sub(x));

        assertThat(query, variable("x", containsInAnyOrder(movie, production, entity, concept)));
    }

    @Test
    public void testIsResource() {
        MatchQuery query = qb.match(x.isa("resource")).limit(10);

        assertThat(query.execute(), hasSize(10));
        assertThat(query, variable("x", everyItem(hasType(resource))));
    }

    @Test
    public void testHasReleaseDate() {
        MatchQuery query = qb.match(x.has("release-date", y));
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
                var().rel(x).rel(y).isa("has-cast"),
                y.has("name", "Robert de Niro")
        ).select("x");

        assertThat(query, variable("x", containsInAnyOrder(heat, neilMcCauley)));
    }

    @Test
    public void testRobertDeNiroNotRelatedToSelfWhenMetaRoleIsSpecified() {
        // This can go wrong because one role-player may use a shortcut edge and the other may not
        MatchQuery query = qb.match(
                var().rel("role", x).rel("actor", y).isa("has-cast"),
                y.has("name", "Robert de Niro")
        ).select("x");

        assertThat(query, variable("x", containsInAnyOrder(heat, neilMcCauley)));
    }

    @Test
    public void testKermitIsRelatedToSelf() {
        MatchQuery query = qb.match(
                var().rel(x).rel(y).isa("has-cast"),
                y.has("name", "Kermit The Frog")
        ).select("x");

        assertThat(query, variable("x", (Matcher) hasItem(kermitTheFrog)));
    }

    @Test
    public void testKermitIsRelatedToSelfWhenMetaRoleIsSpecified() {
        MatchQuery query = qb.match(
                var().rel("role", x).rel(y).isa("has-cast"),
                y.has("name", "Kermit The Frog")
        ).select("x");

        assertThat(query, variable("x", (Matcher) hasItem(kermitTheFrog)));
    }

    @Test
    public void whenQueryingForSuperRolesAndRelations_TheResultsAreTheSame() {
        assertEquals(
                Sets.newHashSet(qb.match(var("x").rel("work", "y").rel("author", "z").isa("authored-by"))),
                Sets.newHashSet(qb.match(var("x").rel("production-being-directed", "y").rel("director", "z").isa("directed-by")))
        );
    }

    @Test
    public void whenQueryingForSuperRolesAndRelationsWithOneRolePlayer_TheResultsAreTheSame() {
        // This is a special case which can cause comparisons between shortcut edges and castings
        assertEquals(
                Sets.newHashSet(qb.match(var("x").rel("y").rel("author", "z").isa("authored-by"))),
                Sets.newHashSet(qb.match(var("x").rel("y").rel("director", "z").isa("directed-by")))
        );
    }

    @Test
    public void whenQueryingForSuperRelationTypes_TheResultsAreTheSame() {
        assertEquals(
                Sets.newHashSet(qb.match(var("x").rel("y").rel("z").isa("authored-by"))),
                Sets.newHashSet(qb.match(var("x").rel("y").rel("z").isa("directed-by")))
        );
    }

    @Test
    public void testMatchDataType() {
        MatchQuery query = qb.match(x.datatype(ResourceType.DataType.DOUBLE));
        assertThat(query, variable("x", contains(tmdbVoteAverage)));

        query = qb.match(x.datatype(ResourceType.DataType.LONG));
        assertThat(query, variable("x", containsInAnyOrder(tmdbVoteCount, runtime)));

        query = qb.match(x.datatype(ResourceType.DataType.BOOLEAN));
        assertThat(query, variable("x", empty()));

        query = qb.match(x.datatype(ResourceType.DataType.STRING));

        assertThat(query, variable("x", containsInAnyOrder(title, gender, realName, name)));
        query = qb.match(x.datatype(ResourceType.DataType.DATE));
        assertThat(query, variable("x", contains(releaseDate)));
    }

    @Test
    public void testSelectRuleTypes() {
        MatchQuery query = qb.match(x.sub(RULE.getLabel().getValue()));
        assertThat(query, variable("x", containsInAnyOrder(
                rule, aRuleType, inferenceRule, constraintRule
        )));
    }

    @Test
    public void testMatchRuleRightHandSide() {
        MatchQuery query = qb.match(x.when(qb.parsePattern("$x id 'expect-when'")).then(qb.parsePattern("$x id 'expect-then'")));

        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(MATCH_INVALID.getMessage("when"));

        query.forEach(r -> {
        });
    }

    @Test
    public void testDisconnectedQuery() {
        MatchQuery query = qb.match(x.isa("movie"), y.isa("person"));
        int numPeople = 10;
        assertThat(query.execute(), hasSize(movies.size() * numPeople));
    }

    @Test
    public void testSubRelationType() {
        // This method should work despite subs
        //noinspection ResultOfMethodCallIgnored
        qb.match(var().rel(x).rel("director", y).isa("authored-by")).stream().count();
    }

    @Test
    public void testHasVariable() {
        MatchQuery query = qb.match(var().has("title", "Godfather").has("tmdb-vote-count", x));
        assertThat(query, variable("x", contains(hasValue(1000L))));
    }

    @Test
    public void testRegexResourceType() {
        MatchQuery query = qb.match(x.regex("(fe)?male"));
        assertThat(query, variable("x", contains(gender)));
    }

    @Test
    public void testGraqlPlaysSemanticsMatchGraphAPI() {
        GraknGraph graph = GraphContext.empty().graph(); // TODO: Try and remove this call if possible
        QueryBuilder qb = graph.graql();

        Label a = Label.of("a");
        Label b = Label.of("b");
        Label c = Label.of("c");
        Label d = Label.of("d");
        Label e = Label.of("e");
        Label f = Label.of("f");

        qb.insert(
                Graql.label(c).sub(Graql.label(b).sub(Graql.label(a).sub("entity"))),
                Graql.label(f).sub(Graql.label(e).sub(Graql.label(d).sub("role"))),
                Graql.label(b).plays(Graql.label(e))
        ).execute();

        Stream.of(a, b, c, d, e, f).forEach(type -> {
            Set<Concept> graqlPlays = qb.match(Graql.label(type).plays(x)).get("x").collect(Collectors.toSet());
            Collection<Role> graphAPIPlays;

            SchemaConcept schemaConcept = graph.getSchemaConcept(type);
            if (schemaConcept.isType()) {
                graphAPIPlays = schemaConcept.asType().plays().collect(toSet());
            } else {
                graphAPIPlays = Collections.EMPTY_SET;
            }

            assertEquals(graqlPlays, graphAPIPlays);
        });

        Stream.of(d, e, f).forEach(type -> {
            Set<Concept> graqlPlayedBy = qb.match(x.plays(Graql.label(type))).get("x").collect(toSet());
            Collection<Type> graphAPIPlayedBy = graph.<Role>getSchemaConcept(type).playedByTypes().collect(toSet());

            assertEquals(graqlPlayedBy, graphAPIPlayedBy);
        });
    }

    @Test
    public void testMatchQueryExecuteAndParallelStream() {
        MatchQuery query = qb.match(x.isa("movie"));
        List<Answer> list = query.execute();
        assertEquals(list, query.parallelStream().collect(toList()));
    }

    @Test
    public void testDistinctRoleplayers() {
        MatchQuery query = qb.match(var().rel(x).rel(y).rel(z).isa("has-cast"));

        assertNotEquals(0, query.stream().count());

        // Make sure none of the resulting relationships have 3 role-players all the same
        query.forEach(result -> {
            Concept cx = result.get(x);
            Concept cy = result.get(y);
            Concept cz = result.get(z);
            assertThat(cx, not(allOf(is(cy), is(cz))));
        });
    }

    @Test
    public void testRelatedToSelf() {
        MatchQuery query = qb.match(var().rel(x).rel(x).rel(x));
        assertThat(query.execute(), empty());
    }

    @Test
    public void testMatchAll() {
        MatchQuery query = qb.match(x);

        // Make sure there a reasonable number of results
        assertThat(query.execute(), hasSize(greaterThan(10)));

        // Make sure results never contain shards
        assertThat(query, variable("x", everyItem(not(isShard()))));
    }

    @Test
    public void testMatchAllInstances() {
        MatchQuery query = qb.match(x.isa(Schema.MetaSchema.THING.getLabel().getValue()));

        // Make sure there a reasonable number of results
        assertThat(query.execute(), hasSize(greaterThan(10)));

        assertThat(query, variable("x", everyItem(isInstance())));
    }

    @Test
    public void testMatchAllPairs() {
        int numConcepts = (int) qb.match(x).stream().count();
        MatchQuery pairs = qb.match(x, y);

        // We expect there to be a result for every pair of concepts
        assertThat(pairs.execute(), hasSize(numConcepts * numConcepts));
    }

    @Test
    public void testMatchAllDistinctPairs() {
        int numConcepts = (int) qb.match(x).stream().count();
        MatchQuery pairs = qb.match(x.neq(y));

        // We expect there to be a result for every distinct pair of concepts
        assertThat(pairs.execute(), hasSize(numConcepts * (numConcepts - 1)));
    }

    @Test
    public void testMatchAllDistinctPairsOfACertainType() {
        int numConcepts = (int) qb.match(x.isa("movie")).stream().count();
        MatchQuery pairs = qb.match(x.isa("movie"), y.isa("movie"), x.neq(y));

        assertThat(pairs.execute(), hasSize(numConcepts * (numConcepts - 1)));
    }

    @Test
    public void testAllGreaterThanResources() {
        MatchQuery query = qb.match(x.val(gt(y)));

        List<Answer> results = query.execute();

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
                x.has("release-date", lt(r)),
                var().has("title", "The Muppets").has("release-date", r)
        );

        assertThat(query, variable("x", contains(godfather)));
    }

    @Test
    public void testMoviesHasHigherTmdbCount() {

        MatchQuery query = qb.match(
                x.has("tmdb-vote-count", lt(r)),
                var().has("title", "The Muppets").has("tmdb-vote-count", r)
        );

        assertThat(query, variable("x", contains(chineseCoffee)));
    }

    @Test
    public void testAllLessThanAttachedResource() {
        MatchQuery query = qb.match(
                var("p").has("release-date", x),
                x.val(lte(y))
        );

        List<Answer> results = query.execute();

        assertThat(results, hasSize(greaterThan(5)));

        results.forEach(result -> {
            Comparable x = (Comparable) result.get("x").asResource().getValue();
            Comparable y = (Comparable) result.get("y").asResource().getValue();
            assertThat(x, lessThanOrEqualTo(y));
        });
    }

    @Test
    public void testMatchAllResourcesUsingResourceName() {
        MatchQuery query = qb.match(var().has("title", "Godfather").has("resource", x));

        Thing godfather = movieGraph.graph().getResourceType("title").getResource("Godfather").owner();
        Set<Resource<?>> expected = godfather.resources().collect(toSet());

        Set<Resource<?>> results = query.get("x").map(Concept::asResource).collect(toSet());

        assertEquals(expected, results);
    }

    @Test
    public void testNoInstancesOfRoleType() {
        MatchQuery query = qb.match(x.isa(y), y.label("actor"));
        assertThat(query.execute(), empty());
    }

    @Test
    public void testNoInstancesOfRoleTypeUnselectedVariable() {
        MatchQuery query = qb.match(var().isa(y), y.label("actor"));
        assertThat(query.execute(), empty());
    }

    @Test
    public void testLookupResourcesOnId() {
        Thing godfather = movieGraph.graph().getResourceType("title").getResource("Godfather").owner();
        ConceptId id = godfather.getId();
        MatchQuery query = qb.match(var().id(id).has("title", x));

        assertThat(query, variable("x", contains(hasValue("Godfather"))));
    }

    @Test
    public void testResultsString() {
        MatchQuery query = qb.match(x.isa("movie"));
        List<String> resultsString = query.resultsString(Printers.graql(false)).collect(toList());
        assertThat(resultsString, everyItem(allOf(containsString("$x"), containsString("movie"), containsString(";"))));
    }

    @Test
    public void testQueryDoesNotCrash() {
        qb.parse("match $m isa movie; (actor: $a1, $m); (actor: $a2, $m); select $a1, $a2;").execute();
    }

    @Test
    public void testMatchHas() {
        MatchQuery query = qb.match(x.has("name"));
        assertThat(query, variable("x", containsInAnyOrder(
                person, language, genre, aRuleType, cluster, character
        )));
    }

    @Test
    public void whenMatchingHas_ThenTheResultOnlyContainsTheExpectedVariables() {
        MatchQuery query = qb.match(x.has("name"));
        for (Answer result : query) {
            assertEquals(result.keySet(), ImmutableSet.of(x));
        }
    }

    @Test
    public void testMatchKey() {
        MatchQuery query = qb.match(x.key("name"));
        assertThat(query, variable("x", contains(genre)));
    }

    @Test
    public void testDontHideImplicitTypesIfExplicitlyMentioned() {
        MatchQuery query = qb.match(x.sub(Schema.MetaSchema.THING.getLabel().getValue()).label(HAS.getLabel("title")));
        assertThat(query, variable("x", (Matcher) hasItem(hasTitle)));
    }

    @Test
    public void whenReferringToImplicitRelationsAndRoles_DontHideResults() {
        VarPattern hasTitle = label(HAS.getLabel("title"));
        VarPattern titleOwner = label(HAS_OWNER.getLabel("title"));
        VarPattern titleValue = label(HAS_VALUE.getLabel("title"));

        MatchQuery hasQuery = qb.match(y.has("title", z));
        MatchQuery explicitQuery = qb.match(var().isa(hasTitle).rel(titleOwner, y).rel(titleValue, z));

        assertEquals(hasQuery.stream().collect(toSet()), explicitQuery.stream().collect(toSet()));
    }

    @Test
    public void testQueryNoVariables() {
        MatchQuery query = qb.match(var().isa("movie"));
        List<Answer> results = query.execute();
        assertThat(results, allOf(
                (Matcher) everyItem(not(hasKey(anything()))),
                hasSize(movies.size())
        ));
    }

    @Test(expected = GraqlQueryException.class)
    public void testMatchEmpty() {
        qb.match().execute();
    }

    @Test
    public void whenQueryDoesNotSpecifyRole_ResultIsTheSameAsSpecifyingMetaRole() {
        Set<Answer> withoutRole = qb.match(var().rel(x).isa("has-cast")).stream().collect(toSet());
        Set<Answer> withRole = qb.match(var().rel("role", x).isa("has-cast")).stream().collect(toSet());

        assertEquals(withoutRole, withRole);
    }

    @Test
    public void whenQueryingForSameRoleTwice_ReturnResultsWithMultipleRolePlayers() {
        MatchQuery query = qb.match(
                var().rel("production-with-cluster", x).rel("production-with-cluster", y).rel(z),
                z.has("name", "1")
        );

        assertThat(query, results(containsInAnyOrder(
                allOf(hasEntry(is(x), hocusPocus), hasEntry(is(y), theMuppets)),
                allOf(hasEntry(is(x), theMuppets), hasEntry(is(y), hocusPocus))
        )));
    }

    @Test
    public void whenQueryingForSameRoleTwiceWhenItIsPlayedOnce_ReturnNoResults() {
        MatchQuery query = qb.match(var().rel("actor", x).rel("actor", y));

        assertThat(query.execute(), empty());
    }

    @Test
    public void whenQueryingForSameRoleTwice_DoNotReturnDuplicateRolePlayers() {
        MatchQuery query = qb.match(var().rel("cluster-of-production", x).rel("cluster-of-production", y));

        query.forEach(result -> {
            assertNotEquals(result.get(x), result.get("y"));
        });
    }

    @Test
    public void whenQueryingForSuperRelationType_ReturnResults() {
        AskQuery query = qb.match(var().isa(Schema.MetaSchema.RELATIONSHIP.getLabel().getValue()).rel(x).rel(y)).ask();
        assertTrue("Query had no results", query.execute());
    }

    @Test
    public void whenQueryingForSuperRoleType_ReturnResults() {
        AskQuery query = qb.match(var().rel("role", x).rel(y)).ask();
        assertTrue("Query had no results", query.execute());
    }

    @Test
    public void whenQueryingForXSubY_ReturnOnlyTypes() {
        MatchQuery query = qb.match(x.sub(y));

        assertThat(query, variable("x", everyItem(not(isInstance()))));
        assertThat(query, variable("y", everyItem(not(isInstance()))));
    }

    @Test
    public void whenQueryingForAResourceWhichHasItselfAsAResource_ReturnTheResource() {
        MatchQuery query = weirdGraph.graph().graql().match(var("x").has("name", var("x")));

        // There are actually two results expected here:
        // This is because the semantics of `$x has foo $y` are "find all connected $x and $y where `$y isa foo`"
        // Therefore, it's valid to arrive back at `$x` by following the binary relation in _either_ direction.
        assertThat(query, variable("x", contains(hasValue("weird"), hasValue("weird"))));
    }

    @Test
    public void whenQueryingForAnImplicitRelationById_TheRelationIsReturned() {
        MatchQuery query = qb.match(var("x").isa(label(Schema.ImplicitType.HAS.getLabel("name"))));

        Relationship relationship = query.get("x").findAny().get().asRelationship();

        MatchQuery queryById = qb.match(var("x").id(relationship.getId()));

        assertThat(queryById, variable("x", contains(MatchableConcept.of(relationship))));
    }

    @Test
    public void whenQueryIsLimitedToANegativeNumber_Throw() {
        expectedException.expect(GraqlQueryException.class);
        expectedException.expectMessage(NON_POSITIVE_LIMIT.getMessage(Long.MIN_VALUE));
        //noinspection ResultOfMethodCallIgnored
        movieGraph.graph().graql().match(var()).limit(Long.MIN_VALUE);
    }

    @Test
    public void whenQueryIsLimitedToZero_Throw() {
        expectedException.expect(GraqlQueryException.class);
        expectedException.expectMessage(NON_POSITIVE_LIMIT.getMessage(0L));
        //noinspection ResultOfMethodCallIgnored
        movieGraph.graph().graql().match(var()).limit(0L);
    }

    @Test
    public void whenQueryIsOffsetByANegativeNumber_Throw() {
        expectedException.expect(GraqlQueryException.class);
        expectedException.expectMessage(NEGATIVE_OFFSET.getMessage(Long.MIN_VALUE));
        //noinspection ResultOfMethodCallIgnored
        movieGraph.graph().graql().match(var()).offset(Long.MIN_VALUE);
    }

    @Test
    public void testDistinctEmpty() {
        Set<Concept> result2 = movieGraph.graph().graql().match(
                var("x").isa("movie").has("title", var("y")),
                var("y").has("name", "xxx")).select("y").distinct().execute()
                .stream()
                .map(stringConceptMap -> stringConceptMap.get("x"))
                .collect(Collectors.toSet());
        assertTrue(result2.isEmpty());
    }

    @Test
    public void testDistinctTuple() {
        int size = movieGraph.graph().graql().match(var("x").isa("genre")).execute().size();
        size *= size;

        List<Answer> result1 = movieGraph.graph().graql().match(
                var("x").isa("genre"),
                var("x").isa("genre"),
                var("x").isa("genre"),
                var("y").isa("genre")).distinct().execute();
        assertEquals(size, result1.size());

        List<Answer> result2 = movieGraph.graph().graql().match(
                var().isa("genre"),
                var().isa("genre"),
                var().isa("genre"),
                var().isa("genre"),
                var().isa("genre")).distinct().execute();
        assertEquals(1, result2.size());

        List<Answer> result3 = movieGraph.graph().graql().match(
                var("x").isa("genre"),
                var("y").isa("genre")).distinct().execute();
        assertEquals(size, result3.size());

        List<Answer> result4 = movieGraph.graph().graql().match(
                var().isa("genre"),
                var("x").isa("genre"),
                var("y").isa("genre")).distinct().execute();
        assertEquals(size, result4.size());
    }

    @Test
    public void whenSelectingVarNotInQuery_Throw() {
        expectedException.expect(GraqlQueryException.class);
        expectedException.expectMessage(VARIABLE_NOT_IN_QUERY.getMessage(Graql.var("x")));
        movieGraph.graph().graql().match(var()).select("x").execute();
    }

    @Test(expected = Exception.class)
    public void testVarNameEmptySet() {
        movieGraph.graph().graql().match(var()).select(Collections.EMPTY_SET).execute();
    }

    @Test(expected = Exception.class)
    public void testVarNameNullSet() {
        movieGraph.graph().graql().match(var()).select((Set<Var>) null).execute();
    }

    @Test(expected = Exception.class)
    public void testVarNameNullString() {
        movieGraph.graph().graql().match(var()).select((String) null).execute();
    }

    @Test(expected = Exception.class)
    public void testOrderBy1() {
        movieGraph.graph().graql().match(var().isa("movie")).orderBy((String) null, Order.desc).execute();
    }

    @Test(expected = Exception.class)
    public void testOrderBy2() {
        movieGraph.graph().graql().match(var().isa("movie")).orderBy((Var) null, Order.desc).execute();
    }

    @Test(expected = Exception.class)
    public void testOrderBy3() {
        movieGraph.graph().graql().match(var("x").isa("movie")).orderBy((String) null, Order.desc).execute();
    }

    @Test(expected = Exception.class)
    public void testOrderBy4() {
        movieGraph.graph().graql().match(var("x").isa("movie")).orderBy((Var) null, Order.desc).execute();
    }

    @Test(expected = Exception.class)
    public void testOrderBy5() {
        movieGraph.graph().graql().match(var("x").isa("movie")).orderBy("y", Order.asc).execute();
    }

    @Test(expected = Exception.class)
    public void testOrderBy6() {
        movieGraph.graph().graql().match(var("x").isa("movie")).orderBy("x", null).execute();
    }

    @Test(expected = Exception.class) //TODO: error message should be more specific
    public void testOrderBy7() {
        movieGraph.graph().graql().match(var("x").isa("movie"),
                var().rel("x").rel("y")).orderBy("y", Order.asc).execute();
    }

    @Test(expected = Exception.class)
    public void testOrderBy8() {
        movieGraph.graph().graql().match(var("x").isa("movie")).orderBy("x", Order.asc).execute();
    }

    @Test
    public void whenExecutingGraqlTraversalFromGraph_ReturnExpectedResults() {
        EntityType type = movieGraph.graph().putEntityType("Concept Type");
        Entity entity = type.addEntity();

        Collection<Concept> results = movieGraph.graph().graql().match(var("x").isa(type.getLabel().getValue())).
                execute().iterator().next().values();

        assertThat(results, containsInAnyOrder(entity));
    }
}