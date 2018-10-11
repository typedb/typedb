
/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.graql.internal.query.match;

import ai.grakn.GraknTx;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Match;
import ai.grakn.graql.Order;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.matcher.MatchableConcept;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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
import static ai.grakn.matcher.GraknMatchers.entity;
import static ai.grakn.matcher.GraknMatchers.hasType;
import static ai.grakn.matcher.GraknMatchers.hasValue;
import static ai.grakn.matcher.GraknMatchers.isInstance;
import static ai.grakn.matcher.GraknMatchers.isShard;
import static ai.grakn.matcher.GraknMatchers.resource;
import static ai.grakn.matcher.GraknMatchers.results;
import static ai.grakn.matcher.GraknMatchers.role;
import static ai.grakn.matcher.GraknMatchers.rule;
import static ai.grakn.matcher.GraknMatchers.variable;
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
import static ai.grakn.matcher.MovieMatchers.expectationRule;
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
import static ai.grakn.matcher.MovieMatchers.materializeRule;
import static ai.grakn.matcher.MovieMatchers.mirandaHeart;
import static ai.grakn.matcher.MovieMatchers.missPiggy;
import static ai.grakn.matcher.MovieMatchers.movie;
import static ai.grakn.matcher.MovieMatchers.movies;
import static ai.grakn.matcher.MovieMatchers.musical;
import static ai.grakn.matcher.MovieMatchers.name;
import static ai.grakn.matcher.MovieMatchers.neilMcCauley;
import static ai.grakn.matcher.MovieMatchers.person;
import static ai.grakn.matcher.MovieMatchers.production;
import static ai.grakn.matcher.MovieMatchers.provenance;
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
import static ai.grakn.util.GraqlTestUtil.assertExists;
import static ai.grakn.util.Schema.ImplicitType.HAS;
import static ai.grakn.util.Schema.ImplicitType.HAS_OWNER;
import static ai.grakn.util.Schema.ImplicitType.HAS_VALUE;
import static ai.grakn.util.Schema.MetaSchema.RULE;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"OptionalGetWithoutIsPresent", "unchecked"})
public class MatchTest {

    private static final Var w = var("w");
    private static final Var x = var("x");
    private static final Var y = var("y");
    public static final Var z = var("z");
    public static final Var t = var("t");
    public static final Var r = var("r");

    private QueryBuilder qb;

    @ClassRule
    public static final SampleKBContext movieKB = MovieKB.context();

    @ClassRule
    public static final SampleKBContext emptyKB = SampleKBContext.empty();

    // This is a graph to contain unusual edge cases
    @ClassRule
    public static final SampleKBContext weirdKB = SampleKBContext.load(graph -> {
        AttributeType<String> weirdLoopType = graph.putAttributeType("name", AttributeType.DataType.STRING);
        weirdLoopType.has(weirdLoopType);
        Attribute<String> weird = weirdLoopType.create("weird");
        weird.has(weird);
    });

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() {
        qb = movieKB.tx().graql();
    }

    @Test
    public void testMovieQuery() {
        Match query = qb.match(x.isa("movie"));
        assertThat(query, variable(x, containsAllMovies));
    }

    @Test
    public void testProductionQuery() {
        Match query = qb.match(x.isa("production"));
        assertThat(query, variable(x, containsAllMovies));
    }

    @Test
    public void testValueQuery() {
        Var tgf = var("tgf");
        Match query = qb.match(tgf.val("Godfather"));
        assertThat(query, variable(tgf, contains(both(hasValue("Godfather")).and(hasType(title)))));
    }

    @Test
    public void testRoleOnlyQuery() {
        Match query = qb.match(var().rel("actor", x));

        assertThat(query, variable(x, containsInAnyOrder(
                marlonBrando, alPacino, missPiggy, kermitTheFrog, martinSheen, robertDeNiro, judeLaw, mirandaHeart,
                betteMidler, sarahJessicaParker
        )));
    }

    @Test
    public void whenQueryingForRole_ResultContainsAllValidRoles() {
        //TODO: reasoner is not able to decompose and parse it correctly
        Match query = qb.infer(false).match(var().rel(x, var().has("name", "Michael Corleone")));

        assertThat(query, variable(x, containsInAnyOrder(
                role("role"),
                role("character-being-played"),
                role("@has-name-owner"),
                role("@has-attribute-owner")
        )));
    }

    @Test
    public void testPredicateQuery1() {
        Match query = qb.match(
                x.isa("movie").has("title", t),
                or(
                        t.val(eq("Apocalypse Now")),
                        and(t.val(lt("Juno")), t.val(gt("Godfather"))),
                        t.val(eq("Spy"))
                ),
                t.val(neq("Apocalypse Now"))
        );

        assertThat(query, variable(x, containsInAnyOrder(hocusPocus, heat, spy)));
    }

    @Test
    public void testPredicateQuery2() {
        Match query = qb.match(
                x.isa("movie").has("title", t),
                or(
                        and(t.val(lte("Juno")), t.val(gte("Godfather")), t.val(neq("Heat"))),
                        t.val("The Muppets")
                )
        );

        assertThat(query, variable(x, containsInAnyOrder(hocusPocus, godfather, theMuppets)));
    }

    @Test
    public void whenExecutingContainsPredicateWithVariable_ReturnExpectedResults() {
        Match query = qb.match(x.val("Sarah Jessica Parker").val(contains(z)), y.has("name", z));
        assertThat(query, variable(y, containsInAnyOrder(sarah, sarahJessicaParker)));
    }

    @Test
    public void whenQueryingForResourcesWithEqualValues_ResultsAreCorrect() {
        Match query = qb.match(x.val(y));

        assertThat(query, iterableWithSize(greaterThan(10)));

        query.forEach(result -> {
            Concept cx = result.get(x);
            Concept cy = result.get(y);
            assertEquals(cx.asAttribute().value(), cy.asAttribute().value());
        });
    }

    @Test
    public void whenQueryingForTitlesWithEqualValues_ResultsAreCorrect() {
        // This is an edge-case which fooled the resource-index optimiser
        Match query = qb.match(x.isa("title").val(y));

        assertThat(query, iterableWithSize(greaterThan(3)));

        query.forEach(result -> {
            Concept cx = result.get(x);
            Concept cy = result.get(y);
            assertEquals(cx.asAttribute().value(), cy.asAttribute().value());
        });
    }

    @Test
    public void testRegexQuery() {
        Match query = qb.match(
                x.isa("genre").has("name", regex("^f.*y$"))
        );

        assertThat(query, variable(x, containsInAnyOrder(family, fantasy)));
    }

    @Test
    public void testContainsQuery() {
        Match query = qb.match(
                x.isa("character").has("name", contains("ar"))
        );

        assertThat(query, variable(x, containsInAnyOrder(sarah, benjaminLWillard, harry)));
    }

    @Test
    public void testSchemaQuery() {
        Var type = var("type");
        Match query = qb.match(
                type.plays("character-being-played")
        );

        assertThat(query, variable(type, containsInAnyOrder(character, person)));
    }

    @Test
    public void testRelationshipQuery() {
        Match query = qb.match(
                x.isa("movie"),
                y.isa("person"),
                z.isa("character").has("name", "Don Vito Corleone"),
                var().rel(x).rel(y).rel(z)
        );

        assertThat(query, allOf(variable(x, contains(godfather)), variable(y, contains(marlonBrando))));
    }

    @Test
    public void testTypeLabelQuery() {
        Match query = qb.match(or(x.label("character"), x.label("person")));

        assertThat(query, variable(x, containsInAnyOrder(character, person)));
    }

    @Test
    public void testRoleQuery() {
        Match query = qb.match(
                var().rel("actor", x).rel(y),
                y.has("title", "Apocalypse Now")
        );

        assertThat(query, variable(x, containsInAnyOrder(marlonBrando, martinSheen)));
    }

    @Test
    public void testResourceMatch() throws ParseException {
        Match query = qb.match(
                x.has("release-date", LocalDate.of(1986, 3, 3).atStartOfDay())
        );

        assertThat(query, variable(x, contains(spy)));
    }

    @Test
    public void testNameQuery() {
        Match query = qb.match(x.has("title", "Godfather"));

        assertThat(query, variable(x, contains(godfather)));
    }


    @Test
    public void testIntPredicateQuery() {
        Match query = qb.match(
                x.has("tmdb-vote-count", lte(400))
        );

        assertThat(query, variable(x, containsInAnyOrder(apocalypseNow, theMuppets, chineseCoffee)));
    }

    @Test
    public void testDoublePredicateQuery() {
        Match query = qb.match(
                x.has("tmdb-vote-average", gt(7.8))
        );

        assertThat(query, variable(x, containsInAnyOrder(apocalypseNow, godfather)));
    }

    @Test
    public void testDatePredicateQuery() throws ParseException {
        Match query = qb.match(
                x.has("release-date", gte(LocalDateTime.of(1984, 6, 23, 12, 34, 56)))
        );

        assertThat(query, variable(x, containsInAnyOrder(spy, theMuppets, chineseCoffee)));
    }

    @Test
    public void testGlobalPredicateQuery() {
        Match query = qb.match(
                x.val(gt(500L)),
                x.val(lt(1000000L))
        );

        assertThat(query, variable(x, contains(both(hasValue(1000L)).and(hasType(tmdbVoteCount)))));
    }

    @Test
    public void testAssertionQuery() {
        Match query = qb.match(
                var("a").rel("production-with-cast", x).rel(y),
                y.has("name", "Miss Piggy"),
                var("a").isa("has-cast")
        );

        assertThat(query, variable(x, contains(theMuppets)));
    }

    @Test
    public void testAndOrPattern() {
        Match query = qb.match(
                x.isa("movie"),
                or(
                        and(y.isa("genre").has("name", "drama"), var().rel(x).rel(y)),
                        x.has("title", "The Muppets")
                )
        );

        assertThat(query, variable(x, containsInAnyOrder(godfather, apocalypseNow, heat, theMuppets, chineseCoffee)));
    }

    @Test
    public void testTypeAsVariable() {
        Match query = qb.match(label("genre").plays(x));
        assertThat(query, variable(x, containsInAnyOrder(genreOfProduction, keyNameOwner)));
    }

    @Test
    public void testVariableAsRoleType() {
        Match query = qb.match(var().rel(var().label("genre-of-production"), y));

        assertThat(query, variable(y, containsInAnyOrder(
                crime, drama, war, action, comedy, family, musical, fantasy
        )));
    }

    @Test
    public void testVariableAsRoleplayer() {
        Match query = qb.match(
                var().rel(x.isa("movie")).rel("genre-of-production", var().has("name", "crime"))
        );

        assertThat(query, variable(x, containsInAnyOrder(godfather, heat)));
    }

    @Test
    public void testVariablesEverywhere() {
        Match query = qb.match(
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
        Match query = qb.match(label("movie").sub(x));

        assertThat(query, variable(x, containsInAnyOrder(movie, production, entity, concept)));
    }

    @Test
    public void testIsResource() {
        Match query = qb.match(x.isa(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue())).limit(10);

        assertThat(query, iterableWithSize(10));
        assertThat(query, variable(x, everyItem(hasType(resource))));
    }

    @Test
    public void testHasReleaseDate() {
        Match query = qb.match(x.has("release-date", y));
        assertThat(query, variable(x, containsInAnyOrder(godfather, theMuppets, spy, chineseCoffee)));
    }

    @Test
    public void testAllowedToReferToNonExistentRoleplayer() {
        Match query = qb.match(var().rel("actor", var().id(ConceptId.of("999999999999999999"))));
        assertThat(query, emptyIterable());
    }

    @Test
    public void testRobertDeNiroNotRelatedToSelf() {
        Match query = qb.match(
                var().rel(x).rel(y).isa("has-cast"),
                y.has("name", "Robert de Niro")
        );

        assertThat(query, variable(x, containsInAnyOrder(heat, neilMcCauley)));
    }

    @Test
    public void testRobertDeNiroNotRelatedToSelfWhenMetaRoleIsSpecified() {
        // This can go wrong because one role-player may use a shortcut edge and the other may not
        Match query = qb.match(
                var().rel("role", x).rel("actor", y).isa("has-cast"),
                y.has("name", "Robert de Niro")
        );

        assertThat(query, variable(x, containsInAnyOrder(heat, neilMcCauley)));
    }

    @Test
    public void testKermitIsRelatedToSelf() {
        Match query = qb.match(
                var().rel(x).rel(y).isa("has-cast"),
                y.has("name", "Kermit The Frog")
        );

        assertThat(query, variable(x, (Matcher) hasItem(kermitTheFrog)));
    }

    @Test
    public void testKermitIsRelatedToSelfWhenMetaRoleIsSpecified() {
        Match query = qb.match(
                var().rel("role", x).rel(y).isa("has-cast"),
                y.has("name", "Kermit The Frog")
        );

        assertThat(query, variable(x, (Matcher) hasItem(kermitTheFrog)));
    }

    @Test
    public void whenQueryingForSuperRolesAndRelations_TheResultsAreTheSame() {
        assertEquals(
                Sets.newHashSet(qb.match(x.rel("work", y).rel("author", "z").isa("authored-by"))),
                Sets.newHashSet(qb.match(x.rel("production-being-directed", y).rel("director", "z").isa("directed-by")))
        );
    }

    @Test
    public void whenQueryingForSuperRolesAndRelationsWithOneRolePlayer_TheResultsAreTheSame() {
        // This is a special case which can cause comparisons between shortcut edges and castings
        assertEquals(
                Sets.newHashSet(qb.match(x.rel(y).rel("author", "z").isa("authored-by"))),
                Sets.newHashSet(qb.match(x.rel(y).rel("director", "z").isa("directed-by")))
        );
    }

    @Test
    public void whenQueryingForSuperRelationTypes_TheResultsAreTheSame() {
        assertEquals(
                Sets.newHashSet(qb.match(x.rel(y).rel("z").isa("authored-by"))),
                Sets.newHashSet(qb.match(x.rel(y).rel("z").isa("directed-by")))
        );
    }

    @Test
    public void testMatchDataType() {
        Match query = qb.match(x.datatype(AttributeType.DataType.DOUBLE));
        assertThat(query, variable(x, contains(tmdbVoteAverage)));

        query = qb.match(x.datatype(AttributeType.DataType.LONG));
        assertThat(query, variable(x, containsInAnyOrder(tmdbVoteCount, runtime)));

        query = qb.match(x.datatype(AttributeType.DataType.BOOLEAN));
        assertThat(query, variable(x, empty()));

        query = qb.match(x.datatype(AttributeType.DataType.STRING));

        assertThat(query, variable(x, containsInAnyOrder(title, gender, realName, name, provenance)));
        query = qb.match(x.datatype(AttributeType.DataType.DATE));
        assertThat(query, variable(x, contains(releaseDate)));
    }

    @Test
    public void testSelectRuleTypes() {
        Match query = qb.match(x.sub(RULE.getLabel().getValue()));
        assertThat(query, variable(x, containsInAnyOrder(
                rule, materializeRule, expectationRule
        )));
    }

    @Test
    public void testMatchRuleRightHandSide() {
        Match query = qb.match(x.when(qb.parser().parsePattern("$x id 'expect-when'")).then(qb.parser().parsePattern("$x id 'expect-then'")));

        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(MATCH_INVALID.getMessage("when"));

        query.forEach(r -> {
        });
    }

    @Test
    public void testDisconnectedQuery() {
        Match query = qb.match(x.isa("movie"), y.isa("person"));
        int numPeople = 10;
        assertThat(Sets.newHashSet(query), hasSize(movies.size() * numPeople));
    }

    @Test
    public void testSubRelationType() {
        // This method should work despite subs
        //noinspection ResultOfMethodCallIgnored
        qb.match(var().rel(x).rel("director", y).isa("authored-by")).stream().count();
    }

    @Test
    public void testHasVariable() {
        Match query = qb.match(var().has("title", "Godfather").has("tmdb-vote-count", x));
        assertThat(query, variable(x, contains(hasValue(1000L))));
    }

    @Test
    public void testRegexResourceType() {
        Match query = qb.match(x.regex("(fe)?male"));
        assertThat(query, variable(x, contains(gender)));
    }

    @Test
    public void testGraqlPlaysSemanticsMatchGraphAPI() {
        GraknTx tx = emptyKB.tx();
        QueryBuilder qb = tx.graql();

        Label a = Label.of("a");
        Label b = Label.of("b");
        Label c = Label.of("c");
        Label d = Label.of("d");
        Label e = Label.of("e");
        Label f = Label.of("f");

        qb.define(
                Graql.label(c).sub(Graql.label(b).sub(Graql.label(a).sub("entity"))),
                Graql.label(f).sub(Graql.label(e).sub(Graql.label(d).sub("role"))),
                Graql.label(b).plays(Graql.label(e))
        ).execute();

        Stream.of(a, b, c, d, e, f).forEach(type -> {
            Set<Concept> graqlPlays = qb.match(Graql.label(type).plays(x)).get(x).stream()
                                        .map(answer -> answer.get(x)).collect(Collectors.toSet());
            Collection<Role> graphAPIPlays;

            SchemaConcept schemaConcept = tx.getSchemaConcept(type);
            if (schemaConcept.isType()) {
                graphAPIPlays = schemaConcept.asType().playing().collect(toSet());
            } else {
                graphAPIPlays = Collections.EMPTY_SET;
            }

            assertEquals(graqlPlays, graphAPIPlays);
        });

        Stream.of(d, e, f).forEach(type -> {
            Set<Concept> graqlPlayedBy = qb.match(x.plays(Graql.label(type))).get(x).stream()
                                           .map(answer -> answer.get(x)).collect(Collectors.toSet());
            Collection<Type> graphAPIPlayedBy = tx.<Role>getSchemaConcept(type).players().collect(toSet());

            assertEquals(graqlPlayedBy, graphAPIPlayedBy);
        });
    }

    @Test
    public void testDistinctRoleplayers() {
        Match query = qb.match(var().rel(x).rel(y).rel(z).isa("has-cast"));

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
        Match query = qb.match(var().rel(x).rel(x).rel(x));
        assertThat(query, emptyIterable());
    }

    @Test
    public void testMatchAll() {
        Match query = qb.match(x);

        // Make sure there a reasonable number of results
        assertThat(query, iterableWithSize(greaterThan(10)));

        // Make sure results never contain shards
        assertThat(query, variable(x, everyItem(not(isShard()))));
    }

    @Test
    public void testMatchAllInstances() {
        Match query = qb.match(x.isa(Schema.MetaSchema.THING.getLabel().getValue()));

        // Make sure there a reasonable number of results
        assertThat(query, iterableWithSize(greaterThan(10)));

        assertThat(query, variable(x, everyItem(isInstance())));
    }

    @Test
    public void testMatchAllPairs() {
        int numConcepts = (int) qb.match(x).stream().count();
        Match pairs = qb.match(x, y);

        // We expect there to be a result for every pair of concepts
        assertThat(pairs, iterableWithSize(numConcepts * numConcepts));
    }

    @Test
    public void testMatchAllDistinctPairs() {
        int numConcepts = (int) qb.match(x).stream().count();
        Match pairs = qb.match(x.neq(y));

        // We expect there to be a result for every distinct pair of concepts
        assertThat(pairs, iterableWithSize(numConcepts * (numConcepts - 1)));
    }

    @Test
    public void testMatchAllDistinctPairsOfACertainType() {
        int numConcepts = (int) qb.match(x.isa("movie")).stream().count();
        Match pairs = qb.match(x.isa("movie"), y.isa("movie"), x.neq(y));

        assertThat(pairs, iterableWithSize(numConcepts * (numConcepts - 1)));
    }

    @Test
    public void testAllGreaterThanResources() {
        Match query = qb.match(x.val(gt(y)));

        assertThat(query, iterableWithSize(greaterThan(10)));

        query.forEach(result -> {
            Comparable cx = (Comparable) result.get(x).asAttribute().value();
            Comparable cy = (Comparable) result.get(y).asAttribute().value();
            assertThat(cx, greaterThan(cy));
        });
    }

    @Test
    public void testMoviesReleasedBeforeTheMuppets() {
        Match query = qb.match(
                x.has("release-date", lt(r)),
                var().has("title", "The Muppets").has("release-date", r)
        );

        assertThat(query, variable(x, contains(godfather)));
    }

    @Test
    public void testMoviesHasHigherTmdbCount() {

        Match query = qb.match(
                x.has("tmdb-vote-count", lt(r)),
                var().has("title", "The Muppets").has("tmdb-vote-count", r)
        );

        assertThat(query, variable(x, contains(chineseCoffee)));
    }

    @Test
    public void testAllLessThanAttachedResource() {
        Match query = qb.match(
                var("p").has("release-date", x),
                x.val(lte(y))
        );

        assertThat(query, iterableWithSize(greaterThan(5)));

        query.forEach(result -> {
            Comparable cx = (Comparable) result.get(x).asAttribute().value();
            Comparable cy = (Comparable) result.get(y).asAttribute().value();
            assertThat(cx, lessThanOrEqualTo(cy));
        });
    }

    @Test
    public void testMatchAllResourcesUsingResourceName() {
        Match match = qb.match(var().has("title", "Godfather").has(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue(), x));

        Thing godfather = movieKB.tx().getAttributeType("title").attribute("Godfather").owner();
        Set<Attribute<?>> expected = godfather.attributes().collect(toSet());

        Set<Attribute<?>> results = match.get(x).stream().map(answer -> answer.get(x).asAttribute()).collect(toSet());

        assertEquals(expected, results);
    }

    @Test
    public void testNoInstancesOfRoleType() {
        Match query = qb.match(x.isa(y), y.label("actor"));
        assertThat(query, emptyIterable());
    }

    @Test
    public void testNoInstancesOfRoleTypeUnselectedVariable() {
        Match query = qb.match(var().isa(y), y.label("actor"));
        assertThat(query, emptyIterable());
    }

    @Test
    public void testLookupResourcesOnId() {
        Thing godfather = movieKB.tx().getAttributeType("title").attribute("Godfather").owner();
        ConceptId id = godfather.id();
        Match query = qb.match(var().id(id).has("title", x));

        assertThat(query, variable(x, contains(hasValue("Godfather"))));
    }

    @Test
    public void testQueryDoesNotCrash() {
        qb.parse("match $m isa movie; (actor: $a1, $m); (actor: $a2, $m); get $a1, $a2;").execute();
    }

    @Test
    public void testMatchHas() {
        Match query = qb.match(x.has("name"));
        assertThat(query, variable(x, containsInAnyOrder(
                person, language, genre, cluster, character
        )));
    }

    @Test
    public void whenMatchingHas_ThenTheResultOnlyContainsTheExpectedVariables() {
        Match query = qb.match(x.has("name"));
        for (ConceptMap result : query) {
            assertEquals(result.vars(), ImmutableSet.of(x));
        }
    }

    @Test
    public void testMatchKey() {
        Match query = qb.match(x.key("name"));
        assertThat(query, variable(x, contains(genre)));
    }

    @Test
    public void testDontHideImplicitTypesIfExplicitlyMentioned() {
        Match query = qb.match(x.sub(Schema.MetaSchema.THING.getLabel().getValue()).label(HAS.getLabel("title")));
        assertThat(query, variable(x, (Matcher) hasItem(hasTitle)));
    }

    @Test
    public void whenReferringToImplicitRelationsAndRoles_DontHideResults() {
        VarPattern hasTitle = label(HAS.getLabel("title"));
        VarPattern titleOwner = label(HAS_OWNER.getLabel("title"));
        VarPattern titleValue = label(HAS_VALUE.getLabel("title"));

        Match hasQuery = qb.match(y.has("title", z));
        Match explicitQuery = qb.match(var().isa(hasTitle).rel(titleOwner, y).rel(titleValue, z));

        assertEquals(hasQuery.stream().collect(toSet()), explicitQuery.stream().collect(toSet()));
    }

    @Test
    public void testQueryNoVariables() {
        Match query = qb.match(x.isa("movie"));
        assertThat(query, allOf(
                (Matcher) everyItem(not(hasKey(anything()))),
                iterableWithSize(movies.size())
        ));
    }

    @Test(expected = GraqlQueryException.class)
    public void testMatchEmpty() {
        qb.match().stream().findAny().get();
    }

    @Test
    public void whenQueryDoesNotSpecifyRole_ResultIsTheSameAsSpecifyingMetaRole() {
        Set<ConceptMap> withoutRole = qb.match(var().rel(x).isa("has-cast")).stream().collect(toSet());
        Set<ConceptMap> withRole = qb.match(var().rel("role", x).isa("has-cast")).stream().collect(toSet());

        assertEquals(withoutRole, withRole);
    }

    @Test
    public void whenQueryingForSameRoleTwice_ReturnResultsWithMultipleRolePlayers() {
        Match query = qb.match(
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
        Match query = qb.match(var().rel("actor", x).rel("actor", y));

        assertThat(query, emptyIterable());
    }

    @Test
    public void whenQueryingForSameRoleTwice_DoNotReturnDuplicateRolePlayers() {
        Match query = qb.match(var().rel("cluster-of-production", x).rel("cluster-of-production", y));

        query.forEach(result -> {
            assertNotEquals(result.get(x), result.get(y));
        });
    }

    @Test
    public void whenQueryingForSuperRelationType_ReturnResults() {
        Match query = qb.match(var().isa(Schema.MetaSchema.RELATIONSHIP.getLabel().getValue()).rel(x).rel(y));
        assertExists(query);
    }

    @Test
    public void whenQueryingForSuperRoleType_ReturnResults() {
        Match query = qb.match(var().rel("role", x).rel(y));
        assertExists(query);
    }

    @Test
    public void whenQueryingForXSubY_ReturnOnlyTypes() {
        Match query = qb.match(x.sub(y));

        assertThat(query, variable(x, everyItem(not(isInstance()))));
        assertThat(query, variable(y, everyItem(not(isInstance()))));
    }


    @Test
    public void directSubThingOnlyReturnsToplevelTypes() {
        Match query = qb.match(x.subExplicit("thing"));
        HashSet<String> expectedLabels = new HashSet<>(Arrays.asList(
                "thing",
                "entity",
                "relationship",
                "attribute"
        ));
        List<ConceptMap> conceptMaps = query.stream().collect(Collectors.toList());
        assertTrue(conceptMaps.stream().allMatch(conceptMap ->
                expectedLabels.contains(
                        conceptMap.get("x").asSchemaConcept().label().toString()
                )
        ));
    }

    @Test
    public void directSubEntityOnlyReturnsDirectChildTypes() {
        Match query = qb.match(x.subExplicit("entity"));
        int expectedEntityTypesCount = 8;
        List<Concept> concepts = query.stream().map(conceptMap -> conceptMap.get("x")).collect(Collectors.toList());
        assertEquals(expectedEntityTypesCount, concepts.size());
    }

    @Test
    public void whenQueryingForHas_AllowReferringToTheImplicitRelation() {
        Label title = Label.of("title");

        RelationshipType hasTitle = movieKB.tx().getType(HAS.getLabel(title));
        Role titleOwner = movieKB.tx().getSchemaConcept(HAS_OWNER.getLabel(title));
        Role titleValue = movieKB.tx().getSchemaConcept(HAS_VALUE.getLabel(title));

        Relationship implicitRelation = hasTitle.instances().iterator().next();

        ConceptId owner = implicitRelation.rolePlayers(titleOwner).iterator().next().id();
        ConceptId value = implicitRelation.rolePlayers(titleValue).iterator().next().id();

        Match query = qb.match(x.id(owner).has(title, y.id(value), r));

        assertThat(query, variable(r, contains(MatchableConcept.of(implicitRelation))));
    }

    @Test
    public void whenQueryingForAResourceWhichHasItselfAsAResource_ReturnTheResource() {
        Match query = weirdKB.tx().graql().match(var("x").has("name", var("x")));

        assertThat(query, variable(x, contains(hasValue("weird"))));
    }

    @Test
    public void whenQueryingForAnImplicitRelationById_TheRelationIsReturned() {
        Match match = qb.match(var("x").isa(label(Schema.ImplicitType.HAS.getLabel("name"))));

        Relationship relationship = match.get("x").stream().map(answer -> answer.get("x").asRelationship()).findAny().get();

        Match queryById = qb.match(var("x").id(relationship.id()));

        assertThat(queryById, variable(x, contains(MatchableConcept.of(relationship))));
    }

    @Test
    public void whenQueryIsLimitedToANegativeNumber_Throw() {
        expectedException.expect(GraqlQueryException.class);
        expectedException.expectMessage(NON_POSITIVE_LIMIT.getMessage(Long.MIN_VALUE));
        //noinspection ResultOfMethodCallIgnored
        movieKB.tx().graql().match(var()).limit(Long.MIN_VALUE);
    }

    @Test
    public void whenQueryIsLimitedToZero_Throw() {
        expectedException.expect(GraqlQueryException.class);
        expectedException.expectMessage(NON_POSITIVE_LIMIT.getMessage(0L));
        //noinspection ResultOfMethodCallIgnored
        movieKB.tx().graql().match(var()).limit(0L);
    }

    @Test
    public void whenQueryIsOffsetByANegativeNumber_Throw() {
        expectedException.expect(GraqlQueryException.class);
        expectedException.expectMessage(NEGATIVE_OFFSET.getMessage(Long.MIN_VALUE));
        //noinspection ResultOfMethodCallIgnored
        movieKB.tx().graql().match(var()).offset(Long.MIN_VALUE);
    }

    @Test
    public void testDistinctTuple() {
        int size = (int) movieKB.tx().graql().match(x.isa("genre")).stream().count();
        size *= size;

        Match match1 = movieKB.tx().graql().match(
                x.isa("genre"),
                x.isa("genre"),
                x.isa("genre"),
                y.isa("genre"));
        assertThat(match1, iterableWithSize(size));

        Match match2 = movieKB.tx().graql().match(
                var().isa("genre"),
                var().isa("genre"),
                var().isa("genre"),
                var().isa("genre"),
                var().isa("genre"));
        assertThat(match2, iterableWithSize(1));

        Match match3 = movieKB.tx().graql().match(
                x.isa("genre"),
                y.isa("genre"));
        assertThat(match3, iterableWithSize(size));

        Match match4 = movieKB.tx().graql().match(
                var().isa("genre"),
                x.isa("genre"),
                y.isa("genre"));
        assertThat(match4, iterableWithSize(size));
    }

    @Test
    public void whenSelectingVarNotInQuery_Throw() {
        expectedException.expect(GraqlQueryException.class);
        expectedException.expectMessage(VARIABLE_NOT_IN_QUERY.getMessage(x));
        movieKB.tx().graql().match(var()).get(ImmutableSet.of(x)).execute();
    }

    @Test
    public void whenExecutingQueryWithManyRoleVars_DontThrow() {
        qb.match(var().rel(w, x).rel(y, z), var().rel(w, z)).iterator().next();
    }

    @Test(expected = Exception.class)
    public void testOrderBy1() {
        movieKB.tx().graql().match(var().isa("movie")).orderBy((String) null, Order.desc).stream().findAny().get();
    }

    @Test(expected = Exception.class)
    public void testOrderBy2() {
        movieKB.tx().graql().match(var().isa("movie")).orderBy((Var) null, Order.desc).stream().findAny().get();
    }

    @Test(expected = Exception.class)
    public void testOrderBy3() {
        movieKB.tx().graql().match(x.isa("movie")).orderBy((String) null, Order.desc).stream().findAny().get();
    }

    @Test(expected = Exception.class)
    public void testOrderBy4() {
        movieKB.tx().graql().match(x.isa("movie")).orderBy((Var) null, Order.desc).stream().findAny().get();
    }

    @Test(expected = Exception.class)
    public void testOrderBy5() {
        movieKB.tx().graql().match(x.isa("movie")).orderBy(y, Order.asc).stream().findAny().get();
    }

    @Test(expected = Exception.class)
    public void testOrderBy6() {
        movieKB.tx().graql().match(x.isa("movie")).orderBy(x, null).stream().findAny().get();
    }

    @Test(expected = Exception.class) //TODO: error message should be more specific
    public void testOrderBy7() {
        movieKB.tx().graql().match(x.isa("movie"),
                var().rel(x).rel(y)).orderBy(y, Order.asc).stream().findAny().get();
    }

    @Test(expected = Exception.class)
    public void testOrderBy8() {
        movieKB.tx().graql().match(x.isa("movie")).orderBy(x, Order.asc).stream().findAny().get();
    }

    @Test
    public void testOrderDescendingString() {
        ConceptMap answer = movieKB.tx().graql().match(x.isa("movie").has("title", y)).orderBy(y, Order.desc).stream().findFirst().get();
        assertEquals("The Muppets", answer.get(y).asAttribute().value());
    }

    @Test
    public void testOrderAscendingString() {
        ConceptMap answer = movieKB.tx().graql().match(x.isa("movie").has("title", y)).orderBy(y, Order.asc).stream().findFirst().get();
        assertEquals("Apocalypse Now", answer.get(y).asAttribute().value());
    }

    @Test
    public void testOrderDescendingDate() {
        ConceptMap answer = movieKB.tx().graql().match(x.isa("movie").has("release-date", y)).orderBy(y, Order.desc).stream().findFirst().get();
        assertEquals(LocalDateTime.of(2000, 9, 2, 0, 0), answer.get(y).asAttribute().value());
    }

    @Test
    public void testOrderAscendingDate() {
        ConceptMap answer = movieKB.tx().graql().match(x.isa("movie").has("release-date", y)).orderBy(y, Order.asc).stream().findFirst().get();
        assertEquals(LocalDateTime.of(1984, 1, 1, 0, 0), answer.get(y).asAttribute().value());
    }

    @Test
    public void whenExecutingGraqlTraversalFromGraph_ReturnExpectedResults() {
        EntityType type = movieKB.tx().putEntityType("Concept Type");
        Entity entity = type.create();

        Collection<Concept> results = movieKB.tx().graql().match(x.isa(type.label().getValue()))
                .stream().findAny().get().concepts();

        assertThat(results, containsInAnyOrder(entity));
    }
}