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
 *
 */

package ai.grakn.test.matcher;

import ai.grakn.concept.Instance;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeName;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.VarName;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static ai.grakn.graql.internal.util.StringConverter.typeNameToString;
import static ai.grakn.test.matcher.MatchableConcept.NAME_TYPES;
import static ai.grakn.util.Schema.MetaSchema.CONCEPT;
import static ai.grakn.util.Schema.MetaSchema.CONSTRAINT_RULE;
import static ai.grakn.util.Schema.MetaSchema.ENTITY;
import static ai.grakn.util.Schema.MetaSchema.INFERENCE_RULE;
import static ai.grakn.util.Schema.MetaSchema.RESOURCE;
import static ai.grakn.util.Schema.MetaSchema.RULE;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;

public class GraknMatchers {

    public static final Matcher<MatchableConcept> concept = type(CONCEPT.getName());
    public static final Matcher<MatchableConcept> entity = type(ENTITY.getName());
    public static final Matcher<MatchableConcept> resource = type(RESOURCE.getName());
    public static final Matcher<MatchableConcept> rule = type(RULE.getName());
    public static final Matcher<MatchableConcept> inferenceRule = type(INFERENCE_RULE.getName());
    public static final Matcher<MatchableConcept> constraintRule = type(CONSTRAINT_RULE.getName());

    public static final Matcher<MatchableConcept> production = type("production");
    public static final Matcher<MatchableConcept> movie = type("movie");
    public static final Matcher<MatchableConcept> person = type("person");
    public static final Matcher<MatchableConcept> genre = type("genre");
    public static final Matcher<MatchableConcept> character = type("character");
    public static final Matcher<MatchableConcept> cluster = type("cluster");
    public static final Matcher<MatchableConcept> language = type("language");
    public static final Matcher<MatchableConcept> title = type("title");
    public static final Matcher<MatchableConcept> gender = type("gender");
    public static final Matcher<MatchableConcept> realName = type("real-name");
    public static final Matcher<MatchableConcept> name = type("name");
    public static final Matcher<MatchableConcept> tmdbVoteCount = type("tmdb-vote-count");
    public static final Matcher<MatchableConcept> releaseDate = type("release-date");
    public static final Matcher<MatchableConcept> runtime = type("runtime");
    public static final Matcher<MatchableConcept> tmdbVoteAverage = type("tmdb-vote-average");
    public static final Matcher<MatchableConcept> genreOfProduction = type("genre-of-production");
    public static final Matcher<MatchableConcept> aRuleType = type("a-rule-type");
    public static final Matcher<MatchableConcept> hasTitle = type("has-title");

    public static final Matcher<MatchableConcept> godfather = instance("Godfather");
    public static final Matcher<MatchableConcept> theMuppets = instance("The Muppets");
    public static final Matcher<MatchableConcept> heat = instance("Heat");
    public static final Matcher<MatchableConcept> apocalypseNow = instance("Apocalypse Now");
    public static final Matcher<MatchableConcept> hocusPocus = instance("Hocus Pocus");
    public static final Matcher<MatchableConcept> spy = instance("Spy");
    public static final Matcher<MatchableConcept> chineseCoffee = instance("Chinese Coffee");
    public static final Matcher<MatchableConcept> marlonBrando = instance("Marlon Brando");
    public static final Matcher<MatchableConcept> alPacino = instance("Al Pacino");
    public static final Matcher<MatchableConcept> missPiggy = instance("Miss Piggy");
    public static final Matcher<MatchableConcept> kermitTheFrog = instance("Kermit The Frog");
    public static final Matcher<MatchableConcept> martinSheen = instance("Martin Sheen");
    public static final Matcher<MatchableConcept> robertDeNiro = instance("Robert de Niro");
    public static final Matcher<MatchableConcept> judeLaw = instance("Jude Law");
    public static final Matcher<MatchableConcept> mirandaHeart = instance("Miranda Heart");
    public static final Matcher<MatchableConcept> betteMidler = instance("Bette Midler");
    public static final Matcher<MatchableConcept> sarahJessicaParker = instance("Sarah Jessica Parker");
    public static final Matcher<MatchableConcept> crime = instance("crime");
    public static final Matcher<MatchableConcept> drama = instance("drama");
    public static final Matcher<MatchableConcept> war = instance("war");
    public static final Matcher<MatchableConcept> action = instance("action");
    public static final Matcher<MatchableConcept> comedy = instance("comedy");
    public static final Matcher<MatchableConcept> family = instance("family");
    public static final Matcher<MatchableConcept> musical = instance("musical");
    public static final Matcher<MatchableConcept> fantasy = instance("fantasy");
    public static final Matcher<MatchableConcept> benjaminLWillard = instance("Benjamin L. Willard");
    public static final Matcher<MatchableConcept> neilMcCauley = instance("Neil McCauley");
    public static final Matcher<MatchableConcept> sarah = instance("Sarah");
    public static final Matcher<MatchableConcept> harry = instance("Harry");

    public static final ImmutableSet<Matcher<? super MatchableConcept>> movies = ImmutableSet.of(
            godfather, theMuppets, apocalypseNow, heat, hocusPocus, spy, chineseCoffee
    );

    public static final Matcher<Iterable<? extends MatchableConcept>> containsAllMovies = containsInAnyOrder(movies);

    public static Matcher<MatchQuery> results(
            Matcher<? extends Iterable<? extends Map<String, ? extends MatchableConcept>>> matcher
    ) {
        return new TypeSafeDiagnosingMatcher<MatchQuery>() {
            @Override
            protected boolean matchesSafely(MatchQuery query, Description mismatch) {
                List<Map<String, MatchableConcept>> results =
                        query.stream().map(m -> Maps.transformValues(m, MatchableConcept::new)).collect(toList());

                if (matcher.matches(results)) {
                    return true;
                } else {
                    mismatch.appendText("results(");
                    matcher.describeMismatch(results, mismatch);
                    mismatch.appendText(")");
                    return false;
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("results(").appendDescriptionOf(matcher).appendText(")");
            }
        };
    }

    public static Matcher<MatchQuery> allVariables(Matcher<? extends Iterable<? extends MatchableConcept>> matcher) {
        return new TypeSafeDiagnosingMatcher<MatchQuery>() {
            @Override
            public boolean matchesSafely(MatchQuery matchQuery, Description mismatch) {
                List<? extends MatchableConcept> concepts = matchQuery.stream()
                        .flatMap(result -> result.values().stream())
                        .map(MatchableConcept::new)
                        .collect(toList());

                if (matcher.matches(concepts)) {
                    return true;
                } else {
                    mismatch.appendText("allVariables(");
                    matcher.describeMismatch(concepts, mismatch);
                    mismatch.appendText(")");
                    return false;
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("allVariables(").appendDescriptionOf(matcher).appendText(")");
            }
        };
    }

    public static Matcher<MatchQuery> variable(
            String varName, Matcher<? extends Iterable<? extends MatchableConcept>> matcher
    ) {
        VarName var = VarName.of(varName);

        return new TypeSafeDiagnosingMatcher<MatchQuery>() {
            @Override
            public boolean matchesSafely(MatchQuery matchQuery, Description mismatch) {
                List<? extends MatchableConcept> concepts =
                        matchQuery.get(varName).map(MatchableConcept::new).collect(toList());

                if (matcher.matches(concepts)) {
                    return true;
                } else {
                    mismatch.appendText("variable(");
                    matcher.describeMismatch(concepts, mismatch);
                    mismatch.appendText(")");
                    return false;
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("variable(")
                        .appendValue(var).appendText(", ").appendDescriptionOf(matcher).appendText(")");
            }
        };
    }

    static Matcher<MatchableConcept> type(String type) {
        return type(TypeName.of(type));
    }

    static Matcher<MatchableConcept> type(TypeName expectedName) {
        return new TypeSafeDiagnosingMatcher<MatchableConcept>() {
            @Override
            protected boolean matchesSafely(MatchableConcept concept, Description mismatch) {
                TypeName typeName = concept.get().asType().getName();

                if (typeName.equals(expectedName)) {
                    return true;
                } else {
                    mismatch.appendText("type(").appendText(typeNameToString(typeName)).appendText(")");
                    return false;
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("type(").appendValue(typeNameToString(expectedName)).appendText(")");
            }
        };
    }

    static Matcher<MatchableConcept> instance(Object value) {
        return instance(hasValue(value));
    }

    static Matcher<MatchableConcept> instance(Matcher<MatchableConcept> matcher) {
        Matcher<Iterable<? super MatchableConcept>> matchResources = hasItem(matcher);

        return new TypeSafeDiagnosingMatcher<MatchableConcept>() {
            @Override
            protected boolean matchesSafely(MatchableConcept concept, Description mismatch) {
                Set<MatchableConcept> resources = concept.get().asInstance().resources().stream()
                        .filter(resource -> NAME_TYPES.contains(resource.type().getName()))
                        .map(MatchableConcept::new)
                        .collect(toSet());

                if (matchResources.matches(resources)) {
                    return true;
                } else {
                    mismatch.appendText("instance(");
                    matchResources.describeMismatch(resources, mismatch);
                    mismatch.appendText(")");
                    return false;
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("instance(").appendDescriptionOf(matcher).appendText(")");
            }
        };
    }

    public static Matcher<MatchableConcept> hasValue(Object expectedValue) {
        return new TypeSafeDiagnosingMatcher<MatchableConcept>() {
            @Override
            protected boolean matchesSafely(MatchableConcept concept, Description mismatch) {
                Object value = concept.get().asResource().getValue();

                if (value.equals(expectedValue)) {
                    return true;
                } else {
                    mismatch.appendValue(value);
                    return false;
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendValue(expectedValue);
            }
        };
    }

    public static Matcher<MatchableConcept> hasType(Matcher<MatchableConcept> matcher) {
        Matcher<Iterable<? super MatchableConcept>> matchTypes = hasItem(matcher);

        return new TypeSafeDiagnosingMatcher<MatchableConcept>() {
            @Override
            protected boolean matchesSafely(MatchableConcept concept, Description mismatch) {
                Set<MatchableConcept> types =
                        getTypes(concept.get().asInstance()).stream().map(MatchableConcept::new).collect(toSet());

                if (matchTypes.matches(types)) {
                    return true;
                } else {
                    mismatch.appendText("hasType(");
                    matchTypes.describeMismatch(types, mismatch);
                    mismatch.appendText(")");
                    return false;
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("hasType(").appendDescriptionOf(matcher).appendText(")");
            }
        };
    }

    public static Matcher<MatchableConcept> isCasting() {
        return new TypeSafeMatcher<MatchableConcept>() {
            @Override
            public boolean matchesSafely(MatchableConcept concept) {
                return concept.get().isInstance() && concept.get().asInstance().type().isRoleType();
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("isCasting()");
            }
        };
    }

    public static Matcher<MatchableConcept> isInstance() {
        return new TypeSafeMatcher<MatchableConcept>() {
            @Override
            public boolean matchesSafely(MatchableConcept concept) {
                return concept.get().isInstance();
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("isInstance()");
            }
        };
    }

    private static Set<Type> getTypes(Instance instance) {
        Set<Type> types = Sets.newHashSet();
        Type type = instance.type();

        while (type != null) {
            types.add(type);
            type = type.superType();
        }

        return types;
    }

}
