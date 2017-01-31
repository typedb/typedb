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
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeName;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.util.StringConverter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static ai.grakn.graql.internal.util.StringConverter.typeNameToString;
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

class QueryUtil {

    static final ImmutableSet<TypeName> nameTypes = ImmutableSet.of(TypeName.of("name"), TypeName.of("title"));

    static final Matcher<MatchableConcept> concept = type(CONCEPT.getName());
    static final Matcher<MatchableConcept> entity = type(ENTITY.getName());
    static final Matcher<MatchableConcept> resource = type(RESOURCE.getName());
    static final Matcher<MatchableConcept> rule = type(RULE.getName());
    static final Matcher<MatchableConcept> inferenceRule = type(INFERENCE_RULE.getName());
    static final Matcher<MatchableConcept> constraintRule = type(CONSTRAINT_RULE.getName());

    static final Matcher<MatchableConcept> production = type("production");
    static final Matcher<MatchableConcept> movie = type("movie");
    static final Matcher<MatchableConcept> person = type("person");
    static final Matcher<MatchableConcept> genre = type("genre");
    static final Matcher<MatchableConcept> character = type("character");
    static final Matcher<MatchableConcept> cluster = type("cluster");
    static final Matcher<MatchableConcept> language = type("language");
    static final Matcher<MatchableConcept> title = type("title");
    static final Matcher<MatchableConcept> gender = type("gender");
    static final Matcher<MatchableConcept> realName = type("real-name");
    static final Matcher<MatchableConcept> name = type("name");
    static final Matcher<MatchableConcept> tmdbVoteCount = type("tmdb-vote-count");
    static final Matcher<MatchableConcept> releaseDate = type("release-date");
    static final Matcher<MatchableConcept> runtime = type("runtime");
    static final Matcher<MatchableConcept> tmdbVoteAverage = type("tmdb-vote-average");
    static final Matcher<MatchableConcept> genreOfProduction = type("genre-of-production");
    static final Matcher<MatchableConcept> aRuleType = type("a-rule-type");
    static final Matcher<MatchableConcept> hasTitle = type("has-title");

    static final Matcher<MatchableConcept> godfather = instance("Godfather");
    static final Matcher<MatchableConcept> theMuppets = instance("The Muppets");
    static final Matcher<MatchableConcept> heat = instance("Heat");
    static final Matcher<MatchableConcept> apocalypseNow = instance("Apocalypse Now");
    static final Matcher<MatchableConcept> hocusPocus = instance("Hocus Pocus");
    static final Matcher<MatchableConcept> spy = instance("Spy");
    static final Matcher<MatchableConcept> chineseCoffee = instance("Chinese Coffee");
    static final Matcher<MatchableConcept> marlonBrando = instance("Marlon Brando");
    static final Matcher<MatchableConcept> alPacino = instance("Al Pacino");
    static final Matcher<MatchableConcept> missPiggy = instance("Miss Piggy");
    static final Matcher<MatchableConcept> kermitTheFrog = instance("Kermit The Frog");
    static final Matcher<MatchableConcept> martinSheen = instance("Martin Sheen");
    static final Matcher<MatchableConcept> robertDeNiro = instance("Robert de Niro");
    static final Matcher<MatchableConcept> judeLaw = instance("Jude Law");
    static final Matcher<MatchableConcept> mirandaHeart = instance("Miranda Heart");
    static final Matcher<MatchableConcept> betteMidler = instance("Bette Midler");
    static final Matcher<MatchableConcept> sarahJessicaParker = instance("Sarah Jessica Parker");
    static final Matcher<MatchableConcept> crime = instance("crime");
    static final Matcher<MatchableConcept> drama = instance("drama");
    static final Matcher<MatchableConcept> war = instance("war");
    static final Matcher<MatchableConcept> action = instance("action");
    static final Matcher<MatchableConcept> comedy = instance("comedy");
    static final Matcher<MatchableConcept> family = instance("family");
    static final Matcher<MatchableConcept> musical = instance("musical");
    static final Matcher<MatchableConcept> fantasy = instance("fantasy");
    static final Matcher<MatchableConcept> benjaminLWillard = instance("Benjamin L. Willard");
    static final Matcher<MatchableConcept> neilMcCauley = instance("Neil McCauley");
    static final Matcher<MatchableConcept> sarah = instance("Sarah");
    static final Matcher<MatchableConcept> harry = instance("Harry");

    static final ImmutableSet<Matcher<? super MatchableConcept>> movies = ImmutableSet.of(
            godfather, theMuppets, apocalypseNow, heat, hocusPocus, spy, chineseCoffee
    );

    static final Matcher<Iterable<? extends MatchableConcept>> containsAllMovies = containsInAnyOrder(movies);

    static Matcher<MatchQuery> results(
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

    static Matcher<MatchQuery> allVariables(Matcher<? extends Iterable<? extends MatchableConcept>> matcher) {
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

    static Matcher<MatchQuery> variable(String varName, Matcher<? extends Iterable<? extends MatchableConcept>> matcher) {
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
                        .filter(resource -> nameTypes.contains(resource.type().getName()))
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

    static Matcher<MatchableConcept> hasValue(Object expectedValue) {
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

    static Matcher<MatchableConcept> hasType(Matcher<MatchableConcept> matcher) {
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

    static Matcher<MatchableConcept> isCasting() {
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

    static Matcher<MatchableConcept> isInstance() {
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

    static Set<Type> getTypes(Instance instance) {
        Set<Type> types = Sets.newHashSet();
        Type type = instance.type();

        while (type != null) {
            types.add(type);
            type = type.superType();
        }

        return types;
    }

    static class MatchableConcept {
        private final Concept concept;

        MatchableConcept(Concept concept) {
            this.concept = concept;
        }

        private Concept get() {
            return concept;
        }

        @Override
        public String toString() {
            if (concept.isInstance()) {

                Collection<Resource<?>> resources = concept.asInstance().resources();
                Optional<?> value = resources.stream()
                        .filter(resource -> nameTypes.contains(resource.type().getName()))
                        .map(Resource::getValue).findFirst();

                return "instance(" + value.map(StringConverter::valueToString).orElse("") + ")";
            } else {
                return "type(" + typeNameToString(concept.asType().getName()) + ")";
            }
        }
    }
}
