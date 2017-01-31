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
import static org.hamcrest.Matchers.hasItem;

/**
 * Collection of static methods to create {@link Matcher} instances for tests.
 */
public class GraknMatchers {

    public static final Matcher<MatchableConcept> concept = type(CONCEPT.getName());
    public static final Matcher<MatchableConcept> entity = type(ENTITY.getName());
    public static final Matcher<MatchableConcept> resource = type(RESOURCE.getName());
    public static final Matcher<MatchableConcept> rule = type(RULE.getName());
    public static final Matcher<MatchableConcept> inferenceRule = type(INFERENCE_RULE.getName());
    public static final Matcher<MatchableConcept> constraintRule = type(CONSTRAINT_RULE.getName());

    /**
     * Create a matcher to test against the results of a Graql query.
     */
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

    /**
     * Create a matcher to test against every variable of every result of a Graql query.
     */
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

    /**
     * Create matcher to test against a particular variable on every result of a Graql query.
     */
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

    /**
     * Create a matcher to test the value of a resource.
     */
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

    /**
     * Create a matcher to test the type of an instance.
     */
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

    /**
     * Create a matcher to test that the concept is a casting.
     */
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

    /**
     * Create a matcher to test that the concept is an instance.
     */
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

    /**
     * Create a matcher to test that the concept has the given type name.
     */
    static Matcher<MatchableConcept> type(String type) {
        return type(TypeName.of(type));
    }

    /**
     * Create a matcher to test that the concept has the given type name.
     */
    private static Matcher<MatchableConcept> type(TypeName expectedName) {
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

    /**
     * Create a matcher to test that the concept is an instance with a 'name' resource of the given value.
     * See {@link MatchableConcept#NAME_TYPES} for possible 'name' resources.
     */
    static Matcher<MatchableConcept> instance(Object value) {
        return instance(hasValue(value));
    }

    /**
     * Create a matcher to test that the concept is an instance with a 'name' resource that matches the given matcher.
     * See {@link MatchableConcept#NAME_TYPES} for possible 'name' resources.
     */
    private static Matcher<MatchableConcept> instance(Matcher<MatchableConcept> matcher) {
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
