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
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.MatchQuery;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Map;
import java.util.Set;

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
import static org.hamcrest.Matchers.is;

/**
 * Collection of static methods to create {@link Matcher} instances for tests.
 */
public class GraknMatchers {

    public static final Matcher<MatchableConcept> concept = type(CONCEPT.getLabel());
    public static final Matcher<MatchableConcept> entity = type(ENTITY.getLabel());
    public static final Matcher<MatchableConcept> resource = type(RESOURCE.getLabel());
    public static final Matcher<MatchableConcept> rule = type(RULE.getLabel());
    public static final Matcher<MatchableConcept> inferenceRule = type(INFERENCE_RULE.getLabel());
    public static final Matcher<MatchableConcept> constraintRule = type(CONSTRAINT_RULE.getLabel());

    /**
     * Create a matcher to test against the results of a Graql query.
     */
    public static Matcher<MatchQuery> results(
            Matcher<? extends Iterable<? extends Map<? extends String, ? extends MatchableConcept>>> matcher
    ) {
        return new PropertyMatcher<MatchQuery, Iterable<? extends Map<? extends String, ? extends MatchableConcept>>>(matcher) {

            @Override
            public String getName() {
                return "results";
            }

            @Override
            Iterable<? extends Map<String, ? extends MatchableConcept>> transform(MatchQuery item) {
                return item.stream().map(m -> Maps.transformValues(m, MatchableConcept::new)).collect(toList());
            }
        };
    }

    /**
     * Create a matcher to test against every variable of every result of a Graql query.
     */
    public static Matcher<MatchQuery> allVariables(Matcher<? extends Iterable<? extends MatchableConcept>> matcher) {
        return new PropertyMatcher<MatchQuery, Iterable<? extends MatchableConcept>>(matcher) {

            @Override
            public String getName() {
                return "allVariables";
            }

            @Override
            Iterable<? extends MatchableConcept> transform(MatchQuery item) {
                return item.stream()
                        .flatMap(result -> result.values().stream())
                        .map(MatchableConcept::new)
                        .collect(toList());
            }
        };
    }

    /**
     * Create matcher to test against a particular variable on every result of a Graql query.
     */
    public static Matcher<MatchQuery> variable(
            String varName, Matcher<? extends Iterable<? extends MatchableConcept>> matcher
    ) {
        return new PropertyMatcher<MatchQuery, Iterable<? extends MatchableConcept>>(matcher) {

            @Override
            public String getName() {
                return "variable";
            }

            @Override
            Iterable<? extends MatchableConcept> transform(MatchQuery item) {
                return item.get(varName).map(MatchableConcept::new).collect(toList());
            }
        };
    }

    /**
     * Create a matcher to test the value of a resource.
     */
    public static Matcher<MatchableConcept> hasValue(Object expectedValue) {
        return new PropertyEqualsMatcher<MatchableConcept, Object>(expectedValue) {

            @Override
            public String getName() {
                return "hasValue";
            }

            @Override
            public Object transform(MatchableConcept item) {
                return item.get().asResource().getValue();
            }
        };
    }

    /**
     * Create a matcher to test the type of an instance.
     */
    public static Matcher<MatchableConcept> hasType(Matcher<MatchableConcept> matcher) {
        return new PropertyMatcher<MatchableConcept, Iterable<? super MatchableConcept>>(hasItem(matcher)) {

            @Override
            public String getName() {
                return "hasType";
            }

            @Override
            Iterable<? super MatchableConcept> transform(MatchableConcept item) {
                return getTypes(item.get().asInstance()).stream().map(MatchableConcept::new).collect(toSet());
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
        return type(TypeLabel.of(type));
    }

    /**
     * Create a matcher to test that the concept has the given type name.
     */
    static Matcher<MatchableConcept> type(TypeLabel expectedLabel) {
        return new PropertyEqualsMatcher<MatchableConcept, TypeLabel>(expectedLabel) {

            @Override
            public String getName() {
                return "type";
            }

            @Override
            TypeLabel transform(MatchableConcept item) {
                return item.get().asType().getLabel();
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
        return new PropertyMatcher<MatchableConcept, Iterable<? super MatchableConcept>>(hasItem(matcher)) {

            @Override
            public String getName() {
                return "instance";
            }

            @Override
            Iterable<? super MatchableConcept> transform(MatchableConcept item) {
                return item.get().asInstance().resources().stream()
                        .filter(resource -> NAME_TYPES.contains(resource.type().getLabel()))
                        .map(MatchableConcept::new)
                        .collect(toSet());
            }

            @Override
            public Matcher<?> innerMatcher() {
                return matcher;
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


/**
 * A matcher for testing properties on objects.
 */
abstract class PropertyEqualsMatcher<T, S> extends PropertyMatcher<T, S> {

    PropertyEqualsMatcher(S expected) {
        super(is(expected));
    }
}


/**
 * A matcher for testing properties on objects.
 */
abstract class PropertyMatcher<T, S> extends TypeSafeDiagnosingMatcher<T> {

    private final Matcher<? extends S> matcher;

    PropertyMatcher(Matcher<? extends S> matcher) {
        this.matcher = matcher;
    }

    @Override
    protected final boolean matchesSafely(T item, Description mismatch) {
        S transformed = transform(item);

        if (matcher.matches(transformed)) {
            return true;
        } else {
            mismatch.appendText(getName()).appendText("(");
            matcher.describeMismatch(transformed, mismatch);
            mismatch.appendText(")");
            return false;
        }
    }

    @Override
    public final void describeTo(Description description) {
        description.appendText(getName()).appendText("(").appendDescriptionOf(innerMatcher()).appendText(")");
    }

    public abstract String getName();

    public Matcher<?> innerMatcher() {
        return matcher;
    }

    abstract S transform(T item);
}
