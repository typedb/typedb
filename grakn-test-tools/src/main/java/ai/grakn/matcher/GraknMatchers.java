/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.matcher;

import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.graql.Streamable;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.kb.internal.structure.Shard;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Map;
import java.util.Set;

import static ai.grakn.util.Schema.MetaSchema.ATTRIBUTE;
import static ai.grakn.util.Schema.MetaSchema.ENTITY;
import static ai.grakn.util.Schema.MetaSchema.RULE;
import static ai.grakn.util.Schema.MetaSchema.THING;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

/**
 * Collection of static methods to create {@link Matcher} instances for tests.
 *
 * @author Felix Chapman
 */
public class GraknMatchers {

    public static final Matcher<MatchableConcept> concept = type(THING.getLabel());
    public static final Matcher<MatchableConcept> entity = type(ENTITY.getLabel());
    public static final Matcher<MatchableConcept> resource = type(ATTRIBUTE.getLabel());
    public static final Matcher<MatchableConcept> rule = rule(RULE.getLabel());

    /**
     * Create a matcher to test against the results of a Graql query.
     */
    public static Matcher<Streamable<? extends Answer>> results(
            Matcher<? extends Iterable<? extends Map<? extends Var, ? extends MatchableConcept>>> matcher
    ) {
        return new PropertyMatcher<Streamable<? extends Answer>, Iterable<? extends Map<? extends Var, ? extends MatchableConcept>>>(matcher) {

            @Override
            public String getName() {
                return "results";
            }

            @Override
            Iterable<? extends Map<Var, ? extends MatchableConcept>> transform(Streamable<? extends Answer> item) {
                return item.stream().map(m -> Maps.transformValues(m.map(), MatchableConcept::of)).collect(toList());
            }
        };
    }

    /**
     * Create matcher to test against a particular variable on every result of a Graql query.
     */
    public static Matcher<Streamable<? extends Answer>> variable(
            Var var, Matcher<? extends Iterable<? extends MatchableConcept>> matcher
    ) {
        return new PropertyMatcher<Streamable<? extends Answer>, Iterable<? extends MatchableConcept>>(matcher) {

            @Override
            public String getName() {
                return "variable";
            }

            @Override
            Iterable<? extends MatchableConcept> transform(Streamable<? extends Answer> item) {
                return item.stream().map(answer -> MatchableConcept.of(answer.get(var))).collect(toList());
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
                return item.get().asAttribute().getValue();
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
                return getTypes(item.get().asThing()).stream().map(MatchableConcept::of).collect(toSet());
            }
        };
    }

    /**
     * Create a matcher to test that the concept is a shard.
     */
    public static Matcher<MatchableConcept> isShard() {
        return new TypeSafeMatcher<MatchableConcept>() {
            @Override
            public boolean matchesSafely(MatchableConcept concept) {
                return concept.get() instanceof Shard;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("isShard()");
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
                return concept.get().isThing();
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
    public static Matcher<MatchableConcept> type(String type) {
        return type(Label.of(type));
    }

    /**
     * Create a matcher to test that the concept has the given type name.
     */
    public static Matcher<MatchableConcept> type(Label expectedLabel) {
        return new PropertyEqualsMatcher<MatchableConcept, Label>(expectedLabel) {

            @Override
            public String getName() {
                return "type";
            }

            @Override
            Label transform(MatchableConcept item) {
                Concept concept = item.get();
                return concept.isType() ? concept.asType().getLabel() : null;
            }
        };
    }

    /**
     * Create a matcher to test that the concept has the given type name.
     */
    public static Matcher<MatchableConcept> role(String type) {
        return role(Label.of(type));
    }

    /**
     * Create a matcher to test that the concept has the given type name.
     */
    public static Matcher<MatchableConcept> role(Label expectedLabel) {
        return new PropertyEqualsMatcher<MatchableConcept, Label>(expectedLabel) {

            @Override
            public String getName() {
                return "role";
            }

            @Override
            Label transform(MatchableConcept item) {
                Concept concept = item.get();
                return concept.isRole() ? concept.asRole().getLabel() : null;
            }
        };
    }

    /**
     * Create a matcher to test that the concept has the given type name.
     */
    public static Matcher<MatchableConcept> rule(String type) {
        return rule(Label.of(type));
    }

    /**
     * Create a matcher to test that the concept has the given type name.
     */
    public static Matcher<MatchableConcept> rule(Label expectedLabel) {
        return new PropertyEqualsMatcher<MatchableConcept, Label>(expectedLabel) {

            @Override
            public String getName() {
                return "rule";
            }

            @Override
            Label transform(MatchableConcept item) {
                Concept concept = item.get();
                return concept.isRule() ? concept.asRule().getLabel() : null;
            }
        };
    }

    /**
     * Create a matcher to test that the concept is an instance with a 'name' resource of the given value.
     * See {@link MatchableConcept#NAME_TYPES} for possible 'name' resources.
     */
    public static Matcher<MatchableConcept> instance(Object value) {
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
                return item.get().asThing().attributes()
                        .filter(resource -> MatchableConcept.NAME_TYPES.contains(resource.type().getLabel()))
                        .map(MatchableConcept::of)
                        .collect(toSet());
            }

            @Override
            public Matcher<?> innerMatcher() {
                return matcher;
            }
        };
    }

    private static Set<Type> getTypes(Thing thing) {
        Set<Type> types = Sets.newHashSet();
        Type type = thing.type();

        while (type != null) {
            types.add(type);
            type = type.sup();
        }

        return types;
    }

    /**
     * A matcher for testing properties on objects.
     */
    private static abstract class PropertyEqualsMatcher<T, S> extends PropertyMatcher<T, S> {

        PropertyEqualsMatcher(S expected) {
            super(is(expected));
        }
    }

    /**
     * A matcher for testing properties on objects.
     */
    private static abstract class PropertyMatcher<T, S> extends TypeSafeDiagnosingMatcher<T> {

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
}
