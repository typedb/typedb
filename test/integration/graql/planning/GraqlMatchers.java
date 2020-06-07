/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.graql.planning;

import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Provides Matcher implementations used by Graql tests in calls to Assert#assertThat.
 *
 */
public class GraqlMatchers {

    /**
     * Create a Matcher that checks that something satisfies the given Predicate.
     * <p>
     * Useful for describing more complicated Matcher classes, or for one-off tests.
     * <p>
     * Example:
     * <pre>
     * Matcher isEven = satisfies(x -> x % 2 == 0);
     * assertThat(10, isEven);
     * </pre>
     *
     * @param predicate the predicate to test against
     * @param <T> the type of the object being tested
     * @return a Matcher that checks that something satisfies the given Predicate
     */
    public static <T> Matcher<T> satisfies(Predicate<T> predicate) {
        return new TypeSafeDiagnosingMatcher<T>() {
            @Override
            protected boolean matchesSafely(T item, Description mismatchDescription) {
                return predicate.test(item);
            }

            @Override
            public void describeTo(Description description) {

            }
        };
    }

    /**
     * Create a Matcher that extracts a feature from an object, then applies a Matcher to that feature.
     * <p>
     * This is useful when there is a field or method on an object that must be tested against.
     * <p>
     * Example:
     * <pre>
     * Matcher stringReprMentionsGraql = feature(containsString("Graql"), "contains graql", Object::toString);
     * assertThat(Lists.newArrayList("1", "2", "Graql"), stringReprMentionsGraql);
     * </pre>
     *
     * @param subMatcher a Matcher to apply to the extracted feature
     * @param name the name of the resulting matcher
     * @param extractor a Function to extract a feature from the the object
     * @param <T> the type of the object being tested
     * @param <U> the type of the extracted feature
     * @return a Matcher that extracts a feature from an object, then applies a Matcher to that feature
     */
    public static <T, U> Matcher<T> feature(Matcher<? super U> subMatcher, String name, Function<T, U> extractor) {
        return new FeatureMatcher<T, U>(subMatcher, name, name) {

            @Override
            protected U featureValueOf(T actual) {
                return extractor.apply(actual);
            }
        };
    }
}
