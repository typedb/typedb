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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.gremlin;

/*-
 * #%L
 * grakn-graql
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Assert;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Provides {@link Matcher} implementations used by Graql tests in calls to {@link Assert#assertThat}.
 *
 * @author Felix Chapman
 */
public class GraqlMatchers {

    /**
     * Create a {@link Matcher} that checks that something satisfies the given {@link Predicate}.
     * <p>
     * Useful for describing more complicated {@link Matcher} classes, or for one-off tests.
     * <p>
     * Example:
     * <pre>
     * Matcher isEven = satisfies(x -> x % 2 == 0);
     * assertThat(10, isEven);
     * </pre>
     *
     * @param predicate the predicate to test against
     * @param <T> the type of the object being tested
     * @return a {@link Matcher} that checks that something satisfies the given {@link Predicate}
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
     * Create a {@link Matcher} that extracts a feature from an object, then applies a {@link Matcher} to that feature.
     * <p>
     * This is useful when there is a field or method on an object that must be tested against.
     * <p>
     * Example:
     * <pre>
     * Matcher stringReprMentionsGraql = feature(containsString("Graql"), "contains graql", Object::toString);
     * assertThat(Lists.newArrayList("1", "2", "Graql"), stringReprMentionsGraql);
     * </pre>
     *
     * @param subMatcher a {@link Matcher} to apply to the extracted feature
     * @param name the name of the resulting matcher
     * @param extractor a {@link Function} to extract a feature from the the object
     * @param <T> the type of the object being tested
     * @param <U> the type of the extracted feature
     * @return a {@link Matcher} that extracts a feature from an object, then applies a {@link Matcher} to that feature
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
