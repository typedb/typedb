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

package ai.grakn.graql.internal.gremlin;

import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author Felix Chapman
 */
public class GraqlMatchers {

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

    public static <T, U> Matcher<T> feature(Matcher<? super U> subMatcher, String name, Function<T, U> extractor) {
        return new FeatureMatcher<T, U>(subMatcher, name, name) {

            @Override
            protected U featureValueOf(T actual) {
                return extractor.apply(actual);
            }
        };
    }
}
