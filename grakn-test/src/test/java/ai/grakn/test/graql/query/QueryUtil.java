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
import ai.grakn.concept.Type;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.VarName;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.List;

import static ai.grakn.graphs.MovieGraph.apocalypseNow;
import static ai.grakn.graphs.MovieGraph.chineseCoffee;
import static ai.grakn.graphs.MovieGraph.godfather;
import static ai.grakn.graphs.MovieGraph.heat;
import static ai.grakn.graphs.MovieGraph.hocusPocus;
import static ai.grakn.graphs.MovieGraph.spy;
import static ai.grakn.graphs.MovieGraph.theMuppets;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

class QueryUtil {

    static final String[] movies = new String[] {
        "Godfather", "The Muppets", "Apocalypse Now", "Heat", "Hocus Pocus", "Spy", "Chinese Coffee"
    };

    static Matcher<MatchQuery> allVariables(Matcher<? extends Iterable<? extends Concept>> matcher) {
        return new TypeSafeDiagnosingMatcher<MatchQuery>() {
            @Override
            public boolean matchesSafely(MatchQuery matchQuery, Description mismatch) {
                List<? extends Concept> concepts = matchQuery.stream()
                        .flatMap(result -> result.values().stream())
                        .collect(toList());

                if (matcher.matches(concepts)) {
                    return true;
                } else {
                    mismatch.appendText("allVariables ");
                    matcher.describeMismatch(concepts, mismatch);
                    return false;
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("allVariables ").appendDescriptionOf(matcher);
            }
        };
    }

    static Matcher<MatchQuery> variable(String varName, Matcher<? extends Iterable<? extends Concept>> matcher) {
        VarName var = VarName.of(varName);

        return new TypeSafeDiagnosingMatcher<MatchQuery>() {
            @Override
            public boolean matchesSafely(MatchQuery matchQuery, Description mismatch) {
                List<? extends Concept> concepts = matchQuery.get(varName).collect(toList());

                if (matcher.matches(concepts)) {
                    return true;
                } else {
                    mismatch.appendText("variable ").appendValue(var).appendText(" was ").appendValue(concepts);
                    return false;
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("variable ").appendValue(var).appendText(" ").appendDescriptionOf(matcher);
            }
        };
    }

    static Matcher<Concept> hasValue(Object value) {
        return hasValue(is(value));
    }

    static Matcher<Concept> hasValue(Matcher<?> matcher) {
        return new TypeSafeDiagnosingMatcher<Concept>() {
            @Override
            protected boolean matchesSafely(Concept concept, Description mismatch) {
                Object value = concept.asResource().getValue();

                if (matcher.matches(value)) {
                    return true;
                } else {
                    matcher.describeMismatch(value, mismatch);
                    return false;
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("hasValue ").appendDescriptionOf(matcher);
            }
        };
    }

    static Matcher<Concept> hasType(Type type) {
        return new TypeSafeDiagnosingMatcher<Concept>() {
            @Override
            protected boolean matchesSafely(Concept concept, Description mismatch) {
                Instance instance = concept.asInstance();
                if (type.instances().contains(instance)) {
                    return true;
                } else {
                    mismatch.appendText("hasType ").appendValue(instance.type());
                    return false;
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("hasType ").appendValue(type);
            }
        };
    }

    static Matcher<Concept> isCasting() {
        return new TypeSafeMatcher<Concept>() {
            @Override
            public boolean matchesSafely(Concept concept) {
                return concept.isInstance() && concept.asInstance().type().isRoleType();
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("isCasting");
            }
        };
    }

    static Matcher<Concept> isInstance() {
        return new TypeSafeMatcher<Concept>() {
            @Override
            public boolean matchesSafely(Concept concept) {
                return concept.isInstance();
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("isInstance");
            }
        };
    }

    static Matcher<Iterable<? extends Instance>> containsAllMovies() {
        return containsInAnyOrder(godfather, theMuppets, apocalypseNow, heat, hocusPocus, spy, chineseCoffee);
    }
}
