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
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.TypeName;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.VarName;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Set;

import static ai.grakn.example.MovieGraphFactory.apocalypseNow;
import static ai.grakn.example.MovieGraphFactory.chineseCoffee;
import static ai.grakn.example.MovieGraphFactory.godfather;
import static ai.grakn.example.MovieGraphFactory.heat;
import static ai.grakn.example.MovieGraphFactory.hocusPocus;
import static ai.grakn.example.MovieGraphFactory.spy;
import static ai.grakn.example.MovieGraphFactory.theMuppets;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsInAnyOrder;

class QueryUtil {

    static final String[] movies = new String[] {
        "Godfather", "The Muppets", "Apocalypse Now", "Heat", "Hocus Pocus", "Spy", "Chinese Coffee"
    };

    static Matcher<MatchQuery> variable(String varName, Matcher<? extends Iterable<? extends Concept>> matcher) {
        VarName var = VarName.of(varName);

        return new DiagnosingMatcher<MatchQuery>() {
            @Override
            public boolean matches(Object item, Description mismatch) {
                MatchQuery matchQuery = (MatchQuery) item;
                List<? extends Concept> concepts = matchQuery.get(varName).collect(toList());

                if (!matcher.matches(concepts)) {
                    mismatch.appendText("variable ").appendValue(var).appendText(" ");
                    matcher.describeMismatch(concepts, mismatch);
                    return false;
                } else {
                    return true;
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("variable ").appendValue(var).appendText(" ").appendDescriptionOf(matcher);
            }
        };
    }

    static Matcher<Concept> called(Object value) {
        return has(new ResourceType[] {}, value);
    }

    static <T> Matcher<Concept> has(ResourceType<T> type, T value) {
        return has(new ResourceType[] {type}, value);
    }

    static Matcher<Concept> has(ResourceType[] types, Object value) {
        return new DiagnosingMatcher<Concept>() {
            @Override
            public boolean matches(Object item, Description mismatch) {
                Concept concept = (Concept) item;
                Set<Object> values = concept.asInstance().resources(types).stream().map(Resource::getValue).collect(toSet());

                if (!values.contains(value)) {
                    mismatch.appendText("was ").appendValue(values);
                    return false;
                } else {
                    return true;
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendValue(value);
            }
        };
    }

    static Matcher<Concept> type(String typeNameValue) {
        TypeName typeName = TypeName.of(typeNameValue);

        return new DiagnosingMatcher<Concept>() {
            @Override
            public boolean matches(Object item, Description mismatch) {
                Concept concept = (Concept) item;
                TypeName thisName = concept.asType().getName();

                if (!thisName.equals(typeName)) {
                    mismatch.appendText("was ").appendValue(thisName);
                    return false;
                } else {
                    return true;
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendValue(typeName);
            }
        };
    }

    static Matcher<Iterable<? extends Instance>> containsAllMovies() {
        return containsInAnyOrder(godfather, theMuppets, apocalypseNow, heat, hocusPocus, spy, chineseCoffee);
    }
}
