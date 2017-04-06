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

import ai.grakn.graphs.MovieGraph;
import ai.grakn.graql.Autocomplete;
import ai.grakn.test.GraphContext;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class AutocompleteTest {

    private final Set<String> types = ImmutableSet.of("production", "movie", "person");

    @ClassRule
    public static final GraphContext rule = GraphContext.preLoad(MovieGraph.get());

    @Test
    public void whenAutocompletingAnEmptyQuery_CandidatesIncludeTypesAndKeywords() {
        Autocomplete autocomplete = Autocomplete.create(types, "", 0);
        assertThat(autocomplete.getCandidates(), hasItems("match", "isa", "movie"));
        assertThat(autocomplete.getCandidates(), not(hasItem("$x")));
        assertEquals(0, autocomplete.getCursorPosition());
    }

    @Test
    public void whenAutocompletingHalfwayThroughAWord_CandidatesOnlyIncludeKeywordsWithCorrectPrefix() {
        String queryString = "match $x isa movie; sel";
        Autocomplete autocomplete = Autocomplete.create(types, queryString, queryString.length());
        assertThat(autocomplete.getCandidates(), hasItem("select"));
        assertThat(autocomplete.getCandidates(), not(hasItem("match")));
        assertEquals(queryString.length() - 3, autocomplete.getCursorPosition());
    }

    @Test
    public void whenAutocompletingWithinQuery_AppropriateCandidatesAreFound() {
        String queryString = " matc $x isa person";
        Autocomplete autocomplete = Autocomplete.create(types, queryString, 4);
        assertThat(autocomplete.getCandidates(), hasItem("match"));
        assertThat(autocomplete.getCandidates(), not(hasItem("insert")));
        assertEquals(1, autocomplete.getCursorPosition());
    }

    @Test
    public void whenAutocompletingWithinWord_AppropriateCandidatesAreFound() {
        String queryString = "match $x has title";
        Autocomplete autocomplete = Autocomplete.create(types, queryString, 11);
        assertThat(autocomplete.getCandidates(), hasItem("has"));
        assertThat(autocomplete.getCandidates(), not(hasItem("delete")));
        assertEquals(9, autocomplete.getCursorPosition());
    }

    @Test
    public void whenAutocompletingAfterACompleteKeyword_InsertASpace() {
        String queryString = "match";
        Autocomplete autocomplete = Autocomplete.create(types, queryString, queryString.length());
        assertEquals(Sets.newHashSet(" "), autocomplete.getCandidates());
        assertEquals(queryString.length(), autocomplete.getCursorPosition());
    }

    @Test
    public void whenAutocompletingAQuery_IncludeTypesInTheOntology() {
        String queryString = "insert $x isa pro";
        Autocomplete autocomplete = Autocomplete.create(types, queryString, queryString.length());
        assertThat(autocomplete.getCandidates(), hasItem("production"));
        assertEquals(queryString.length() - 3, autocomplete.getCursorPosition());
    }

    @Test
    public void whenAutocompletingAQuery_IncludeVariablesInTheQuery() {
        String queryString = "insert $x isa ";
        Autocomplete autocomplete = Autocomplete.create(types, queryString, queryString.length());
        assertThat(autocomplete.getCandidates(), hasItem("$x"));
        assertEquals(queryString.length(), autocomplete.getCursorPosition());
    }

    @Test
    public void whenAutocompletingAQueryStartingWithADollar_IncludeVariablesInTheQuery() {
        String queryString = "insert $x isa $";
        Autocomplete autocomplete = Autocomplete.create(types, queryString, queryString.length());
        assertThat(autocomplete.getCandidates(), hasItem("$x"));
        assertEquals(queryString.length() - 1, autocomplete.getCursorPosition());
    }
}
