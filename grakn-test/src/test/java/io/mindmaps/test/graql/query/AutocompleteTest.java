/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.test.graql.query;

import com.google.common.collect.Sets;
import io.mindmaps.graql.Autocomplete;
import io.mindmaps.test.AbstractMovieGraphTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AutocompleteTest extends AbstractMovieGraphTest {

    @Test
    public void testAutocompleteEmpty() {
        Autocomplete autocomplete = Autocomplete.create(graph, "", 0);
        assertTrue(autocomplete.getCandidates().contains("match"));
        assertTrue(autocomplete.getCandidates().contains("isa"));
        assertTrue(autocomplete.getCandidates().contains("movie"));
        assertFalse(autocomplete.getCandidates().contains("$x"));
        assertEquals(0, autocomplete.getCursorPosition());
    }

    @Test
    public void testAutocompleteKeywords() {
        String queryString = "match $x isa movie; sel";
        Autocomplete autocomplete = Autocomplete.create(graph, queryString, queryString.length());
        assertTrue(autocomplete.getCandidates().contains("select"));
        assertFalse(autocomplete.getCandidates().contains("match"));
        assertEquals(queryString.length() - 3, autocomplete.getCursorPosition());
    }

    @Test
    public void testAutocompleteKeywordCursorInQuery() {
        String queryString = " matc $x isa person";
        Autocomplete autocomplete = Autocomplete.create(graph, queryString, 4);
        assertTrue(autocomplete.getCandidates().contains("match"));
        assertFalse(autocomplete.getCandidates().contains("insert"));
        assertEquals(1, autocomplete.getCursorPosition());
    }

    @Test
    public void testAutocompleteKeywordCursorInWord() {
        String queryString = "match $x has-re title";
        Autocomplete autocomplete = Autocomplete.create(graph, queryString, 11);
        assertTrue(autocomplete.getCandidates().contains("has-resource"));
        assertFalse(autocomplete.getCandidates().contains("delete"));
        assertEquals(9, autocomplete.getCursorPosition());
    }

    @Test
    public void testAutocompleteSpace() {
        String queryString = "match";
        Autocomplete autocomplete = Autocomplete.create(graph, queryString, queryString.length());
        assertEquals(Sets.newHashSet(" "), autocomplete.getCandidates());
        assertEquals(queryString.length(), autocomplete.getCursorPosition());
    }

    @Test
    public void testAutocompleteType() {
        String queryString = "insert $x isa pro";
        Autocomplete autocomplete = Autocomplete.create(graph, queryString, queryString.length());
        assertTrue(autocomplete.getCandidates().contains("production"));
        assertEquals(queryString.length() - 3, autocomplete.getCursorPosition());
    }

    @Test
    public void testAutocompleteVariables() {
        String queryString = "insert $x isa ";
        Autocomplete autocomplete = Autocomplete.create(graph, queryString, queryString.length());
        assertTrue(autocomplete.getCandidates().contains("$x"));
        assertEquals(queryString.length(), autocomplete.getCursorPosition());
    }

    @Test
    public void testAutocompleteVariablesDollar() {
        String queryString = "insert $x isa $";
        Autocomplete autocomplete = Autocomplete.create(graph, queryString, queryString.length());
        assertTrue(autocomplete.getCandidates().contains("$x"));
        assertEquals(queryString.length() - 1, autocomplete.getCursorPosition());
    }
}
