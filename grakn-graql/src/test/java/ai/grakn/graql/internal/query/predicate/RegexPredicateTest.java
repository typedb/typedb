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

package ai.grakn.graql.internal.query.predicate;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Felix Chapman
 */
public class RegexPredicateTest {

    @Test
    public void regexPredicateInterpretsCharacterClassesCorrectly() {
        P<Object> predicate = RegexPredicate.of("\\d").getPredicate().get();

        assertTrue(predicate.test("0"));
        assertTrue(predicate.test("1"));
        assertFalse(predicate.test("a"));
    }

    @Test
    public void regexPredicateInterpretsQuotesCorrectly() {
        P<Object> predicate = RegexPredicate.of("\"").getPredicate().get();

        assertTrue(predicate.test("\""));
        assertFalse(predicate.test("\\\""));
    }

    @Test
    public void regexPredicateInterpretsBackslashesCorrectly() {
        P<Object> predicate = RegexPredicate.of("\\\\").getPredicate().get();

        assertTrue(predicate.test("\\"));
        assertFalse(predicate.test("\\\\"));
    }

    @Test
    public void regexPredicateInterpretsNewlineCorrectly() {
        P<Object> predicate = RegexPredicate.of("\\n").getPredicate().get();

        assertTrue(predicate.test("\n"));
        assertFalse(predicate.test("\\n"));
    }

    @Test
    public void regexPredicateToStringDoesNotEscapeMostThings() {
        RegexPredicate predicate = RegexPredicate.of("don't escape these: \\d, \", \n ok");

        assertEquals("/don't escape these: \\d, \", \n ok/", predicate.toString());
    }

    @Test
    public void regexPredicateToStringEscapesForwardSlashes() {
        RegexPredicate predicate = RegexPredicate.of("escape this: / ok");

        assertEquals("/escape this: \\/ ok/", predicate.toString());
    }
}