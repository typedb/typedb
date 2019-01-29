/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.graql.query.predicate;

import grakn.core.graql.internal.executor.property.ValueExecutor;
import grakn.core.graql.query.Query;
import grakn.core.graql.query.pattern.property.ValueProperty;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RegexPredicateTest {

    @Test
    public void regexPredicateInterpretsCharacterClassesCorrectly() {
        P<String> predicate = new ValueExecutor.Operation
                .Comparison.String(Query.Comparator.LIKE, "\\d").predicate();

        assertTrue(predicate.test("0"));
        assertTrue(predicate.test("1"));
        assertFalse(predicate.test("a"));
    }

    @Test
    public void regexPredicateInterpretsQuotesCorrectly() {
        P<String> predicate = new ValueExecutor.Operation
                .Comparison.String(Query.Comparator.LIKE, "\"").predicate();

        assertTrue(predicate.test("\""));
        assertFalse(predicate.test("\\\""));
    }

    @Test
    public void regexPredicateInterpretsBackslashesCorrectly() {
        P<String> predicate = new ValueExecutor.Operation
                .Comparison.String(Query.Comparator.LIKE, "\\\\").predicate();

        assertTrue(predicate.test("\\"));
        assertFalse(predicate.test("\\\\"));
    }

    @Test
    public void regexPredicateInterpretsNewlineCorrectly() {
        P<String> predicate = new ValueExecutor.Operation
                .Comparison.String(Query.Comparator.LIKE, "\\n").predicate();

        assertTrue(predicate.test("\n"));
        assertFalse(predicate.test("\\n"));
    }

    @Test
    public void regexPredicateToStringDoesNotEscapeMostThings() {
        ValueProperty.Operation.Comparison.String predicate = new ValueProperty.Operation
                .Comparison.String(Query.Comparator.LIKE, "don't escape these: \\d, \", \n ok");

        assertEquals(Query.Comparator.LIKE + " \"don't escape these: \\d, \", \n ok\"", predicate.toString());
    }

    @Test
    public void regexPredicateToStringEscapesForwardSlashes() {
        ValueProperty.Operation.Comparison.String predicate = new ValueProperty.Operation
                .Comparison.String(Query.Comparator.LIKE, "escape this: / ok");

        assertEquals(Query.Comparator.LIKE + " \"escape this: \\/ ok\"", predicate.toString());
    }
}