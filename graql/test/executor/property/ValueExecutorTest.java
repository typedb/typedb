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
 */

package grakn.core.graql.executor.property;

import grakn.core.graql.planning.gremlin.value.ValueComparison;
import graql.lang.Graql;
import graql.lang.property.ValueProperty;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ValueExecutorTest {

    @Test
    public void regexPredicateInterpretsCharacterClassesCorrectly() {
        ValueComparison.String predicate = new ValueComparison.String(Graql.Token.Comparator.LIKE, "\\d");

        assertTrue(predicate.test("0"));
        assertTrue(predicate.test("1"));
        assertFalse(predicate.test("a"));
    }

    @Test
    public void regexPredicateInterpretsQuotesCorrectly() {
        ValueComparison.String predicate = new ValueComparison.String(Graql.Token.Comparator.LIKE, "\"");

        assertTrue(predicate.test("\""));
        assertFalse(predicate.test("\\\""));
    }

    @Test
    public void regexPredicateInterpretsBackslashesCorrectly() {
        ValueComparison.String predicate = new ValueComparison.String(Graql.Token.Comparator.LIKE, "\\\\");

        assertTrue(predicate.test("\\"));
        assertFalse(predicate.test("\\\\"));
    }

    @Test
    public void regexPredicateInterpretsNewlineCorrectly() {
        ValueComparison.String predicate = new ValueComparison.String(Graql.Token.Comparator.LIKE, "\\n");

        assertTrue(predicate.test("\n"));
        assertFalse(predicate.test("\\n"));
    }

    @Test
    public void regexPredicateToStringDoesNotEscapeMostThings() {
        ValueProperty.Operation.Comparison.String predicate = new ValueProperty.Operation
                .Comparison.String(Graql.Token.Comparator.LIKE, "don't escape these: \\d, \", \n ok");

        assertEquals(Graql.Token.Comparator.LIKE + " \"don't escape these: \\d, \", \n ok\"", predicate.toString());
    }

    @Test
    public void regexPredicateToStringEscapesForwardSlashes() {
        ValueProperty.Operation.Comparison.String predicate = new ValueProperty.Operation
                .Comparison.String(Graql.Token.Comparator.LIKE, "escape this: / ok");

        assertEquals(Graql.Token.Comparator.LIKE + " \"escape this: \\/ ok\"", predicate.toString());
    }
}