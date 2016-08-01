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

package io.mindmaps.graql.api.query;

import org.junit.Test;

import static org.junit.Assert.*;

public class PatternTest {

    @Test
    public void testVarPattern() {
        Pattern x = QueryBuilder.var("x");

        assertTrue(x.admin().isVar());
        assertFalse(x.admin().isDisjunction());
        assertFalse(x.admin().isConjunction());

        assertEquals(x.admin(), x.admin().asVar());
    }

    @Test
    public void testDisjunction() {
        Pattern disjunction = QueryBuilder.or();

        assertFalse(disjunction.admin().isVar());
        assertTrue(disjunction.admin().isDisjunction());
        assertFalse(disjunction.admin().isConjunction());

        //noinspection AssertEqualsBetweenInconvertibleTypes
        assertEquals(disjunction.admin(), disjunction.admin().asDisjunction());
    }

    @Test
    public void testConjunction() {
        Pattern conjunction = QueryBuilder.and();

        assertFalse(conjunction.admin().isVar());
        assertFalse(conjunction.admin().isDisjunction());
        assertTrue(conjunction.admin().isConjunction());

        //noinspection AssertEqualsBetweenInconvertibleTypes
        assertEquals(conjunction.admin(), conjunction.admin().asConjunction());
    }
}
