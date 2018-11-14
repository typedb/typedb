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

package grakn.core.util;

import grakn.core.Transaction;
import grakn.core.graql.GetQuery;
import grakn.core.graql.Pattern;
import grakn.core.graql.QueryBuilder;

import java.util.Collection;
import org.apache.commons.collections.CollectionUtils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Helper methods for writing tests for Graql
 *
 */
public class GraqlTestUtil {

    public static void assertExists(Transaction tx, Pattern... patterns) {
        assertExists(tx.graql(), patterns);
    }

    public static void assertExists(QueryBuilder qb, Pattern... patterns) {
        assertExists(qb.match(patterns));
    }

    public static void assertExists(Iterable<?> iterable) {
        assertTrue(iterable.iterator().hasNext());
    }

    public static void assertNotExists(Transaction tx, Pattern... patterns) {
        assertNotExists(tx.graql(), patterns);
    }

    public static void assertNotExists(QueryBuilder qb, Pattern... patterns) {
        assertNotExists(qb.match(patterns));
    }

    public static void assertNotExists(Iterable<?> iterable) {
        assertFalse(iterable.iterator().hasNext());
    }

    public static <T> void assertCollectionsEqual(Collection<T> c1, Collection<T> c2) {
        assertTrue(CollectionUtils.isEqualCollection(c1, c2));
    }

    public static void assertQueriesEqual(GetQuery q1, GetQuery q2) {
        assertCollectionsEqual(q1.execute(), q2.execute());
    }
}
