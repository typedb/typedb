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

package grakn.core.common.iterator;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.set;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class PermutationIteratorTest {

    @Test
    public void test_permutations_with_n_equals_4() {
        PermutationIterator<Integer> it = new PermutationIterator<>(list(1, 2, 3, 4));
        int i = 0;
        List<List<Integer>> results = new ArrayList();
        while (it.hasNext()) {
            List<Integer> p = new ArrayList<>(it.next());
            results.add(p);
            assertEquals(set(1, 2, 3, 4), set(p));
            System.out.println(i + ": " + p);
            i ++;
        }
        assertEquals(24, results.size());
        assertEquals(24, set(results).size());
    }

    @Test
    public void test_permutations_with_n_equals_2() {
        PermutationIterator<Integer> it = new PermutationIterator<>(list(1, 2));
        int i = 0;
        List<List<Integer>> results = new ArrayList();
        while (it.hasNext()) {
            List<Integer> p = new ArrayList<>(it.next());
            results.add(p);
            assertEquals(set(1, 2), set(p));

            System.out.println(i + ": " + p);
            i ++;
        }
        assertEquals(2, results.size());
        assertEquals(2, set(results).size());
    }

    @Test
    public void test_permutations_of_empty_list_is_one_empty_list() {
        PermutationIterator<Integer> it = new PermutationIterator<>(list());
        assertTrue(it.hasNext());
        assertEquals(list(), it.next());
        assertFalse(it.hasNext());
    }
}
