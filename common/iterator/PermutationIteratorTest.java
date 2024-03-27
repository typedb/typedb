/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.iterator;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class PermutationIteratorTest {

    @Test
    public void test_permutations_with_n_equals_4() {
        PermutationIterator<Integer> it = new PermutationIterator<>(list(1, 2, 3, 4));
        List<List<Integer>> results = new ArrayList<>();
        for (int i = 0; it.hasNext(); i++) {
            List<Integer> p = new ArrayList<>(it.next());
            results.add(p);
            assertEquals(set(1, 2, 3, 4), set(p));
            System.out.println(i + ": " + p);
        }
        assertEquals(24, results.size());
        assertEquals(24, set(results).size());
    }

    @Test
    public void test_permutations_with_n_equals_2() {
        PermutationIterator<Integer> it = new PermutationIterator<>(list(1, 2));
        List<List<Integer>> results = new ArrayList<>();
        for (int i = 0; it.hasNext(); i++) {
            List<Integer> p = new ArrayList<>(it.next());
            results.add(p);
            assertEquals(set(1, 2), set(p));

            System.out.println(i + ": " + p);
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
