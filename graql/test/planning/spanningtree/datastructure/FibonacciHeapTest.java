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
package grakn.core.graql.planning.spanningtree.datastructure;

import com.google.common.collect.Maps;
import grakn.core.kb.graql.planning.spanningtree.datastructure.FibonacciHeap;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * These tests have been modified by Sam Thomson (sthomson@cs.cmu.edu)
 * from the original tests by Nathan L. Fiedler
 * https://github.com/nlfiedler/graphmaker/blob/990227c766a9891be1d4669c582975f7c1a4db87/core/test/unit/src/com/bluemarsh/graphmaker/core/util/FibonacciHeapTest.java
 */
public class FibonacciHeapTest {
    @Test
    public void testDecreasePriority() {
        // Inserts a set of elements, decreases some of their priorities, then
        // extracts them by key order and ensures everything comes out
        // in the order expected.
        final FibonacciHeap<Integer, Integer> heap = FibonacciHeap.create();
        assertTrue(heap.isEmpty());
        assertEquals(0, heap.size());
        // keep references to entries as we add them so we can decreasePriority and remove
        final Map<Integer, FibonacciHeap<Integer, Integer>.Entry> entries = Maps.newHashMap();
        for (int i = 100; i < 200; i++) {
            entries.put(i, heap.add(i, i));
        }
        assertFalse(heap.isEmpty());
        assertEquals(100, heap.size());
        heap.decreasePriority(entries.get(110), 50);
        heap.decreasePriority(entries.get(140), 25);
        heap.decreasePriority(entries.get(160), 15);
        // Last one should be the min value.
        assertEquals(heap.peek(), entries.get(160));
        assertEquals(160, heap.poll().intValue());
        // Second last should now be the min value.
        assertEquals(heap.peek(), entries.get(140));
        heap.remove(entries.get(140));
        // Remove the third smallest entry.
        heap.remove(entries.get(110));
        // Original min value should now be the min.
        assertEquals(heap.peek(), entries.get(100));
        heap.clear();
        assertTrue(heap.isEmpty());
        assertEquals(0, heap.size());
    }

    @Test
    public void testDuplicates() {
        final FibonacciHeap<Integer, Double> heap = FibonacciHeap.create();
        assertTrue(heap.isEmpty());
        assertEquals(0, heap.size());
        // Insert entries with duplicate priorities.
        final double priority = Double.MIN_NORMAL;
        for (int i = 1; i < 1001; i++) {
            heap.add(i, priority);
        }
        assertFalse(heap.isEmpty());
        assertEquals(1000, heap.size());
        heap.poll();
        assertFalse(heap.isEmpty());
        assertEquals(999, heap.size());
        heap.clear();
        assertTrue(heap.isEmpty());
        assertEquals(0, heap.size());
    }

    @Test
    public void testDuplicatesLarger() {
        // Test a heap consisting of all duplicate priorities, except for one
        // whose value is greater than the others.
        final FibonacciHeap<Integer, Double> heap = FibonacciHeap.create();
        assertTrue(heap.isEmpty());
        assertEquals(0, heap.size());
        // Insert entries with duplicate priorities.
        final double priority = 0.0;
        for (int i = 1; i < 1000; i++) {
            heap.add(i, priority);
        }
        heap.add(1001, Double.MIN_NORMAL);
        assertFalse(heap.isEmpty());
        assertEquals(1000, heap.size());
        assertTrue(heap.poll() < 1001);
        assertFalse(heap.isEmpty());
        assertEquals(999, heap.size());
        heap.clear();
        assertTrue(heap.isEmpty());
        assertEquals(0, heap.size());
    }

    @Test
    public void testDuplicatesSmaller() {
        // Test a heap consisting of all duplicate priorities, except for one
        // whose value is less than the others.
        final FibonacciHeap<Integer, Double> heap = FibonacciHeap.create();
        assertTrue(heap.isEmpty());
        assertEquals(0, heap.size());
        // Insert entries with duplicate priorities.
        final double priority = Double.MIN_NORMAL;
        for (int i = 1; i < 1000; i++) {
            heap.add(i, priority);
        }
        heap.add(1001, 0.0);
        assertFalse(heap.isEmpty());
        assertEquals(1000, heap.size());
        assertEquals(1001, heap.poll().intValue());
        assertFalse(heap.isEmpty());
        assertEquals(999, heap.size());
        heap.clear();
        assertTrue(heap.isEmpty());
        assertEquals(0, heap.size());
    }

    @Test
    public void testUnion() {
        final FibonacciHeap<Integer, Integer> heap1 = FibonacciHeap.create();
        assertTrue(heap1.isEmpty());
        assertEquals(0, heap1.size());
        heap1.add(1, 1);
        heap1.add(2, 2);
        heap1.add(3, 3);
        heap1.add(4, 4);
        heap1.add(5, 5);
        assertFalse(heap1.isEmpty());
        assertEquals(5, heap1.size());
        final FibonacciHeap<Integer, Integer> heap2 = FibonacciHeap.create();
        assertTrue(heap2.isEmpty());
        assertEquals(0, heap2.size());
        heap2.add(6, 6);
        heap2.add(7, 7);
        heap2.add(8, 8);
        heap2.add(9, 9);
        heap2.add(10, 10);
        assertFalse(heap2.isEmpty());
        assertEquals(5, heap2.size());
        final FibonacciHeap<Integer, Integer> joined = FibonacciHeap.merge(heap1, heap2);
        assertFalse(joined.isEmpty());
        assertEquals(10, joined.size());
        int oVal = joined.poll();
        int i = 1;
        assertTrue(oVal == i);
        while (!joined.isEmpty()) {
            oVal = joined.poll();
            assertTrue(oVal > i);
            i = oVal;
        }
        assertTrue(joined.isEmpty());
        assertEquals(0, joined.size());
    }

    @Test
    public void testMinComparison() {
        // Test case contributed by Travis Wheeler which exposed a problem
        // when the min pointer had not been adjusted even though the start
        // pointer had been moved during consolidate.
        final double[] values = {
                0.0834, 0.01187, 0.10279, 0.09835, 0.09883,
                0.1001, 0.1129, 0.09599, 0.09468, 0.09063,
                0.09083, 0.08194, 0.10182, 0.09323, 0.08796,
                0.09972, 0.09429, 0.08069, 0.09008, 0.10346,
                0.10594, 0.09416, 0.06915, 0.08638, 0.0886,
                0.09538, 0.08546, 0.09271, 0.0936, 0.09941,
                0.08026, 0.0952, 0.09446, 0.09309, 0.09855,
                0.08682, 0.09464, 0.0857, 0.09154, 0.08024,
                0.08824, 0.09442, 0.09495, 0.08731, 0.08428,
                0.08959, 0.07994, 0.08034, 0.09095, 0.09659,
                0.10066, 0.0821, 0.09606, 0.12346, 0.07866,
                0.07723, 0.08642, 0.08076, 0.07455, 0.07961,
                0.07364, 0.08911, 0.06946, 0.07509, 0.087,
                0.071, 0.08653, 0.07899, 0.09512, 0.09456,
                0.09161, 0.08412, 0.09649, 0.09994, 0.10151,
                0.09751, 0.1019, 0.10499, 0.0873, 0.1085,
                0.10189, 0.09987, 0.08912, 0.10606, 0.09552,
                0.08902, 0.09158, 0.08046, 0.10687, 0.0906,
                0.09937, 0.09737, 0.09825, 0.10234, 0.09926,
                0.09147, 0.09071, 0.09659, 0.09472, 0.09327,
                0.0949, 0.09316, 0.09393, 0.09328, 0.01187,
                0.00848, 0.02284, 0.03053, 0.08393, 0.08167,
                0.10191, 0.06527, 0.06613, 0.06863, 0.0652,
                0.06848, 0.06681, 0.07466, 0.06444, 0.05991,
                0.07031, 0.06612, 0.06873, 0.06598, 0.07283,
                0.06862, 0.06437, 0.06599, 0.07291, 0.06355,
                0.0685, 0.06599, 0.06593, 0.0869, 0.07364,
                0.08118, 0.07693, 0.06779, 0.06605, 0.07286,
                0.05655, 0.06352, 0.06105, 0.09177, 0.08312,
                0.0978, 0.07464, 0.07977, 0.06241, 0.07227,
                0.06255, 0.0675, 0.07953, 0.07806, 0.06702,
                0.08429, 0.08567, 0.0933, 0.087, 0.08809,
                0.07888, 0.06351, 0.08651, 0.08294, 0.07282,
                0.11102, 0.08711, 0.06192, 0.0652, 0.06957,
                0.06763, 0.07123, 0.0687, 0.06773, 0.06338,
                0.06694, 0.09871, 0.09221, 0.08962, 0.0879,
                0.09625, 0.09953, 0.09532, 0.09903, 0.0946,
                0.09406, 0.09704, 0.09877, 0.07257, 0.1001,
                0.09458, 0.10141, 0.10581, 0.09824, 0.10668,
                0.09835, 0.10816, 0.09667, 0.08962, 0.08486,
                0.08572, 0.08324, 0.08826, 0.08801, 0.09744,
                0.09916, 0.09996, 0.10054, 0.10761, 0.105,
                0.10604, 0.10161, 0.09155, 0.10162, 0.08549,
                0.10342, 0.09419, 0.11429, 0.09764, 0.09505,
                0.09394, 0.10411, 0.08792, 0.08887, 0.08648,
                0.07637, 0.08544, 0.08034, 0.12373, 0.12963,
                0.13817, 0.13904, 0.12648, 0.13207, 0.10788,
                0.09605, 0.12674, 0.08139, 0.08326, 0.08835,
                0.10922, 0.103, 0.12225, 0.09854, 0.09326,
                0.11181, 0.089, 0.12674, 0.11631, 0.0879,
                0.09866, 0.11393, 0.09839, 0.09738, 0.09922,
                0.1145, 0.09967, 0.1032, 0.11624, 0.10472,
                0.09999, 0.09762, 0.1075, 0.11558, 0.10482,
                0.10237, 0.10776, 0.08781, 0.08771, 0.09751,
                0.09025, 0.09201, 0.08731, 0.08537, 0.0887,
                0.0844, 0.0804, 0.08217, 0.10216, 0.07789,
                0.08693, 0.0833, 0.08542, 0.09729, 0.0937,
                0.09886, 0.092, 0.08392, 0.09668, 0.09444,
                0.09401, 0.08657, 0.09659, 0.08553, 0.0834,
                0.0846, 0.10167, 0.10447, 0.09838, 0.09545,
                0.09163, 0.10475, 0.09761, 0.09475, 0.09769,
                0.09873, 0.09033, 0.09202, 0.08637, 0.0914,
                0.09146, 0.09437, 0.08454, 0.09009, 0.08888,
                0.0811, 0.12672, 0.10517, 0.11959, 0.10941,
                0.10319, 0.10544, 0.10717, 0.11218, 0.12347,
                0.10637, 0.11558, 0.1198, 0.10133, 0.09795,
                0.10818, 0.11657, 0.10836, 0.11127, 0.09611,
                0.08462, 0.1056, 0.09537, 0.09815, 0.10385,
                0.10246, 0.11299, 0.11926, 0.104, 0.10309,
                0.09494, 0.10078, 0.09966, 0.08215, 0.09136,
                0.10058, 0.10078, 0.10121, 0.09711, 0.10072,
                0.10881, 0.09396, 0.09925, 0.09221, 0.0939,
                0.08804, 0.09234, 0.09647, 0.07966, 0.09939,
                0.09651, 0.10765, 0.10154, 0.07889, 0.10452,
                0.1023, 0.10275, 0.08817, 0.0923, 0.09237,
                0.09481, 0.09309, 0.08683, 0.09903, 0.08784,
                0.09309, 0.08876, 0.08442, 0.097, 0.10054,
                0.09463, 0.10038, 0.08208, 0.10209, 0.10181,
                0.10416, 0.08065, 0.09581, 0.08961, 0.08553,
                0.10272, 0.08432, 0.08437, 0.08946, 0.07594,
                0.07751, 0.07935, 0.07751, 0.07714, 0.09572,
                0.09626, 0.08606, 0.08031, 0.08196, 0.09758,
                0.0754, 0.08671, 0.10245, 0.07644, 0.07965,
                0.09553, 0.08362, 0.07587, 0.08234, 0.08611,
                0.09835, 0.09917, 0.09264, 0.09656, 0.0992,
                0.10802, 0.10905, 0.09726, 0.09911, 0.11056,
                0.08599, 0.09095, 0.10547, 0.08824, 0.09831,
                0.08445, 0.09562, 0.09378, 0.08482, 0.08686,
                0.09192, 0.09617, 0.09142, 0.1024, 0.10415,
                0.10673, 0.08337, 0.10091, 0.08162, 0.08284,
                0.08472, 0.1021, 0.09073, 0.10521, 0.09252,
                0.08545, 0.09849, 0.0891, 0.10849, 0.08897,
                0.08306, 0.10775, 0.10054, 0.09952, 0.10851,
                0.10823, 0.10827, 0.11254, 0.11344, 0.10478,
                0.11348, 0.10646, 0.12112, 0.10183, 0.1197,
                0.12399, 0.11847, 0.11572, 0.14614, 0.13348,
                0.12449, 0.12358, 0.12792, 0.12525, 0.12265,
                0.1305, 0.13037, 0.12684, 0.12374, 0.12907,
                0.12858, 0.1285, 0.12857, 0.15825, 0.15937,
                0.1467, 0.128305, 0.118165, 0.119619995, 0.117565,
                0.12769, 0.11013
        };
        final FibonacciHeap<Double, Double> heap = FibonacciHeap.create();
        for (double d : values) {
            heap.add(d, d);
        }
        Arrays.sort(values);
        int i = 0;
        while (!heap.isEmpty()) {
            assertEquals(values[i], heap.poll(), 0.0001);
            i++;
        }
    }
}