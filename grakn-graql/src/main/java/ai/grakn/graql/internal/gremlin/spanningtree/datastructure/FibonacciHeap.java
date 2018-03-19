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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.gremlin.spanningtree.datastructure;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.singletonIterator;
import static com.google.common.collect.Iterators.transform;

/**
 * A Fibonacci heap (due to Fredman and Tarjan).
 *
 * @param <V> the type of the values stored in the heap
 * @param <P> the type of the priorities
 * @author Jason Liu
 */
public class FibonacciHeap<V, P> implements Iterable<FibonacciHeap<V, P>.Entry> {
    public final static int MAX_CAPACITY = Integer.MAX_VALUE;
    // the largest degree of any root list entry is guaranteed to be <= log_phi(MAX_CAPACITY) = 45
    private final static int MAX_DEGREE = 45;

    private Optional<Entry> oMinEntry = Optional.empty();
    private int size = 0;
    private final Comparator<? super P> comparator;

    class Entry {
        public final V value;
        private P priority;
        private Optional<Entry> oParent = Optional.empty();
        private Optional<Entry> oFirstChild = Optional.empty();
        private Entry previous;
        private Entry next;
        private int degree = 0;
        // Whether this entry has had a child cut since it was added to its parent.
        // An entry can only have one child cut before it has to be cut itself.
        private boolean isMarked = false;

        private Entry(V value, P priority) {
            this.value = value;
            this.priority = priority;
            previous = next = this;
        }
    }

    private FibonacciHeap(Comparator<? super P> comparator) {
        // we'll use nulls to force a node to the top when we delete it
        this.comparator = Ordering.from(comparator).nullsFirst();
    }

    public static <T, C> FibonacciHeap<T, C> create(Comparator<? super C> comparator) {
        return new FibonacciHeap<>(comparator);
    }

    /**
     * Create a new FibonacciHeap based on the natural ordering on `C`
     */
    public static <T, C extends Comparable> FibonacciHeap<T, C> create() {
        return FibonacciHeap.create(Ordering.<C>natural());
    }

    /**
     * Returns the comparator used to order the elements in this
     * queue.
     */
    public Comparator<? super P> comparator() {
        return comparator;
    }

    /**
     * Removes all elements from the heap.
     * Runtime: O(1)
     */
    public void clear() {
        oMinEntry = Optional.empty();
        size = 0;
    }

    /**
     * Decreases the priority for an entry.
     * Runtime: O(1) (amortized)
     */
    public void decreasePriority(Entry entry, P newPriority) {
        Preconditions.checkArgument(comparator.compare(newPriority, entry.priority) <= 0,
                "Cannot increase priority");
        entry.priority = newPriority;
        assert oMinEntry.isPresent();
        final Entry minEntry = oMinEntry.get();
        Optional<Entry> oParent = entry.oParent;
        if (oParent.isPresent() && (comparator.compare(newPriority, oParent.get().priority) < 0)) {
            cutAndMakeRoot(entry);
        }
        if (comparator.compare(newPriority, minEntry.priority) < 0) {
            oMinEntry = Optional.of(entry);
        }
    }

    /**
     * Deletes `entry` from the heap. The heap will be consolidated, if necessary.
     * Runtime: O(log n) amortized
     */
    public void remove(Entry entry) {
        entry.priority = null;
        cutAndMakeRoot(entry);
        oMinEntry = Optional.of(entry);
        pollOption();
    }

    /**
     * Returns true if the heap is empty, false otherwise.
     * Runtime: O(1)
     */
    public boolean isEmpty() {
        return !oMinEntry.isPresent();
    }

    /**
     * Inserts a new entry into the heap and returns the entry if heap is not full. Returns absent otherwise.
     * No heap consolidation is performed.
     * Runtime: O(1)
     */
    public Optional<Entry> add(V value, P priority) {
        Preconditions.checkNotNull(value);
        Preconditions.checkNotNull(priority);
        if (size >= MAX_CAPACITY) return Optional.empty();
        final Entry result = new Entry(value, priority);
        // add as a root
        oMinEntry = mergeLists(Optional.of(result), oMinEntry);
        size++;
        return Optional.of(result);
    }

    /**
     * Returns the entry with the minimum priority, or absent if empty.
     * Runtime: O(1)
     */
    public Optional<Entry> peekOption() {
        return oMinEntry;
    }

    /**
     * Removes the smallest element from the heap. This will cause
     * the trees in the heap to be consolidated, if necessary.
     * Runtime: O(log n) amortized
     */
    public Optional<V> pollOption() {
        if (!oMinEntry.isPresent()) return Optional.empty();
        final Entry minEntry = oMinEntry.get();
        // move minEntry's children to the root list
        Optional<Entry> oFirstChild = minEntry.oFirstChild;
        if (oFirstChild.isPresent()) {
            for (Entry childOfMin : getCycle(oFirstChild.get())) {
                childOfMin.oParent = Optional.empty();
            }
            mergeLists(oMinEntry, oFirstChild);
        }
        // remove minEntry from root list
        if (size == 1) {
            oMinEntry = Optional.empty();
        } else {
            final Entry next = minEntry.next;
            unlinkFromNeighbors(minEntry);
            oMinEntry = Optional.of(consolidate(next));
        }
        size--;
        return Optional.of(minEntry.value);
    }

    /**
     * Returns the number of elements in the heap.
     * Runtime: O(1)
     */
    public int size() {
        return size;
    }

    /**
     * Joins two Fibonacci heaps into a new one. No heap consolidation is
     * performed; the two root lists are just spliced together.
     * Runtime: O(1)
     */
    public static <U, P> FibonacciHeap<U, P> merge(FibonacciHeap<U, P> a, FibonacciHeap<U, P> b) {
        Preconditions.checkArgument(a.comparator().equals(b.comparator()),
                "Heaps that use different comparators can't be merged.");
        final FibonacciHeap<U, P> result = FibonacciHeap.create(a.comparator);
        result.oMinEntry = a.mergeLists(a.oMinEntry, b.oMinEntry);
        result.size = a.size + b.size;
        return result;
    }

    /**
     * Returns every entry in this heap, in no particular order.
     */
    @Override
    public Iterator<Entry> iterator() {
        return siblingsAndBelow(oMinEntry);
    }

    /**
     * Depth-first iteration
     */
    private Iterator<Entry> siblingsAndBelow(Optional<Entry> oEntry) {
        return oEntry.map(entry1 -> concat(transform(getCycle(entry1).iterator(),
                entry -> {
                    assert entry != null;
                    return concat(singletonIterator(entry), siblingsAndBelow(entry.oFirstChild));
                }))).orElse(Collections.emptyIterator());
    }

    private List<Entry> getCycle(Entry start) {
        final List<Entry> results = new ArrayList<>();
        Entry current = start;
        do {
            results.add(current);
            current = current.next;
        } while (!current.equals(start));
        return results;
    }

    /**
     * Merge two doubly-linked circular lists, given a pointer into each.
     * Return the smaller of the two arguments.
     */
    private Optional<Entry> mergeLists(Optional<Entry> oA, Optional<Entry> oB) {
        if (!oA.isPresent()) return oB;
        if (!oB.isPresent()) return oA;
        final Entry a = oA.get();
        final Entry b = oB.get();
        // splice the two circular lists together like a Mobius strip
        final Entry aOldNext = a.next;
        a.next = b.next;
        a.next.previous = a;
        b.next = aOldNext;
        b.next.previous = b;
        return comparator.compare(a.priority, b.priority) < 0 ? oA : oB;
    }

    /**
     * Cuts this entry from its parent and adds it to the root list, and then
     * does the same for its parent, and so on up the tree.
     * Runtime: O(log n)
     */
    private void cutAndMakeRoot(Entry entry) {
        Optional<Entry> oParent = entry.oParent;
        if (!oParent.isPresent()) return;  // already a root
        final Entry parent = oParent.get();
        parent.degree--;
        entry.isMarked = false;
        // update parent's `oFirstChild` pointer
        Optional<Entry> oFirstChild = parent.oFirstChild;
        assert oFirstChild.isPresent();
        if (oFirstChild.get().equals(entry)) {
            if (parent.degree == 0) {
                parent.oFirstChild = Optional.empty();
            } else {
                parent.oFirstChild = Optional.of(entry.next);
            }
        }
        entry.oParent = Optional.empty();
        unlinkFromNeighbors(entry);
        // add to root list
        mergeLists(Optional.of(entry), oMinEntry);
        if (parent.oParent.isPresent()) {
            if (parent.isMarked) {
                cutAndMakeRoot(parent);
            } else {
                parent.isMarked = true;
            }
        }
    }

    /**
     * Attaches `entry` as a child of `parent`. Returns `parent`.
     */
    private Entry setParent(Entry entry, Entry parent) {
        unlinkFromNeighbors(entry);
        entry.oParent = Optional.of(parent);
        parent.oFirstChild = mergeLists(Optional.of(entry), parent.oFirstChild);
        parent.degree++;
        entry.isMarked = false;
        return parent;
    }

    private static void unlinkFromNeighbors(FibonacciHeap.Entry entry) {
        entry.previous.next = entry.next;
        entry.next.previous = entry.previous;
        entry.previous = entry;
        entry.next = entry;
    }

    /**
     * Consolidates the trees in the heap by joining trees of equal
     * degree until there are no more trees of equal degree in the
     * root list. Returns the new minimum root.
     * Runtime: O(log n) (amortized)
     */
    private Entry consolidate(Entry someRoot) {
        Entry minRoot = someRoot;
        // `rootsByDegree[d]` will hold the best root we've looked at so far with degree `d`.
        final Object[] rootsByDegree = new Object[MAX_DEGREE];
        for (Entry currRoot : getCycle(someRoot)) {
            // Put `currRoot` into `rootsByDegree`. If there's already something in its spot,
            // merge them into a new tree of degree `degree + 1`. Keep merging until we find an
            // empty spot.
            Entry mergedRoot = currRoot;
            for (int degree = currRoot.degree; rootsByDegree[degree] != null; degree++) {
                @SuppressWarnings("unchecked")
                Entry oldRoot = (Entry) rootsByDegree[degree];
                // move the worse root beneath the better root
                if (comparator.compare(mergedRoot.priority, oldRoot.priority) < 0) {
                    mergedRoot = setParent(oldRoot, mergedRoot);
                } else {
                    mergedRoot = setParent(mergedRoot, oldRoot);
                }
                rootsByDegree[degree] = null;
            }
            rootsByDegree[mergedRoot.degree] = mergedRoot;
            if (comparator.compare(mergedRoot.priority, minRoot.priority) <= 0) {
                minRoot = mergedRoot;
            }
        }
        return minRoot;
    }
}
