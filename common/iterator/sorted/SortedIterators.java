/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.iterator.sorted;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.parameters.Order;

import java.util.Collection;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.parameters.Order.Asc.ASC;

public class SortedIterators {

    public static <T extends Comparable<? super T>, ORDER extends Order> SortedIterator<T, ORDER> iterateSorted(
            ORDER order, List<T> list
    ) {
        return new BaseSortedIterator<>(list, order);
    }

    public static <T extends Comparable<? super T>, ORDER extends Order> SortedIterator<T, ORDER> distinct(
            SortedIterator<T, ORDER> iterator
    ) {
        return new DistinctSortedIterator<>(iterator);
    }

    public static <T extends Comparable<? super T>, ORDER extends Order> SortedIterator<T, ORDER> filter(
            SortedIterator<T, ORDER> iterator, Predicate<T> predicate
    ) {
        return new FilteredSortedIterator<>(iterator, predicate);
    }

    public static <T extends Comparable<? super T>, ORDER extends Order> SortedIterator<T, ORDER> limit(
            SortedIterator<T, ORDER> iterator, long limit
    ) {
        return new LimitedSortedIterator<>(iterator, limit);
    }

    public static <T extends Comparable<? super T>, U extends Comparable<? super U>, ORDER extends Order>
    SortedIterator<U, ORDER> mapSorted(ORDER order, SortedIterator<T, ?> iterator, Function<T, U> mappingFn) {
        return new MappedSortedIterator<>(iterator, mappingFn, order);
    }

    @SafeVarargs
    public static <T extends Comparable<? super T>, ORDER extends Order> SortedIterator<T, ORDER> merge(
            SortedIterator<T, ORDER> iterator, SortedIterator<T, ORDER>... iterators
    ) {
        return new MergeMappedSortedIterator<>(Iterators.iterate(list(list(iterators), iterator)), e -> e, iterator.order());
    }

    public static <T extends Comparable<? super T>, ORDER extends Order> SortedIterator<T, ORDER> merge(
            ORDER order, FunctionalIterator<SortedIterator<T, ORDER>> iterators
    ) {
        return new MergeMappedSortedIterator<>(iterators, e -> e, order);
    }

    public static <T extends Comparable<? super T>, ORDER extends Order> SortedIterator<T, ORDER> onConsume(
            SortedIterator<T, ORDER> iterator, Runnable onConsume
    ) {
        return new ConsumeHandledSortedIterator<>(iterator, onConsume);
    }

    public static <T extends Comparable<? super T>, ORDER extends Order> SortedIterator<T, ORDER> onFinalise(
            SortedIterator<T, ORDER> iterator, Runnable finalise
    ) {
        return new FinaliseSortedIterator<>(iterator, finalise);
    }

    public static class Forwardable {

        public static <T extends Comparable<? super T>> SortedIterator.Forwardable<T, Order.Asc> emptySorted() {
            return emptySorted(ASC);
        }

        public static <T extends Comparable<? super T>, ORDER extends Order> SortedIterator.Forwardable<T, ORDER> emptySorted(
                ORDER order
        ) {
            return iterateSorted(new TreeSet<T>(), order);
        }

        public static <T extends Comparable<? super T>, ORDER extends Order> SortedIterator.Forwardable<T, ORDER> iterateSorted(
                Collection<T> elements, ORDER order
        ) {
            return new BaseForwardableIterator<>(new TreeSet<>(elements), order);
        }

        @SafeVarargs
        public static <T extends Comparable<? super T>, ORDER extends Order> SortedIterator.Forwardable<T, ORDER> iterateSorted(
                ORDER order, T... elements
        ){
            return new BaseForwardableIterator<>(new TreeSet<>(list(elements)), order);
        }

        public static <T extends Comparable<? super T>, ORDER extends Order> SortedIterator.Forwardable<T, ORDER> iterateSorted(
                NavigableSet<T> set, ORDER order
        ) {
            return new BaseForwardableIterator<>(set, order);
        }

        public static <T extends Comparable<? super T>, ORDER extends Order> SortedIterator.Forwardable<T, ORDER> distinct(
                SortedIterator.Forwardable<T, ORDER> iterator
        ) {
            return new DistinctSortedIterator.Forwardable<>(iterator);
        }

        public static <T extends Comparable<? super T>, ORDER extends Order> SortedIterator.Forwardable<T, ORDER> filter(
                SortedIterator.Forwardable<T, ORDER> iterator, Predicate<T> predicate
        ) {
            return new FilteredSortedIterator.Forwardable<>(iterator, predicate);
        }

        public static <T extends Comparable<? super T>, ORDER extends Order> SortedIterator.Forwardable<T, ORDER> limit(
                SortedIterator.Forwardable<T, ORDER> iterator, long limit
        ) {
            return new LimitedSortedIterator.Forwardable<>(iterator, limit);
        }

        public static <T extends Comparable<? super T>, ORDER extends Order> SortedIterator.Forwardable<T, ORDER> takeWhile(
                SortedIterator.Forwardable<T, ORDER> iterator, Function<T, Boolean> condition
        ) {
            return new WhileSortedIterator.Forwardable<>(iterator, condition);
        }

        @SafeVarargs
        public static <T extends Comparable<? super T>, ORDER extends Order> SortedIterator.Forwardable<T, ORDER> merge(
                SortedIterator.Forwardable<T, ORDER> iterator, SortedIterator.Forwardable<T, ORDER>... iterators
        ) {
            return new MergeMappedSortedIterator.Forwardable<>(Iterators.iterate(list(list(iterators), iterator)), e -> e, iterator.order());
        }

        public static <T extends Comparable<? super T>, ORDER extends Order> SortedIterator.Forwardable<T, ORDER> merge(
                FunctionalIterator<SortedIterator.Forwardable<T, ORDER>> iterators, ORDER order
        ){
            return new MergeMappedSortedIterator.Forwardable<>(iterators, e -> e, order);
        }

        @SafeVarargs
        public static <T extends Comparable<? super T>, ORDER extends Order> SortedIterator.Forwardable<T, ORDER> intersect(
                SortedIterator.Forwardable<T, ORDER> iterator, SortedIterator.Forwardable<T, ORDER>... iterators
        ){
            return new IntersectForwardableIterator<>(list(list(iterators), iterator), iterator.order());
        }

        public static <T extends Comparable<? super T>, ORDER extends Order> SortedIterator.Forwardable<T, ORDER> intersect(
                FunctionalIterator<SortedIterator.Forwardable<T, ORDER>> iterators, ORDER order
        ){
            return new IntersectForwardableIterator<>(iterators.toList(), order);
        }

        public static <T extends Comparable<? super T>, U extends Comparable<? super U>, ORDER extends Order>
        SortedIterator.Forwardable<U, ORDER> mapSorted(
                ORDER order, SortedIterator.Forwardable<T, ?> iterator, Function<T, U> mappingFn, Function<U, T> reverseMappingFn
        ){
            return new MappedSortedIterator.Forwardable<>(iterator, mappingFn, reverseMappingFn, order);
        }

        public static <T extends Comparable<? super T>, ORDER extends Order> SortedIterator.Forwardable<T, ORDER> onConsume(
                SortedIterator.Forwardable<T, ORDER> iterator, Runnable onConsume
        ) {
            return new ConsumeHandledSortedIterator.Forwardable<>(iterator, onConsume);
        }

        public static <T extends Comparable<? super T>, ORDER extends Order> SortedIterator.Forwardable<T, ORDER> onFinalise(
                SortedIterator.Forwardable<T, ORDER> iterator, Runnable finalise
        ) {
            return new FinaliseSortedIterator.Forwardable<>(iterator, finalise);
        }
    }
}
