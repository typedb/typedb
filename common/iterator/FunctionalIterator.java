/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.iterator;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator;
import com.vaticle.typedb.core.common.parameters.Order;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

public interface FunctionalIterator<T> extends Iterator<T> {

    FunctionalIterator<T> distinct();

    FunctionalIterator<T> distinct(Set<T> duplicates);

    <U> FunctionalIterator<U> map(Function<T, U> mappingFn);

    <U extends Comparable<? super U>, ORDER extends Order> SortedIterator<U, ORDER> mapSorted(Function<T, U> mappingFn, ORDER order);

    <U> FunctionalIterator<U> flatMap(Function<T, FunctionalIterator<U>> mappingFn);

    <U extends Comparable<? super U>, ORDER extends Order> SortedIterator<U, ORDER> mergeMap(
            Function<T, SortedIterator<U, ORDER>> mappingFn, ORDER order
    );

    <U extends Comparable<? super U>, ORDER extends Order> SortedIterator.Forwardable<U, ORDER> mergeMapForwardable(
            Function<T, SortedIterator.Forwardable<U, ORDER>> mappingFn, ORDER order
    );

    FunctionalIterator<T> filter(Predicate<T> predicate);

    FunctionalIterator<T> offset(long offset);

    FunctionalIterator<T> limit(long limit);

    FunctionalIterator<T> link(FunctionalIterator<T> iterator);

    FunctionalIterator<T> link(Iterator<T> iterator);

    FunctionalIterator<T> noNulls();

    boolean allMatch(Predicate<T> predicate);

    boolean anyMatch(Predicate<T> predicate);

    boolean noneMatch(Predicate<T> predicate);

    Optional<T> first();

    T firstOrNull();

    Stream<T> stream();

    List<T> toList();

    List<List<T>> toLists(int split);

    List<List<T>> toLists(int minSize, int maxSplit);

    void toList(List<T> list);

    Set<T> toSet();

    void toSet(Set<T> set);

    <U extends Collection<? super T>> U collect(Supplier<U> constructor);

    long count();

    <ACC> ACC reduce(ACC initial, BiFunction<T, ACC, ACC> accumulate);

    FunctionalIterator<T> onConsumed(Runnable function);

    FunctionalIterator<T> onError(Function<Exception, TypeDBException> exceptionFn);

    FunctionalIterator<T> onFinalise(Runnable function);

    void recycle();

    @Override
    default void forEachRemaining(Consumer<? super T> action) {
        Objects.requireNonNull(action);
        while (hasNext()) {
            action.accept(next());
        }
        recycle();
    }
}
