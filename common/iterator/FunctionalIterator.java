/*
 * Copyright (C) 2021 Vaticle
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
 *
 */

package com.vaticle.typedb.core.common.iterator;

import com.vaticle.typedb.core.common.exception.TypeDBException;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public interface FunctionalIterator<T> extends Iterator<T> {

    FunctionalIterator<T> distinct();

    FunctionalIterator<T> distinct(Set<T> duplicates);

    <U> FunctionalIterator<U> map(Function<T, U> mappingFn);

    <U> FunctionalIterator<U> flatMap(Function<T, FunctionalIterator<U>> flatMappingFn);

    <K extends Comparable<K>> Sorted<T, K> flatMerge(Function<T, Sorted<T, K>> flatMappingFn);

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

    LinkedHashSet<T> toLinkedSet();

    long count();

    FunctionalIterator<T> onConsumed(Runnable function);

    FunctionalIterator<T> onError(Function<Exception, TypeDBException> exceptionFn);

    FunctionalIterator<T> onFinalise(Runnable function);

    void recycle();

    interface Sorted<T, K extends Comparable<K>> extends FunctionalIterator<T> {

        Function<T, K> keyExtractor();

        void seek(T target); // TODO maybe this should return a boolean

        T peek();

//        Sorted<T, K> merge(Sorted<T, K>... iterator);

        Sorted<T, K> distinct();

        Sorted<T, K> filter(Predicate<T> predicate);

//        Sorted<T, K> offset(long offset);
//
//        Sorted<T, K> onConsumed(Runnable function);
//
//        Sorted<T, K> onError(Function<Exception, TypeDBException> exceptionFn);
//
//        Sorted<T, K> onFinalise(Runnable function);

    }
}
