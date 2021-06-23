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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;

public abstract class AbstractFunctionalIterator<T> implements FunctionalIterator<T> {

    @Override
    public FunctionalIterator<T> distinct() {
        return new DistinctIterator<>(this);
    }

    @Override
    public FunctionalIterator<T> distinct(Set<T> duplicates) {
        return new DistinctIterator<>(this, duplicates);
    }

    @Override
    public <U> FunctionalIterator<U> map(Function<T, U> mappingFn) {
        return new MappedIterator<>(this, mappingFn);
    }

    @Override
    public <U> FunctionalIterator<U> flatMap(Function<T, FunctionalIterator<U>> flatMappingFn) {
        return new FlatMappedIterator<>(this, flatMappingFn);
    }

    @Override
    public <U extends Comparable<U>> FunctionalIterator.Sorted<U> flatMerge(Function<T, FunctionalIterator.Sorted<U>> flatMappingFn) {
        return new FlatMergeSortedIterator<>(this, flatMappingFn);
    }

    @Override
    public FunctionalIterator<T> filter(Predicate<T> predicate) {
        return new FilteredIterator<>(this, predicate);
    }

    @Override
    public FunctionalIterator<T> offset(long offset) {
        return new OffsetIterator<>(this, offset);
    }

    @Override
    public FunctionalIterator<T> limit(long limit) {
        return new LimitedIterator<>(this, limit);
    }

    @Override
    public FunctionalIterator<T> link(FunctionalIterator<T> iterator) {
        return new LinkedIterators<>(list(this, iterator));
    }

    @Override
    public FunctionalIterator<T> link(Iterator<T> iterator) {
        if (iterator instanceof AbstractFunctionalIterator<?>) return link((FunctionalIterator<T>) iterator);
        return new LinkedIterators<>(list(this, iterate(iterator)));
    }

    @Override
    public FunctionalIterator<T> noNulls() {
        return this.filter(Objects::nonNull);
    }

    @Override
    public boolean allMatch(Predicate<T> predicate) {
        boolean match = !filter(e -> !predicate.test(e)).hasNext();
        recycle();
        return match;
    }

    @Override
    public boolean anyMatch(Predicate<T> predicate) {
        boolean match = filter(predicate).hasNext();
        recycle();
        return match;
    }

    @Override
    public boolean noneMatch(Predicate<T> predicate) {
        return !anyMatch(predicate);
    }

    @Override
    public Optional<T> first() {
        return Optional.ofNullable(firstOrNull());
    }

    @Override
    public T firstOrNull() {
        T next = hasNext() ? next() : null;
        recycle();
        return next;
    }

    @Override
    public Stream<T> stream() {
        return StreamSupport.stream(
                spliteratorUnknownSize(this, ORDERED | IMMUTABLE), false
        ).onClose(this::recycle);
    }

    @Override
    public List<T> toList() {
        ArrayList<T> list = new ArrayList<>();
        forEachRemaining(list::add);
        recycle();
        return list;
    }

    @Override
    public List<List<T>> toLists(int split) {
        List<List<T>> lists = new ArrayList<>(split);
        for (int i = 0; i < split; i++) lists.add(new ArrayList<>());
        int i = 0;
        while (hasNext()) {
            lists.get(i).add(next());
            i++;
            if (i == split) i = 0;
        }
        return lists;
    }

    @Override
    public List<List<T>> toLists(int minSize, int maxSplit) {
        assert minSize > 0 && maxSplit > 0;
        List<List<T>> lists = new ArrayList<>(maxSplit);

        if (!hasNext()) {
            lists.add(new ArrayList<>());
            return lists;
        }

        for (int i = 0; i < maxSplit && hasNext(); i++) {
            List<T> list = new ArrayList<>();
            for (int j = 0; j < minSize && hasNext(); j++) list.add(next());
            lists.add(list);
        }
        int i = 0;
        while (hasNext()) {
            lists.get(i).add(next());
            i++;
            if (i == maxSplit) i = 0;
        }
        return lists;
    }

    @Override
    public void toList(List<T> list) {
        forEachRemaining(list::add);
        recycle();
    }

    @Override
    public Set<T> toSet() {
        HashSet<T> set = new HashSet<>();
        this.forEachRemaining(set::add);
        return set;
    }

    @Override
    public void toSet(Set<T> set) {
        this.forEachRemaining(set::add);
        recycle();
    }

    @Override
    public LinkedHashSet<T> toLinkedSet() {
        LinkedHashSet<T> linkedSet = new LinkedHashSet<>();
        forEachRemaining(linkedSet::add);
        recycle();
        return linkedSet;
    }

    @Override
    public long count() {
        long count = 0;
        for (; hasNext(); count++) next();
        recycle();
        return count;
    }

    @Override
    public FunctionalIterator<T> onConsumed(Runnable function) {
        return new ConsumeHandledIterator<>(this, function);
    }

    @Override
    public FunctionalIterator<T> onError(Function<Exception, TypeDBException> exceptionFn) {
        return new ErrorHandledIterator<>(this, exceptionFn);
    }

    @Override
    public FunctionalIterator<T> onFinalise(Runnable function) {
        return new FinaliseHandledIterator<>(this, function);
    }

    @Override
    public abstract void recycle();

    public static abstract class Sorted<T extends Comparable<? super T>> extends AbstractFunctionalIterator<T> implements FunctionalIterator.Sorted<T> {

        @SafeVarargs
        @Override
        public final FunctionalIterator.Sorted<T> merge(FunctionalIterator.Sorted<T>... iterators) {
            List<FunctionalIterator.Sorted<T>> iters = list(list(iterators), this);
            return new FlatMergeSortedIterator<>(iterate(iters), e -> e);
        }

        @Override
        public <U extends Comparable<? super U>> FunctionalIterator.Sorted<U> mapSorted(Function<T, U> mappingFn, Function<U, T> reverseMappingFn) {
            return new MappedIterator.Sorted<>(this, mappingFn, reverseMappingFn);
        }

        @Override
        public FunctionalIterator.Sorted<T> distinct() {
            return new DistinctIterator.Sorted<>(this);
        }

        @Override
        public FunctionalIterator.Sorted<T> filter(Predicate<T> predicate) {
            return new FilteredIterator.Sorted<>(this, predicate);
        }


//        @Override
//        public FunctionalIterator.Sorted<T, K> offset(long offset) {
//
//        }
//
//        @Override
//        public FunctionalIterator.Sorted<T, K> onConsumed(Runnable function) {
//
//        }
//
//        @Override
//        public FunctionalIterator.Sorted<T, K> onError(Function<Exception, TypeDBException> exceptionFn) {
//
//        }
//
        @Override
        public FunctionalIterator.Sorted<T> onFinalise(Runnable function) {
            return new FinaliseHandledIterator.Sorted<>(this, function);
        }
    }
}
