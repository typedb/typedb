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

package com.vaticle.typedb.core.common.iterator.sorted;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.AbstractFunctionalIterator;
import com.vaticle.typedb.core.common.iterator.ConsumeHandledIterator;
import com.vaticle.typedb.core.common.iterator.DistinctIterator;
import com.vaticle.typedb.core.common.iterator.ErrorHandledIterator;
import com.vaticle.typedb.core.common.iterator.FlatMappedIterator;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.iterator.LimitedIterator;
import com.vaticle.typedb.core.common.iterator.LinkedIterators;
import com.vaticle.typedb.core.common.iterator.MappedIterator;
import com.vaticle.typedb.core.common.iterator.OffsetIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;

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

public abstract class AbstractSortedIterator<T extends Comparable<? super T>, ORDER extends Order>
        implements SortedIterator<T, ORDER> {

    final ORDER order;

    protected AbstractSortedIterator(ORDER order) {
        this.order = order;
    }

    @Override
    public ORDER order() {
        return order;
    }

    @Override
    public final SortedIterator<T, ORDER> merge(SortedIterator<T, ORDER> iterator) {
        return new MergeMappedIterator<>(order, iterate(this, iterator), e -> e);
    }

    @Override
    public <U extends Comparable<U>, ORD extends Order> Seekable<U, ORD> mergeMap(ORD order, Function<T, Seekable<U, ORD>> mappingFn) {
        return new MergeMappedIterator.Seekable<>(order, this, mappingFn);
    }

    @Override
    public <U> FunctionalIterator<U> map(Function<T, U> mappingFn) {
        return new MappedIterator<>(this, mappingFn);
    }

    @Override
    public <U extends Comparable<? super U>, ORD extends Order> SortedIterator<U, ORD> mapSorted(ORD order,
                                                                                                 Function<T, U> mappingFn) {
        return new MappedSortedIterator<>(order, this, mappingFn);
    }

    @Override
    public <U> FunctionalIterator<U> flatMap(Function<T, FunctionalIterator<U>> mappingFn) {
        return new FlatMappedIterator<>(this, mappingFn);
    }

    @Override
    public SortedIterator<T, ORDER> distinct() {
        return new DistinctSortedIterator<>(this);
    }

    @Override
    public FunctionalIterator<T> distinct(Set<T> duplicates) {
        return new DistinctIterator<>(this, duplicates);
    }

    @Override
    public SortedIterator<T, ORDER> filter(Predicate<T> predicate) {
        return Iterators.Sorted.filter(this, predicate);
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
    public SortedIterator<T, ORDER> onConsumed(Runnable function) {
        return new ConsumeHandledSortedIterator<>(this, function);
    }

    @Override
    public SortedIterator<T, ORDER> onFinalised(Runnable function) {
        return new FinaliseSortedIterator<>(this, function);
    }

    @Override
    public FunctionalIterator<T> onError(Function<Exception, TypeDBException> exceptionFn) {
        return new ErrorHandledIterator<>(this, exceptionFn);
    }
}
