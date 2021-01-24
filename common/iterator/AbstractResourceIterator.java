/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.common.iterator;

import grakn.core.common.exception.GraknException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static grakn.common.collection.Collections.list;
import static grakn.core.common.iterator.Iterators.iterate;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;

public abstract class AbstractResourceIterator<T> implements ResourceIterator<T> {

    @Override
    public ResourceIterator<T> distinct() {
        return new DistinctIterator<>(this);
    }

    @Override
    public ResourceIterator<T> distinct(Set<T> duplicates) {
        return new DistinctIterator<>(this, duplicates);
    }

    @Override
    public <U> ResourceIterator<U> map(Function<T, U> mappingFn) {
        return new MappedIterator<>(this, mappingFn);
    }

    @Override
    public <U> ResourceIterator<U> flatMap(Function<T, ResourceIterator<U>> flatMappingFn) {
        return new FlatMappedIterator<>(this, flatMappingFn);
    }

    @Override
    public ResourceIterator<T> filter(Predicate<T> predicate) {
        return new FilteredIterator<>(this, predicate);
    }

    @Override
    public ResourceIterator<T> offset(long offset) {
        return new OffsetIterator<>(this, offset);
    }

    @Override
    public ResourceIterator<T> limit(long limit) {
        return new LimitedIterator<>(this, limit);
    }

    @Override
    public ResourceIterator<T> link(ResourceIterator<T> iterator) {
        return new LinkedIterators<>(list(this, iterator));
    }

    @Override
    public ResourceIterator<T> link(Iterator<T> iterator) {
        if (iterator instanceof AbstractResourceIterator<?>) return link((ResourceIterator<T>) iterator);
        return new LinkedIterators<>(list(this, iterate(iterator)));
    }

    @Override
    public ResourceIterator<T> noNulls() {
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
    public ResourceIterator<T> onConsumed(Runnable function) {
        return new ConsumeHandledIterator<>(this, function);
    }

    @Override
    public ResourceIterator<T> onError(Function<Exception, GraknException> exceptionFn) {
        return new ErrorHandledIterator<>(this, exceptionFn);
    }

    @Override
    protected void finalize() {
        recycle();
    }

    @Override
    public abstract void recycle();
}
