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
 *
 */

package grakn.core.common.iterator;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static grakn.common.collection.Collections.list;
import static grakn.core.common.iterator.Iterators.iterate;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;

public interface ResourceIterator<T> extends Iterator<T> {

    default ResourceIterator<T> distinct() {
        return new DistinctIterator<>(this);
    }

    default ResourceIterator<T> distinct(Set<T> duplicates) {
        return new DistinctIterator<>(this, duplicates);
    }

    default <U> ResourceIterator<U> map(Function<T, U> mappingFn) {
        return new MappedIterator<>(this, mappingFn);
    }

    default <U> ResourceIterator<U> flatMap(Function<T, ResourceIterator<U>> flatMappingFn) {
        return new FlatMappedIterator<>(this, flatMappingFn);
    }

    default ResourceIterator<T> filter(Predicate<T> predicate) {
        return new FilteredIterator<>(this, predicate);
    }

    default ResourceIterator<T> limit(int limit) {
        return new LimitedIterator<>(this, limit);
    }

    default ResourceIterator<T> link(ResourceIterator<T> iterator) {
        return new LinkedIterators<>(list(this, iterator));
    }

    default ResourceIterator<T> link(Iterator<T> iterator) {
        if (iterator instanceof ResourceIterator<?>) return link((ResourceIterator<T>) iterator);
        return new LinkedIterators<>(list(this, iterate(iterator)));
    }

    default ResourceIterator<T> noNulls() {
        return this.filter(Objects::nonNull);
    }

    default boolean anyMatch(Predicate<T> predicate) {
        return this.filter(predicate).hasNext();
    }

    default T firstOrNull() {
        if (hasNext()) return next();
        else return null;
    }

    default Stream<T> stream() {
        return StreamSupport.stream(
                spliteratorUnknownSize(this, ORDERED | IMMUTABLE), false
        ).onClose(this::recycle);
    }

    default List<T> toList() {
        final LinkedList<T> list = new LinkedList<>();
        this.forEachRemaining(list::addLast);
        return list;
    }

    default Set<T> toSet() {
        final Set<T> set = new HashSet<>();
        this.forEachRemaining(set::add);
        return set;
    }

    default int count() {
        return this.toList().size();
    }

    void recycle();
}
