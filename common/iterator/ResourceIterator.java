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

import grakn.common.collection.Either;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static grakn.common.collection.Collections.list;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;

public interface ResourceIterator<T> extends ComposableIterator<T>, RecyclableIterator<T> {

    @Override
    default ResourceIterator<T> distinct() {
        return new DistinctIterator<>(Either.first(this));
    }

    @Override
    default <U> ResourceIterator<U> map(final Function<T, U> function) {
        return new MappedIterator<>(Either.first(this), function);
    }

    @Override
    default ResourceIterator<T> filter(final Predicate<T> predicate) {
        return new FilteredIterator<>(Either.first(this), predicate);
    }

    @Override
    default ResourceIterator<T> link(final RecyclableIterator<T> iterator) {
        return new LinkedIterators<>(new LinkedList<>(list(Either.first(this), Either.first(iterator))));
    }

    @Override
    default ResourceIterator<T> link(final Iterator<T> iterator) {
        if (iterator instanceof RecyclableIterator<?>) return link((RecyclableIterator<T>) iterator);
        return new LinkedIterators<>(new LinkedList<>(list(Either.first(this), Either.second(iterator))));
    }

    @Override
    default Stream<T> stream() {
        return StreamSupport.stream(
                spliteratorUnknownSize(this, ORDERED | IMMUTABLE), false
        ).onClose(this::recycle);
    }
}
