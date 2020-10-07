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

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static grakn.common.collection.Collections.list;

public interface ComposableIterator<T> extends Iterator<T> {

    default DistinctIterator<T> distinct() {
        return new DistinctIterator<>(Either.second(this));
    }

    default <U> MappedIterator<T, U> map(final Function<T, U> function) {
        return new MappedIterator<>(Either.second(this), function);
    }

    default FilteredIterator<T> filter(final Predicate<T> predicate) {
        return new FilteredIterator<>(Either.second(this), predicate);
    }

    default LinkedIterators<T> link(final RecyclableIterator<T> iterator) {
        return new LinkedIterators<>(new LinkedList<>(list(Either.second(this), Either.first(iterator))));
    }

    default LinkedIterators<T> link(final Iterator<T> iterator) {
        if (iterator instanceof RecyclableIterator<?>) return link((RecyclableIterator<T>) iterator);
        return new LinkedIterators<>(new LinkedList<>(list(Either.second(this), Either.second(iterator))));
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
}
