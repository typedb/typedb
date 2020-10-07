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

import static grakn.common.collection.Collections.list;

public interface ResourceIterator<T> extends Iterators.Composable<T>, Iterators.Recyclable<T> {

    @Override
    default DistinctIterator<T> distinct() {
        return new DistinctIterator<>(Either.first(this));
    }

    @Override
    default <U> MappedIterator<T, U> map(final Function<T, U> function) {
        return new MappedIterator<>(Either.first(this), function);
    }

    @Override
    default FilteredIterator<T> filter(final Predicate<T> predicate) {
        return new FilteredIterator<>(Either.first(this), predicate);
    }

    @Override
    default LinkedIterators<T> link(final Iterators.Recyclable<T> iterator) {
        return new LinkedIterators<>(new LinkedList<>(list(Either.first(this), Either.first(iterator))));
    }

    @Override
    default LinkedIterators<T> link(final Iterator<T> iterator) {
        if (iterator instanceof Iterators.Recyclable<?>) return link((Iterators.Recyclable<T>) iterator);
        return new LinkedIterators<>(new LinkedList<>(list(Either.first(this), Either.second(iterator))));
    }
}
