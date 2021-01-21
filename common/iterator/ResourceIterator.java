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

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public interface ResourceIterator<T> extends Iterator<T> {

    ResourceIterator<T> distinct();

    ResourceIterator<T> distinct(Set<T> duplicates);

    <U> ResourceIterator<U> map(Function<T, U> mappingFn);

    <U> ResourceIterator<U> flatMap(Function<T, ResourceIterator<U>> flatMappingFn);

    ResourceIterator<T> filter(Predicate<T> predicate);

    ResourceIterator<T> offset(long offset);

    ResourceIterator<T> limit(long limit);

    ResourceIterator<T> link(ResourceIterator<T> iterator);

    ResourceIterator<T> link(Iterator<T> iterator);

    ResourceIterator<T> noNulls();

    boolean allMatch(Predicate<T> predicate);

    boolean anyMatch(Predicate<T> predicate);

    boolean noneMatch(Predicate<T> predicate);

    Optional<T> first();

    T firstOrNull();

    Stream<T> stream();

    List<T> toList();

    void toList(List<T> list);

    Set<T> toSet();

    LinkedHashSet<T> toLinkedSet();

    long count();

    ResourceIterator<T> onError(Function<Exception, GraknException> exceptionFn);

    void recycle();
}
