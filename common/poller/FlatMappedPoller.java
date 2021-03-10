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

package com.vaticle.typedb.core.common.poller;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;

import java.util.Optional;
import java.util.function.Function;

public class FlatMappedPoller<T, U> extends AbstractPoller<U> {

    private final Poller<T> source;
    private final Function<T, FunctionalIterator<U>> flatMappingFn;
    private FunctionalIterator<U> currentIterator;

    public FlatMappedPoller(AbstractPoller<T> poller, Function<T, FunctionalIterator<U>> flatMappingFn) {
        this.source = poller;
        this.flatMappingFn = flatMappingFn;
        currentIterator = Iterators.empty();
    }

    @Override
    public Optional<U> poll() {
        mayFetchIterator();
        if (currentIterator.hasNext()) return Optional.of(currentIterator.next());
        else return Optional.empty();
    }

    private void mayFetchIterator() {
        Optional<T> nextSource;
        while (!currentIterator.hasNext() && (nextSource = source.poll()).isPresent()) {
            currentIterator = flatMappingFn.apply(nextSource.get());
        }
    }

}
