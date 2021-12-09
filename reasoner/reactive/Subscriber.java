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

package com.vaticle.typedb.core.reasoner.reactive;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;

import javax.annotation.Nullable;
import java.util.function.Function;

public interface Subscriber<T> {

    Publisher<T> subscribe(Publisher<T> publisher);

    void receive(Publisher<T> publisher, T packet);  // TODO: The publisher argument is only needed by compound - can we do without it?

    // TODO: I think these methods should sit on publisher
    Reactive<T, T> findFirstSubscribe();

    <R> Reactive<R, T> mapSubscribe(Function<R, T> function);

    <R> Reactive<R, T> flatMapOrRetrySubscribe(Function<R, FunctionalIterator<T>> function);
}
