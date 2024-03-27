/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.concurrent.producer;

import java.util.function.Function;
import java.util.function.Predicate;

public interface FunctionalProducer<T> extends Producer<T> {

    <U> FunctionalProducer<U> map(Function<T, U> mappingFn);

    FunctionalProducer<T> filter(Predicate<T> predicate);

    FunctionalProducer<T> distinct();
}
