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

package com.vaticle.typedb.core.concurrent.producer;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.parameters.Arguments;

import java.util.List;
import java.util.concurrent.Executor;

import static com.vaticle.typedb.common.collection.Collections.list;

public class Producers {

    public static final long LIMIT_DEFAULT = Long.MAX_VALUE;
    public static final int BATCH_SIZE_DEFAULT = 32;
    public static final int BATCH_SIZE_MAX = (Integer.MAX_VALUE / 2) - 1;

    public static <T> FunctionalProducer<T> empty() { return async(Iterators.empty()); }

    public static <T> FunctionalProducer<T> async(FunctionalIterator<FunctionalIterator<T>> iterators, int parallelisation) {
        return new AsyncProducer<>(iterators, parallelisation);
    }

    public static <T> FunctionalProducer<T> async(FunctionalIterator<T> iterator) {
        return new BaseProducer<>(iterator);
    }

    public static <T> ProducerIterator<T> produce(Producer<T> producer, Either<Arguments.Query.Producer, Long> context, Executor executor) {
        return produce(list(producer), context, executor);
    }

    public static <T> ProducerIterator<T> produce(List<Producer<T>> producers, Either<Arguments.Query.Producer, Long> context, Executor executor) {
        int batchSize = context.isSecond() || context.first().isIncremental() ? BATCH_SIZE_DEFAULT : BATCH_SIZE_MAX;
        long limit = context.isSecond() ? context.second() : LIMIT_DEFAULT;
        return new ProducerIterator<>(producers, batchSize, limit, executor);
    }
}
