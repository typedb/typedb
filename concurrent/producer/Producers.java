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

package grakn.core.concurrent.producer;

import grakn.common.collection.Either;
import grakn.core.common.iterator.Iterators;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Arguments;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static grakn.common.collection.Collections.list;

public class Producers {

    public static final long LIMIT_DEFAULT = Long.MAX_VALUE;
    public static final int BATCH_SIZE_DEFAULT = 32;
    public static final int BATCH_SIZE_MAX = (Integer.MAX_VALUE / 2) - 1;

    public static <T> FunctionalProducer<T> empty() { return async(Iterators.empty()); }

    public static <T> FunctionalProducer<T> async(ResourceIterator<ResourceIterator<T>> iterators, int parallelisation) {
        return new AsyncProducer<>(iterators, parallelisation);
    }

    public static <T> FunctionalProducer<T> async(ResourceIterator<T> iterator) {
        return new BaseProducer<>(iterator);
    }

    public static <T> ProducerIterator<T> produce(Producer<T> producer, Either<Arguments.Query.Producer, Long> context, ExecutorService executor) {
        return produce(list(producer), context, executor);
    }

    public static <T> ProducerIterator<T> produce(List<Producer<T>> producers, Either<Arguments.Query.Producer, Long> context, ExecutorService executor) {
        int batchSize = context.isSecond() || context.first().isIncremental() ? BATCH_SIZE_DEFAULT : BATCH_SIZE_MAX;
        long limit = context.isSecond() ? context.second() : LIMIT_DEFAULT;
        return new ProducerIterator<>(producers, batchSize, limit, executor);
    }
}
