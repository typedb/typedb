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

import grakn.core.common.iterator.Iterators;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Arguments;

import java.util.List;

import static grakn.common.collection.Collections.list;

public class Producers {

    public static final int DEFAULT_BATCH_SIZE = 32;
    public static final int MAX_BATCH_SIZE = (Integer.MAX_VALUE / 2) - 1;

    public static <T> BaseProducer<T> empty() { return producer(Iterators.empty()); }

    public static <T> Producer<T> async(ResourceIterator<ResourceIterator<T>> iterators, int parallelisation) {
        return new AsyncProducer<>(iterators, parallelisation);
    }

    public static <T> BaseProducer<T> producer(ResourceIterator<T> iterator) {
        return new BaseProducer<>(iterator);
    }

    public static <T> ProducerIterator<T> produce(Producer<T> producer, Arguments.Query.Producer mode) {
        return produce(list(producer), mode);
    }

    public static <T> ProducerIterator<T> produce(Producer<T> producer, int batchSize) {
        return produce(list(producer), batchSize);
    }

    public static <T> ProducerIterator<T> produce(List<Producer<T>> producers, Arguments.Query.Producer mode) {
        return new ProducerIterator<>(producers, mode.isIncremental() ? DEFAULT_BATCH_SIZE : MAX_BATCH_SIZE);
    }

    public static <T> ProducerIterator<T> produce(List<Producer<T>> producers, int batchSize) {
        return new ProducerIterator<>(producers, batchSize);
    }
}
