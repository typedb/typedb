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

package grakn.core.common.producer;

import grakn.core.common.iterator.ResourceIterator;

import java.util.List;

import static grakn.common.collection.Collections.list;

public class Producers {

    public static <T> BaseProducer<T> produce(ResourceIterator<T> iterator) {
        return new BaseProducer<>(iterator);
    }

    public static <T> ProducerQueue<T> queue(Producer<T> producer) {
        return new ProducerQueue<>(list(producer));
    }

    public static <T> ProducerQueue<T> queue(Producer<T> producer, int bufferMinSize, int bufferMaxSize) {
        return new ProducerQueue<>(list(producer), bufferMinSize, bufferMaxSize);
    }

    public static <T> ProducerQueue<T> queue(List<Producer<T>> producers) {
        return new ProducerQueue<>(producers);
    }

    public static <T> ProducerQueue<T> queue(List<Producer<T>> producers, int bufferMinSize, int bufferMaxSize) {
        return new ProducerQueue<>(producers, bufferMinSize, bufferMaxSize);
    }
}
