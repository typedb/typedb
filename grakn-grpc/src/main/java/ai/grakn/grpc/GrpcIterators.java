/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.grpc;

import ai.grakn.rpc.generated.GrpcGrakn.Done;
import ai.grakn.rpc.generated.GrpcGrakn.TxResponse;
import ai.grakn.rpc.generated.GrpcIterator.IteratorId;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Contains a mutable map of iterators of {@link TxResponse}s for gRPC. These iterators are used for returning
 * lazy, streaming responses such as for Graql query results.
 *
 * @author Felix Chapman
 */
public class GrpcIterators {
    private final AtomicInteger iteratorIdCounter = new AtomicInteger();
    private final Map<IteratorId, Iterator<TxResponse>> iterators = new ConcurrentHashMap<>();

    private GrpcIterators() {
    }

    public static GrpcIterators create() {
        return new GrpcIterators();
    }

    /**
     * Register a new iterator and return the ID of the iterator
     */
    public IteratorId add(Iterator<TxResponse> iterator) {
        IteratorId iteratorId = IteratorId.newBuilder().setId(iteratorIdCounter.getAndIncrement()).build();

        iterators.put(iteratorId, iterator);
        return iteratorId;
    }

    /**
     * Return the next response from an iterator. Will return a {@link Done} response if the iterator is exhausted.
     */
    public Optional<TxResponse> next(IteratorId iteratorId) {
        return Optional.ofNullable(iterators.get(iteratorId)).map(iterator -> {
            TxResponse response;

            if (iterator.hasNext()) {
                response = iterator.next();
            } else {
                response = GrpcUtil.doneResponse();
                stop(iteratorId);
            }

            return response;
        });
    }

    /**
     * Stop an iterator
     */
    public void stop(IteratorId iteratorId) {
        iterators.remove(iteratorId);
    }
}