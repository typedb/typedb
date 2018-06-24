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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.remote.rpc;

import ai.grakn.remote.Grakn;
import ai.grakn.rpc.generated.GrpcGrakn;
import ai.grakn.rpc.generated.GrpcGrakn.Done;
import ai.grakn.rpc.generated.GrpcIterator;
import ai.grakn.util.CommonUtil;
import com.google.common.collect.AbstractIterator;

import java.util.function.Function;

import static ai.grakn.rpc.generated.GrpcIterator.Next;

/**
 * A client-side iterator over gRPC messages. Will send {@link Next} messages until it receives a {@link Done} message.
 *
 * @param <T> class type of objects being iterated
 */
public class RequestIterator<T> extends AbstractIterator<T> {
    private final GrpcIterator.IteratorId iteratorId;
    private Grakn.Transaction tx;
    private Function<GrpcGrakn.TxResponse, T> responseReader;

    public RequestIterator(Grakn.Transaction tx, GrpcIterator.IteratorId iteratorId, Function<GrpcGrakn.TxResponse, T> responseReader) {
        this.tx = tx;
        this.iteratorId = iteratorId;
        this.responseReader = responseReader;
    }

    @Override
    protected final T computeNext() {
        GrpcGrakn.TxResponse response = tx.next(iteratorId);

        switch (response.getResponseCase()) {
            case DONE:
                return endOfData();
            case RESPONSE_NOT_SET:
                throw CommonUtil.unreachableStatement("Unexpected " + response);
            default:
                return responseReader.apply(response);
        }
    }
}
