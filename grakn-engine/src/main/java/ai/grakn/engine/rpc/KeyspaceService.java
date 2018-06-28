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

package ai.grakn.engine.rpc;

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.rpc.proto.KeyspaceGrpc;
import ai.grakn.rpc.proto.KeyspaceProto;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Grakn RPC Keyspace Service
 */
public class KeyspaceService extends KeyspaceGrpc.KeyspaceImplBase {

    private final OpenRequest requestOpener;

    public KeyspaceService(OpenRequest requestOpener) {
        this.requestOpener = requestOpener;
    }

    @Override
    public void create(KeyspaceProto.Create.Req request, StreamObserver<KeyspaceProto.Create.Res> response) {
        response.onError(new StatusRuntimeException(Status.UNIMPLEMENTED));
    }

    @Override
    public void retrieve(KeyspaceProto.Retrieve.Req request, StreamObserver<KeyspaceProto.Retrieve.Res> response) {
        response.onError(new StatusRuntimeException(Status.UNIMPLEMENTED));
    }

    @Override
    public void delete(KeyspaceProto.Delete.Req request, StreamObserver<KeyspaceProto.Delete.Res> response) {
        try {
            ServerOpenRequest.Arguments args = new ServerOpenRequest.Arguments(Keyspace.of(request.getName()), GraknTxType.WRITE);
            try (GraknTx tx = requestOpener.open(args)) {
                tx.admin().delete();

                response.onNext(ResponseBuilder.delete());
                response.onCompleted();
            }
        } catch (RuntimeException e) {
            response.onError(ResponseBuilder.exception(e));
        }
    }
}
