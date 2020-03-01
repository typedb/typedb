/*
 * Copyright (C) 2020 Grakn Labs
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
 */

package grakn.core.server.rpc;

import grakn.protocol.keyspace.KeyspaceProto;
import grakn.protocol.keyspace.KeyspaceServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Grakn RPC Keyspace Service
 */
public class KeyspaceService extends KeyspaceServiceGrpc.KeyspaceServiceImplBase {
    private final Logger LOG = LoggerFactory.getLogger(KeyspaceService.class);

    private final KeyspaceRequestsHandler requestsHandler;

    public KeyspaceService(KeyspaceRequestsHandler handler) {
        this.requestsHandler = handler;
    }

    @Override
    public void retrieve(KeyspaceProto.Keyspace.Retrieve.Req request, StreamObserver<KeyspaceProto.Keyspace.Retrieve.Res> response) {
        try {
            Iterable<String> list = this.requestsHandler.retrieve(request);
            response.onNext(KeyspaceProto.Keyspace.Retrieve.Res.newBuilder().addAllNames(list).build());
            response.onCompleted();
        } catch (RuntimeException e) {
            LOG.error("Exception during keyspaces retrieval.", e);
            response.onError(ResponseBuilder.exception(e));
        }
    }

    @Override
    public void delete(KeyspaceProto.Keyspace.Delete.Req request, StreamObserver<KeyspaceProto.Keyspace.Delete.Res> response) {
        try {
            this.requestsHandler.delete(request);
            response.onNext(KeyspaceProto.Keyspace.Delete.Res.getDefaultInstance());
            response.onCompleted();
        } catch (RuntimeException e) {
            LOG.error("Exception during keyspace deletion.", e);
            response.onError(ResponseBuilder.exception(e));
        }
    }
}
