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

import com.google.protobuf.ByteString;
import grakn.core.Grakn;
import grakn.core.server.rpc.util.ResponseBuilder;
import grakn.protocol.DatabaseProto.Database;
import grakn.protocol.GraknGrpc;
import grakn.protocol.TransactionProto.Transaction;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.protobuf.ByteString.copyFrom;
import static grakn.core.common.collection.Bytes.bytesToUUID;
import static grakn.core.common.collection.Bytes.uuidToBytes;
import static grakn.core.common.exception.Error.DatabaseManager.DATABASE_DELETED;
import static grakn.core.common.exception.Error.DatabaseManager.DATABASE_EXISTS;
import static grakn.core.common.exception.Error.DatabaseManager.DATABASE_NOT_FOUND;
import static grakn.core.common.exception.Error.Server.SERVER_SHUTDOWN;
import static grakn.core.common.exception.Error.Session.SESSION_NOT_FOUND;
import static grakn.protocol.SessionProto.Session;
import static java.util.stream.Collectors.toList;


/**
 * Grakn RPC Service
 */
public class GraknRPC extends GraknGrpc.GraknImplBase implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(GraknRPC.class);

    private final Grakn grakn;
    private final ConcurrentMap<UUID, SessionRPC> sessions;

    public GraknRPC(Grakn grakn) {
        this.grakn = grakn;
        sessions = new ConcurrentHashMap<>();
    }

    @Override
    public void databaseContains(Database.Contains.Req request, StreamObserver<Database.Contains.Res> response) {
        try {
            boolean contains = grakn.databases().contains(request.getName());
            response.onNext(Database.Contains.Res.newBuilder().setContains(contains).build());
            response.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            response.onError(ResponseBuilder.exception(e));
        }
    }

    @Override
    public void databaseCreate(Database.Create.Req request, StreamObserver<Database.Create.Res> response) {
        try {
            if (grakn.databases().contains(request.getName())) {
                throw Status.ALREADY_EXISTS.withDescription(DATABASE_EXISTS.message(request.getName())).asRuntimeException();
            }
            grakn.databases().create(request.getName());
            response.onNext(Database.Create.Res.getDefaultInstance());
            response.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            response.onError(ResponseBuilder.exception(e));
        }
    }

    @Override
    public void databaseAll(Database.All.Req request, StreamObserver<Database.All.Res> response) {
        try {
            Iterable<String> list = grakn.databases().all().stream().map(Grakn.Database::name).collect(toList());
            response.onNext(Database.All.Res.newBuilder().addAllNames(list).build());
            response.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            response.onError(ResponseBuilder.exception(e));
        }
    }

    @Override
    public void databaseDelete(Database.Delete.Req request, StreamObserver<Database.Delete.Res> response) {
        try {
            if (!grakn.databases().contains(request.getName())) {
                String message = DATABASE_NOT_FOUND.message(request.getName());
                throw Status.NOT_FOUND.withDescription(message).asRuntimeException();
            }

            Grakn.Database database = grakn.databases().get(request.getName());
            String message = DATABASE_DELETED.message(request.getName());
            StatusRuntimeException exception = Status.ABORTED.withDescription(message).asRuntimeException();
            database.sessions().parallel().forEach(session -> sessions.get(session.uuid()).close(exception));
            database.delete();
            response.onNext(Database.Delete.Res.getDefaultInstance());
            response.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            response.onError(ResponseBuilder.exception(e));
        }
    }

    @Override
    public void sessionOpen(Session.Open.Req request, StreamObserver<Session.Open.Res> responseObserver) {
        try {
            Grakn.Session.Type sessionType = Grakn.Session.Type.of(request.getType().getNumber());
            SessionRPC sessionRPC = new SessionRPC(grakn, request.getDatabase(), sessionType);
            sessions.put(sessionRPC.session().uuid(), sessionRPC);
            ByteString uuid = copyFrom(uuidToBytes(sessionRPC.session().uuid()));
            responseObserver.onNext(Session.Open.Res.newBuilder().setSessionID(uuid).build());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            responseObserver.onError(ResponseBuilder.exception(e));
        }
    }

    @Override
    public void sessionClose(Session.Close.Req request, StreamObserver<Session.Close.Res> responseObserver) {
        try {
            UUID sessionID = bytesToUUID(request.getSessionID().toByteArray());
            if (sessions.containsKey(sessionID)) {
                sessions.remove(sessionID).close(null);
            } else {
                throw Status.NOT_FOUND.withDescription(SESSION_NOT_FOUND.message(sessionID)).asRuntimeException();
            }

            responseObserver.onNext(Session.Close.Res.newBuilder().build());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            responseObserver.onError(ResponseBuilder.exception(e));
        }
    }

    @Override
    public StreamObserver<Transaction.Req> transaction(StreamObserver<Transaction.Res> responseSender) {
        return new TransactionRPC(sessionID -> sessions.getOrDefault(sessionID, null), responseSender);
    }

    @Override
    public void close() {
        String message = SERVER_SHUTDOWN.message();
        StatusRuntimeException exception = Status.ABORTED.withDescription(message).asRuntimeException();
        sessions.values().parallelStream().forEach(session -> session.close(exception));
        sessions.clear();
    }
}
