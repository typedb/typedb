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
 */

package grakn.core.server.rpc;

import grakn.core.Grakn;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Options;
import grakn.protocol.DatabaseProto;
import grakn.protocol.GraknGrpc;
import grakn.protocol.SessionProto;
import grakn.protocol.TransactionProto;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static grakn.core.common.collection.Bytes.bytesToUUID;
import static grakn.core.common.exception.ErrorMessage.Database.DATABASE_DELETED;
import static grakn.core.common.exception.ErrorMessage.Database.DATABASE_EXISTS;
import static grakn.core.common.exception.ErrorMessage.Database.DATABASE_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.Server.SERVER_SHUTDOWN;
import static grakn.core.common.exception.ErrorMessage.Session.SESSION_NOT_FOUND;
import static grakn.core.server.rpc.common.RequestReader.setDefaultOptions;
import static grakn.core.server.rpc.common.ResponseBuilder.exception;
import static java.util.stream.Collectors.toList;

public class GraknRPCService extends GraknGrpc.GraknImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(GraknRPCService.class);

    private final Grakn grakn;
    private final ConcurrentMap<UUID, SessionRPC> rpcSessions;

    public GraknRPCService(Grakn grakn) {
        this.grakn = grakn;
        rpcSessions = new ConcurrentHashMap<>();
    }

    @Override
    public void databaseContains(DatabaseProto.Database.Contains.Req request, StreamObserver<DatabaseProto.Database.Contains.Res> responder) {
        try {
            boolean contains = grakn.databases().contains(request.getName());
            responder.onNext(DatabaseProto.Database.Contains.Res.newBuilder().setContains(contains).build());
            responder.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void databaseCreate(DatabaseProto.Database.Create.Req request, StreamObserver<DatabaseProto.Database.Create.Res> responder) {
        try {
            if (grakn.databases().contains(request.getName())) {
                throw GraknException.of(DATABASE_EXISTS, request.getName());
            }
            grakn.databases().create(request.getName());
            responder.onNext(DatabaseProto.Database.Create.Res.getDefaultInstance());
            responder.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void databaseAll(DatabaseProto.Database.All.Req request, StreamObserver<DatabaseProto.Database.All.Res> responder) {
        try {
            List<String> databaseNames = grakn.databases().all().stream().map(Grakn.Database::name).collect(toList());
            responder.onNext(DatabaseProto.Database.All.Res.newBuilder().addAllNames(databaseNames).build());
            responder.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void databaseDelete(DatabaseProto.Database.Delete.Req request, StreamObserver<DatabaseProto.Database.Delete.Res> responder) {
        try {
            String databaseName = request.getName();
            if (!grakn.databases().contains(databaseName)) {
                throw GraknException.of(DATABASE_NOT_FOUND, databaseName);
            }
            Grakn.Database database = grakn.databases().get(databaseName);
            database.sessions().parallel().forEach(session -> {
                UUID sessionId = session.uuid();
                SessionRPC sessionRPC = rpcSessions.get(sessionId);
                if (sessionRPC != null) {
                    sessionRPC.closeWithError(GraknException.of(DATABASE_DELETED, databaseName));
                    rpcSessions.remove(sessionId);
                }
            });
            database.delete();
            responder.onNext(DatabaseProto.Database.Delete.Res.getDefaultInstance());
            responder.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void sessionOpen(SessionProto.Session.Open.Req request, StreamObserver<SessionProto.Session.Open.Res> responder) {
        try {
            Arguments.Session.Type sessionType = Arguments.Session.Type.of(request.getType().getNumber());
            Options.Session options = setDefaultOptions(new Options.Session(), request.getOptions());
            Grakn.Session session = grakn.session(request.getDatabase(), sessionType, options);
            SessionRPC sessionRPC = new SessionRPC(this, session, options);
            rpcSessions.put(sessionRPC.session().uuid(), sessionRPC);
            responder.onNext(SessionProto.Session.Open.Res.newBuilder().setSessionId(sessionRPC.uuidAsByteString()).build());
            responder.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void sessionClose(SessionProto.Session.Close.Req request, StreamObserver<SessionProto.Session.Close.Res> responder) {
        try {
            UUID sessionID = bytesToUUID(request.getSessionId().toByteArray());
            SessionRPC sessionRPC = rpcSessions.get(sessionID);
            if (sessionRPC == null) throw GraknException.of(SESSION_NOT_FOUND, sessionID);
            sessionRPC.close();
            responder.onNext(SessionProto.Session.Close.Res.newBuilder().build());
            responder.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void sessionPulse(SessionProto.Session.Pulse.Req request, StreamObserver<SessionProto.Session.Pulse.Res> responder) {
        try {
            UUID sessionID = bytesToUUID(request.getSessionId().toByteArray());
            SessionRPC sessionRPC = rpcSessions.get(sessionID);
            boolean isAlive = sessionRPC != null && sessionRPC.isOpen();
            if (isAlive) sessionRPC.keepAlive();
            responder.onNext(SessionProto.Session.Pulse.Res.newBuilder().setAlive(isAlive).build());
            responder.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            responder.onError(exception(e));
        }
    }

    @Override
    public StreamObserver<TransactionProto.Transaction.Req> transaction(StreamObserver<TransactionProto.Transaction.Res> responder) {
        return new TransactionStream(this, responder);
    }

    public void close() {
        rpcSessions.values().parallelStream().forEach(sessionRPC -> sessionRPC.closeWithError(GraknException.of(SERVER_SHUTDOWN)));
        rpcSessions.clear();
    }

    SessionRPC getSession(UUID id) {
        return rpcSessions.get(id);
    }

    void removeSession(UUID id) {
        rpcSessions.remove(id);
    }
}
