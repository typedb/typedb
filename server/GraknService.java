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

package grakn.core.server;

import grakn.core.Grakn;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Options;
import grakn.protocol.CoreDatabaseProto.CoreDatabase;
import grakn.protocol.CoreDatabaseProto.CoreDatabaseManager;
import grakn.protocol.GraknCoreGrpc;
import grakn.protocol.SessionProto;
import grakn.protocol.TransactionProto;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
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
import static grakn.core.server.common.RequestReader.applyDefaultOptions;
import static grakn.core.server.common.ResponseBuilder.Database.deleteRes;
import static grakn.core.server.common.ResponseBuilder.Database.schemaRes;
import static grakn.core.server.common.ResponseBuilder.DatabaseManager.allRes;
import static grakn.core.server.common.ResponseBuilder.DatabaseManager.containsRes;
import static grakn.core.server.common.ResponseBuilder.DatabaseManager.createRes;
import static grakn.core.server.common.ResponseBuilder.Session.closeRes;
import static grakn.core.server.common.ResponseBuilder.Session.openRes;
import static grakn.core.server.common.ResponseBuilder.Session.pulseRes;
import static grakn.core.server.common.ResponseBuilder.exception;
import static java.util.stream.Collectors.toList;

public class GraknService extends GraknCoreGrpc.GraknCoreImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(GraknService.class);

    private final Grakn grakn;
    private final ConcurrentMap<UUID, SessionService> sessionServices;

    public GraknService(Grakn grakn) {
        this.grakn = grakn;
        sessionServices = new ConcurrentHashMap<>();
    }

    @Override
    public void databasesContains(CoreDatabaseManager.Contains.Req request,
                                  StreamObserver<CoreDatabaseManager.Contains.Res> responder) {
        try {
            boolean contains = grakn.databases().contains(request.getName());
            responder.onNext(containsRes(contains));
            responder.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void databasesCreate(CoreDatabaseManager.Create.Req request,
                                StreamObserver<CoreDatabaseManager.Create.Res> responder) {
        try {
            if (grakn.databases().contains(request.getName())) {
                throw GraknException.of(DATABASE_EXISTS, request.getName());
            }
            grakn.databases().create(request.getName());
            responder.onNext(createRes());
            responder.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void databasesAll(CoreDatabaseManager.All.Req request,
                             StreamObserver<CoreDatabaseManager.All.Res> responder) {
        try {
            List<String> databaseNames = grakn.databases().all().stream().map(Grakn.Database::name).collect(toList());
            responder.onNext(allRes(databaseNames));
            responder.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void databaseSchema(CoreDatabase.Schema.Req request, StreamObserver<CoreDatabase.Schema.Res> responder) {
        try {
            String schema = grakn.databases().get(request.getName()).schema();
            responder.onNext(schemaRes(schema));
            responder.onCompleted();
        } catch (GraknException e) {
            LOG.error(e.getMessage(), e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void databaseDelete(CoreDatabase.Delete.Req request, StreamObserver<CoreDatabase.Delete.Res> responder) {
        try {
            String databaseName = request.getName();
            if (!grakn.databases().contains(databaseName)) {
                throw GraknException.of(DATABASE_NOT_FOUND, databaseName);
            }
            Grakn.Database database = grakn.databases().get(databaseName);
            database.sessions().parallel().forEach(session -> {
                UUID sessionId = session.uuid();
                SessionService sessionSvc = sessionServices.get(sessionId);
                if (sessionSvc != null) {
                    sessionSvc.close(GraknException.of(DATABASE_DELETED, databaseName));
                    sessionServices.remove(sessionId);
                }
            });
            database.delete();
            responder.onNext(deleteRes());
            responder.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void sessionOpen(SessionProto.Session.Open.Req request,
                            StreamObserver<SessionProto.Session.Open.Res> responder) {
        try {
            Instant start = Instant.now();
            Arguments.Session.Type sessionType = Arguments.Session.Type.of(request.getType().getNumber());
            Options.Session options = applyDefaultOptions(new Options.Session(), request.getOptions());
            Grakn.Session session = grakn.session(request.getDatabase(), sessionType, options);
            SessionService sessionSvc = new SessionService(this, session, options);
            sessionServices.put(sessionSvc.session().uuid(), sessionSvc);
            int duration = (int) Duration.between(start, Instant.now()).toMillis();
            responder.onNext(openRes(sessionSvc, duration));
            responder.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void sessionClose(SessionProto.Session.Close.Req request,
                             StreamObserver<SessionProto.Session.Close.Res> responder) {
        try {
            UUID sessionID = bytesToUUID(request.getSessionId().toByteArray());
            SessionService sessionSvc = sessionServices.get(sessionID);
            if (sessionSvc == null) throw GraknException.of(SESSION_NOT_FOUND, sessionID);
            sessionSvc.close();
            responder.onNext(closeRes());
            responder.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void sessionPulse(SessionProto.Session.Pulse.Req request,
                             StreamObserver<SessionProto.Session.Pulse.Res> responder) {
        try {
            UUID sessionID = bytesToUUID(request.getSessionId().toByteArray());
            SessionService sessionSvc = sessionServices.get(sessionID);
            boolean isAlive = sessionSvc != null && sessionSvc.isOpen();
            if (isAlive) sessionSvc.keepAlive();
            responder.onNext(pulseRes(isAlive));
            responder.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            responder.onError(exception(e));
        }
    }

    @Override
    public StreamObserver<TransactionProto.Transaction.Client> transaction(
            StreamObserver<TransactionProto.Transaction.Server> responder) {
        return new TransactionService(this, responder);
    }

    public SessionService session(UUID uuid) {
        return sessionServices.get(uuid);
    }

    public void remove(SessionService sessionSvc) {
        sessionServices.remove(sessionSvc.UUID());
    }

    public void close() {
        sessionServices.values().parallelStream().forEach(s -> s.close(GraknException.of(SERVER_SHUTDOWN)));
        sessionServices.clear();
    }
}
