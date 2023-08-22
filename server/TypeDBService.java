/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.server;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concurrent.executor.Executors;
import com.vaticle.typedb.core.server.common.ResponseBuilder;
import com.vaticle.typedb.protocol.ConnectionProto;
import com.vaticle.typedb.protocol.CoreDatabaseProto.CoreDatabase;
import com.vaticle.typedb.protocol.CoreDatabaseProto.CoreDatabaseManager;
import com.vaticle.typedb.protocol.SessionProto;
import com.vaticle.typedb.protocol.TransactionProto;
import com.vaticle.typedb.protocol.TypeDBGrpc;
import com.vaticle.typedb.protocol.VersionProto;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.DATABASE_DELETED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.DATABASE_EXISTS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.DATABASE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.ERROR_LOGGING_CONNECTIONS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.PROTOCOL_VERSION_MISMATCH;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.SERVER_SHUTDOWN;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Session.SESSION_NOT_FOUND;
import static com.vaticle.typedb.core.server.common.RequestReader.applyDefaultOptions;
import static com.vaticle.typedb.core.server.common.RequestReader.byteStringAsUUID;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Database.deleteRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Database.ruleSchemaRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Database.schemaRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Database.typeSchemaRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.DatabaseManager.allRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.DatabaseManager.containsRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.DatabaseManager.createRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Session.closeRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Session.openRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Session.pulseRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.exception;
import static java.util.stream.Collectors.toList;

public class TypeDBService extends TypeDBGrpc.TypeDBImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(TypeDBService.class);

    protected final TypeDB.DatabaseManager databaseMgr;
    private final ConcurrentMap<UUID, SessionService> sessionServices;

    public TypeDBService(TypeDB.DatabaseManager databaseMgr) {
        this.databaseMgr = databaseMgr;
        sessionServices = new ConcurrentHashMap<>();

        if (LOG.isDebugEnabled()) {
            Executors.scheduled().scheduleAtFixedRate(this::logConnectionStates, 0, 1, TimeUnit.MINUTES);
        }
    }

    private void logConnectionStates() {
        if (sessionServices.isEmpty()) return;
        try {
            Map<String, List<String>> databaseConnections = new HashMap<>();
            Instant now = Instant.now();
            sessionServices.forEach((uuid, sessionService) ->
                    databaseConnections.compute(sessionService.session().database().name(), (key, val) -> {
                        List<String> sessionInfos;
                        if (val == null) sessionInfos = new ArrayList<>();
                        else sessionInfos = val;
                        sessionInfos.add(String.format(
                                "Session '%s' (open %d seconds) has %d open transaction(s)",
                                uuid.toString(), Duration.between(sessionService.openTime(), now).getSeconds(),
                                sessionService.transactionCount()
                        ));
                        return sessionInfos;
                    })
            );
            StringBuilder connectionStates = new StringBuilder("Server connections: ").append("\n");
            databaseConnections.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
                connectionStates.append("Database '").append(entry.getKey()).append("' has ").append(entry.getValue().size()).append(" sessions:\n");
                entry.getValue().forEach(state -> connectionStates.append("\t").append(state).append("\n"));
            });
            LOG.debug(connectionStates.toString());
        } catch (Exception e) {
            LOG.error(ERROR_LOGGING_CONNECTIONS.message(), e);
        }
    }

    @Override
    public void connectionOpen(ConnectionProto.Connection.Open.Req request,
                               StreamObserver<ConnectionProto.Connection.Open.Res> responder) {
        try {
            if (request.getVersion() != VersionProto.Version.VERSION) {
                int clientProtocolVersion = request.getVersion() == VersionProto.Version.UNSPECIFIED ?
                        0 : request.getVersion().getNumber();
                TypeDBException error = TypeDBException.of(
                        PROTOCOL_VERSION_MISMATCH, VersionProto.Version.VERSION.getNumber(), clientProtocolVersion
                );
                responder.onError(exception(error));
                LOG.error(error.getMessage(), error);
            } else {
                responder.onNext(ResponseBuilder.Connection.openRes());
                responder.onCompleted();
            }
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void databasesContains(CoreDatabaseManager.Contains.Req request,
                                  StreamObserver<CoreDatabaseManager.Contains.Res> responder) {
        try {
            boolean contains = databaseMgr.contains(request.getName());
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
            if (databaseMgr.contains(request.getName())) {
                throw TypeDBException.of(DATABASE_EXISTS, request.getName());
            }
            databaseMgr.create(request.getName());
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
            List<String> databaseNames = databaseMgr.all().stream().map(TypeDB.Database::name).collect(toList());
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
            String schema = databaseMgr.get(request.getName()).schema();
            responder.onNext(schemaRes(schema));
            responder.onCompleted();
        } catch (TypeDBException e) {
            LOG.error(e.getMessage(), e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void databaseTypeSchema(CoreDatabase.TypeSchema.Req request, StreamObserver<CoreDatabase.TypeSchema.Res> responder) {
        try {
            String schema = databaseMgr.get(request.getName()).typeSchema();
            responder.onNext(typeSchemaRes(schema));
            responder.onCompleted();
        } catch (TypeDBException e) {
            LOG.error(e.getMessage(), e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void databaseRuleSchema(CoreDatabase.RuleSchema.Req request, StreamObserver<CoreDatabase.RuleSchema.Res> responder) {
        try {
            String schema = databaseMgr.get(request.getName()).ruleSchema();
            responder.onNext(ruleSchemaRes(schema));
            responder.onCompleted();
        } catch (TypeDBException e) {
            LOG.error(e.getMessage(), e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void databaseDelete(CoreDatabase.Delete.Req request, StreamObserver<CoreDatabase.Delete.Res> responder) {
        try {
            String databaseName = request.getName();
            if (!databaseMgr.contains(databaseName)) {
                throw TypeDBException.of(DATABASE_NOT_FOUND, databaseName);
            }
            TypeDB.Database database = databaseMgr.get(databaseName);
            database.sessions().parallel().forEach(session -> {
                UUID sessionId = session.uuid();
                SessionService sessionSvc = sessionServices.get(sessionId);
                if (sessionSvc != null) {
                    sessionSvc.close(TypeDBException.of(DATABASE_DELETED, databaseName));
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
            TypeDB.Session session = databaseMgr.session(request.getDatabase(), sessionType, options);
            SessionService sessionSvc = createSessionService(session, options);
            sessionServices.put(sessionSvc.UUID(), sessionSvc);
            int duration = (int) Duration.between(start, Instant.now()).toMillis();
            responder.onNext(openRes(sessionSvc.UUID(), duration));
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
            UUID sessionID = byteStringAsUUID(request.getSessionId());
            SessionService sessionSvc = sessionServices.get(sessionID);
            if (sessionSvc == null) throw TypeDBException.of(SESSION_NOT_FOUND, sessionID);
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
            UUID sessionID = byteStringAsUUID(request.getSessionId());
            SessionService sessionSvc = sessionServices.get(sessionID);
            boolean isAlive = sessionSvc != null && sessionSvc.isOpen();
            if (isAlive) sessionSvc.resetIdleTimeout();
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

    protected SessionService createSessionService(TypeDB.Session session, Options.Session options) {
        return new SessionService(this, session, options);
    }

    public SessionService session(UUID uuid) {
        return sessionServices.get(uuid);
    }

    public void closed(SessionService sessionSvc) {
        sessionServices.remove(sessionSvc.UUID());
    }

    public void close() {
        sessionServices.values().parallelStream().forEach(s -> s.close(TypeDBException.of(SERVER_SHUTDOWN)));
        sessionServices.clear();
    }
}
