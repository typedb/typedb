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
import com.vaticle.typedb.core.common.diagnostics.CoreDiagnostics;
import com.vaticle.typedb.core.common.exception.ErrorMessage;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concurrent.executor.Executors;
import com.vaticle.typedb.core.server.common.ResponseBuilder;
import com.vaticle.typedb.protocol.ConnectionProto;
import com.vaticle.typedb.protocol.DatabaseProto.Database;
import com.vaticle.typedb.protocol.DatabaseProto.DatabaseManager;
import com.vaticle.typedb.protocol.ServerProto.ServerManager;
import com.vaticle.typedb.protocol.SessionProto;
import com.vaticle.typedb.protocol.TransactionProto;
import com.vaticle.typedb.protocol.TypeDBGrpc;
import com.vaticle.typedb.protocol.UserProto.User;
import com.vaticle.typedb.protocol.UserProto.UserManager;
import com.vaticle.typedb.protocol.VersionProto;
import io.grpc.stub.StreamObserver;
import io.sentry.Sentry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
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
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.USER_MANAGEMENT_NOT_AVAILABLE;
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
import static com.vaticle.typedb.core.server.common.ResponseBuilder.DatabaseManager.getRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Session.closeRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Session.openRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Session.pulseRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.exception;
import static java.util.stream.Collectors.toList;

public class TypeDBService extends TypeDBGrpc.TypeDBImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(TypeDBService.class);

    private final String address;
    protected final TypeDB.DatabaseManager databaseMgr;
    private final ConcurrentMap<UUID, SessionService> sessionServices;

    public TypeDBService(InetSocketAddress address, TypeDB.DatabaseManager databaseMgr) {
        this.address = address.getHostString() + ":" + address.getPort();
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
                int driverProtocolVersion = request.getVersion() == VersionProto.Version.UNSPECIFIED ?
                        0 : request.getVersionValue();
                TypeDBException error = TypeDBException.of(
                        PROTOCOL_VERSION_MISMATCH, VersionProto.Version.VERSION.getNumber(), driverProtocolVersion
                );
                responder.onError(exception(error));
                CoreDiagnostics.submitError(error);
                LOG.error(error.getMessage(), error);
            } else {
                responder.onNext(ResponseBuilder.Connection.openRes());
                responder.onCompleted();
            }
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            Sentry.captureException(e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void serversAll(ServerManager.All.Req request, StreamObserver<ServerManager.All.Res> responder) {
        try {
            responder.onNext(ResponseBuilder.ServerManager.allRes(address));
            responder.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            CoreDiagnostics.submitError(e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void usersContains(UserManager.Contains.Req request, StreamObserver<UserManager.Contains.Res> responder) {
        ErrorMessage errorMessage = USER_MANAGEMENT_NOT_AVAILABLE;
        LOG.error(errorMessage.message());
        TypeDBException exception = TypeDBException.of(errorMessage);
        CoreDiagnostics.submitError(exception);
        responder.onError(exception);
    }

    @Override
    public void usersCreate(UserManager.Create.Req request, StreamObserver<UserManager.Create.Res> responder) {
        ErrorMessage errorMessage = USER_MANAGEMENT_NOT_AVAILABLE;
        LOG.error(errorMessage.message());
        TypeDBException exception = TypeDBException.of(errorMessage);
        CoreDiagnostics.submitError(exception);
        responder.onError(exception);
    }

    @Override
    public void usersDelete(UserManager.Delete.Req request, StreamObserver<UserManager.Delete.Res> responder) {
        ErrorMessage errorMessage = USER_MANAGEMENT_NOT_AVAILABLE;
        LOG.error(errorMessage.message());
        TypeDBException exception = TypeDBException.of(errorMessage);
        CoreDiagnostics.submitError(exception);
        responder.onError(exception);
    }

    @Override
    public void usersAll(UserManager.All.Req request, StreamObserver<UserManager.All.Res> responder) {
        ErrorMessage errorMessage = USER_MANAGEMENT_NOT_AVAILABLE;
        LOG.error(errorMessage.message());
        TypeDBException exception = TypeDBException.of(errorMessage);
        CoreDiagnostics.submitError(exception);
        responder.onError(exception);
    }

    @Override
    public void usersPasswordSet(UserManager.PasswordSet.Req request, StreamObserver<UserManager.PasswordSet.Res> responder) {
        ErrorMessage errorMessage = USER_MANAGEMENT_NOT_AVAILABLE;
        LOG.error(errorMessage.message());
        TypeDBException exception = TypeDBException.of(errorMessage);
        CoreDiagnostics.submitError(exception);
        responder.onError(exception);
    }

    @Override
    public void usersGet(UserManager.Get.Req request, StreamObserver<UserManager.Get.Res> responder) {
        ErrorMessage errorMessage = USER_MANAGEMENT_NOT_AVAILABLE;
        LOG.error(errorMessage.message());
        TypeDBException exception = TypeDBException.of(errorMessage);
        CoreDiagnostics.submitError(exception);
        responder.onError(exception);
    }

    @Override
    public void userPasswordUpdate(User.PasswordUpdate.Req request, StreamObserver<User.PasswordUpdate.Res> responder) {
        ErrorMessage errorMessage = USER_MANAGEMENT_NOT_AVAILABLE;
        LOG.error(errorMessage.message());
        TypeDBException exception = TypeDBException.of(errorMessage);
        CoreDiagnostics.submitError(exception);
        responder.onError(exception);
    }

    @Override
    public void userToken(User.Token.Req request, StreamObserver<User.Token.Res> responder) {
        ErrorMessage errorMessage = USER_MANAGEMENT_NOT_AVAILABLE;
        LOG.error(errorMessage.message());
        TypeDBException exception = TypeDBException.of(errorMessage);
        CoreDiagnostics.submitError(exception);
        responder.onError(exception);
    }

    @Override
    public void databasesContains(DatabaseManager.Contains.Req request, StreamObserver<DatabaseManager.Contains.Res> responder) {
        try {
            boolean contains = databaseMgr.contains(request.getName());
            responder.onNext(containsRes(contains));
            responder.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            CoreDiagnostics.submitError(e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void databasesCreate(DatabaseManager.Create.Req request, StreamObserver<DatabaseManager.Create.Res> responder) {
        try {
            doCreateDatabase(request.getName());
            responder.onNext(createRes());
            responder.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            CoreDiagnostics.submitError(e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void databasesGet(DatabaseManager.Get.Req request, StreamObserver<DatabaseManager.Get.Res> responder) {
        try {
            responder.onNext(getRes(address, request.getName()));
            responder.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            CoreDiagnostics.submitError(e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void databasesAll(DatabaseManager.All.Req request, StreamObserver<DatabaseManager.All.Res> responder) {
        try {
            List<String> databaseNames = databaseMgr.all().stream().map(TypeDB.Database::name).collect(toList());
            responder.onNext(allRes(address, databaseNames));
            responder.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            CoreDiagnostics.submitError(e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void databaseSchema(Database.Schema.Req request, StreamObserver<Database.Schema.Res> responder) {
        try {
            String schema = databaseMgr.get(request.getName()).schema();
            responder.onNext(schemaRes(schema));
            responder.onCompleted();
        } catch (TypeDBException e) {
            LOG.error(e.getMessage(), e);
            CoreDiagnostics.submitError(e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void databaseTypeSchema(Database.TypeSchema.Req request, StreamObserver<Database.TypeSchema.Res> responder) {
        try {
            String schema = databaseMgr.get(request.getName()).typeSchema();
            responder.onNext(typeSchemaRes(schema));
            responder.onCompleted();
        } catch (TypeDBException e) {
            LOG.error(e.getMessage(), e);
            CoreDiagnostics.submitError(e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void databaseRuleSchema(Database.RuleSchema.Req request, StreamObserver<Database.RuleSchema.Res> responder) {
        try {
            String schema = databaseMgr.get(request.getName()).ruleSchema();
            responder.onNext(ruleSchemaRes(schema));
            responder.onCompleted();
        } catch (TypeDBException e) {
            LOG.error(e.getMessage(), e);
            CoreDiagnostics.submitError(e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void databaseDelete(Database.Delete.Req request, StreamObserver<Database.Delete.Res> responder) {
        try {
            doDeleteDatabase(request.getName());
            responder.onNext(deleteRes());
            responder.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            CoreDiagnostics.submitError(e);
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
            SessionService sessionSvc = doCreateSessionService(session, options);
            sessionServices.put(sessionSvc.UUID(), sessionSvc);
            int duration = (int) Duration.between(start, Instant.now()).toMillis();
            responder.onNext(openRes(sessionSvc.UUID(), duration));
            responder.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            CoreDiagnostics.submitError(e);
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
            CoreDiagnostics.submitError(e);
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
            CoreDiagnostics.submitError(e);
            responder.onError(exception(e));
        }
    }

    @Override
    public StreamObserver<TransactionProto.Transaction.Client> transaction(
            StreamObserver<TransactionProto.Transaction.Server> responder) {
        return new TransactionService(this, responder);
    }

    protected void doCreateDatabase(String databaseName) {
        if (databaseMgr.contains(databaseName)) {
            throw TypeDBException.of(DATABASE_EXISTS, databaseName);
        }
        databaseMgr.create(databaseName);
    }

    protected void doDeleteDatabase(String databaseName) {
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
    }

    protected SessionService doCreateSessionService(TypeDB.Session session, Options.Session options) {
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
