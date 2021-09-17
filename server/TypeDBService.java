/*
 * Copyright (C) 2021 Vaticle
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
import com.vaticle.typedb.core.server.common.ResponseBuilder;
import com.vaticle.typedb.protocol.ClientProto;
import com.vaticle.typedb.protocol.CoreDatabaseProto.CoreDatabase;
import com.vaticle.typedb.protocol.CoreDatabaseProto.CoreDatabaseManager;
import com.vaticle.typedb.protocol.SessionProto;
import com.vaticle.typedb.protocol.TransactionProto;
import com.vaticle.typedb.protocol.TypeDBGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Client.CLIENT_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.DATABASE_DELETED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.DATABASE_EXISTS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.DATABASE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.SERVER_SHUTDOWN;
import static com.vaticle.typedb.core.server.common.RequestReader.applyDefaultOptions;
import static com.vaticle.typedb.core.server.common.RequestReader.byteStringAsUUID;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Client.pulseRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Database.deleteRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Database.schemaRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.DatabaseManager.allRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.DatabaseManager.containsRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.DatabaseManager.createRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Session.closeRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Session.openRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.exception;
import static com.vaticle.typedb.protocol.ClientProto.Client.Open.Req.IdleTimeoutOptCase.IDLE_TIMEOUT_MILLIS;
import static java.util.stream.Collectors.toList;

public class TypeDBService extends TypeDBGrpc.TypeDBImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(TypeDBService.class);

    protected final TypeDB typeDB;
    private final ConcurrentMap<UUID, ClientService> clientServices;

    public TypeDBService(TypeDB typeDB) {
        this.typeDB = typeDB;
        clientServices = new ConcurrentHashMap<>();
    }

    @Override
    public void clientOpen(ClientProto.Client.Open.Req request,
                            StreamObserver<ClientProto.Client.Open.Res> responder) {
        try {
            ClientService clientSvc;
            if (request.getIdleTimeoutOptCase().equals(IDLE_TIMEOUT_MILLIS)) {
                clientSvc = new ClientService(typeDB, request.getIdleTimeoutMillis());
            } else {
                clientSvc = new ClientService(typeDB);
            }
            clientServices.put(clientSvc.ID(), clientSvc);
            responder.onNext(ResponseBuilder.Client.openRes(clientSvc.ID()));
            responder.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void clientPulse(ClientProto.Client.Pulse.Req request,
                            StreamObserver<ClientProto.Client.Pulse.Res> responder) {
        try {
            UUID clientID = byteStringAsUUID(request.getClientId());
            ClientService clientSvc = clientServices.get(clientID);
            boolean isAlive = clientSvc != null && clientSvc.isOpen();
            if (isAlive) clientSvc.resetIdleTimeout();
            System.out.println("client pulse - alive: " + isAlive);
            responder.onNext(pulseRes(isAlive));
            responder.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void clientClose(ClientProto.Client.Close.Req request,
                             StreamObserver<ClientProto.Client.Close.Res> responder) {
        try {
            UUID clientID = byteStringAsUUID(request.getClientId());
            ClientService clientSvc = clientServices.get(clientID);
            if (clientSvc == null) throw TypeDBException.of(CLIENT_NOT_FOUND, clientID);
            clientSvc.close();
            responder.onNext(ResponseBuilder.Client.closeRes());
            responder.onCompleted();
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            responder.onError(exception(e));
        }
    }

    @Override
    public void databasesContains(CoreDatabaseManager.Contains.Req request,
                                  StreamObserver<CoreDatabaseManager.Contains.Res> responder) {
        try {
            boolean contains = typeDB.databases().contains(request.getName());
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
            if (typeDB.databases().contains(request.getName())) {
                throw TypeDBException.of(DATABASE_EXISTS, request.getName());
            }
            typeDB.databases().create(request.getName());
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
            List<String> databaseNames = typeDB.databases().all().stream().map(TypeDB.Database::name).collect(toList());
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
            String schema = typeDB.databases().get(request.getName()).schema();
            responder.onNext(schemaRes(schema));
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
            if (!typeDB.databases().contains(databaseName)) {
                throw TypeDBException.of(DATABASE_NOT_FOUND, databaseName);
            }
            TypeDB.Database database = typeDB.databases().get(databaseName);
            database.sessions().parallel().forEach(session ->
                    clientServices.values().parallelStream().forEach(clientSvc -> {
                        if (clientSvc.sessionGet(session.uuid()) != null) {
                            clientSvc.sessionClose(session.uuid(), TypeDBException.of(DATABASE_DELETED, databaseName));
                        }
                    })
            );
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
            UUID clientID = byteStringAsUUID(request.getClientId());
            ClientService clientSvc = clientServices.get(clientID);
            if (clientSvc == null) throw TypeDBException.of(CLIENT_NOT_FOUND);
            Arguments.Session.Type sessionType = Arguments.Session.Type.of(request.getType().getNumber());
            Options.Session options = applyDefaultOptions(new Options.Session(), request.getOptions());
            UUID sessionID = clientSvc.sessionOpen(request.getDatabase(), sessionType, options);
            int duration = (int) Duration.between(start, Instant.now()).toMillis();
            responder.onNext(openRes(sessionID, duration));
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
            UUID clientID = byteStringAsUUID(request.getClientId());
            ClientService clientSvc = clientServices.get(clientID);
            if (clientSvc == null) throw TypeDBException.of(CLIENT_NOT_FOUND);
            UUID sessionID = byteStringAsUUID(request.getSessionId());
            clientSvc.sessionClose(sessionID);
            responder.onNext(closeRes());
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

    @Nullable
    public ClientService client(UUID ID) {
        return clientServices.get(ID);
    }

    public void close() {
        clientServices.values().parallelStream().forEach(c -> c.close(TypeDBException.of(SERVER_SHUTDOWN)));
        clientServices.clear();
    }
}
