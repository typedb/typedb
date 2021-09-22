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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Session.SESSION_NOT_FOUND;
import static com.vaticle.typedb.core.concurrent.executor.Executors.scheduled;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ClientService {
    private static final Logger LOG = LoggerFactory.getLogger(SessionService.class);
    private static final int DEFAULT_IDLE_TIMEOUT_MILLIS = 30_000;

    private final UUID ID;
    private final TypeDB typeDB;
    private final ConcurrentMap<UUID, SessionService> sessionServices;
    private final AtomicBoolean isOpen;
    private final int idleTimeoutMillis;
    private ScheduledFuture<?> idleTimeoutTask;

    public ClientService(TypeDB typeDB) {
        this(typeDB, DEFAULT_IDLE_TIMEOUT_MILLIS);
    }

    public ClientService(TypeDB typeDB, int idleTimeoutMillis) {
        ID = UUID.randomUUID();
        this.typeDB = typeDB;
        sessionServices = new ConcurrentHashMap<>();
        isOpen = new AtomicBoolean(true);
        this.idleTimeoutMillis = idleTimeoutMillis;
        mayStartIdleTimeout(); // start idle timeout
    }

    public UUID sessionOpen(String database, Arguments.Session.Type sessionType, Options.Session options) {
        TypeDB.Session session = typeDB.session(database, sessionType, options);
        SessionService sessionSvc = new SessionService(this, session, options);
        sessionServices.put(sessionSvc.UUID(), sessionSvc);
        return sessionSvc.UUID();
    }

    @Nullable
    public SessionService sessionGet(UUID ID) {
        return sessionServices.get(ID);
    }

    public void sessionClose(UUID ID) {
        SessionService sessionSvc = sessionServices.get(ID);
        if (sessionSvc == null) throw TypeDBException.of(SESSION_NOT_FOUND, ID);
        sessionSvc.close();
    }

    public void sessionClose(UUID ID, Throwable error) {
        SessionService sessionSvc = sessionServices.get(ID);
        if (sessionSvc == null) throw TypeDBException.of(SESSION_NOT_FOUND, ID);
        sessionSvc.close(error);
    }

    public void sessionUnregister(UUID ID) {
        sessionServices.remove(ID);
    }

    public UUID ID() {
        return ID;
    }

    public boolean isOpen() {
        return isOpen.get();
    }

    public synchronized void resetIdleTimeout() {
        cancelIdleTimeout();
        mayStartIdleTimeout();
    }

    private void cancelIdleTimeout() {
        assert idleTimeoutTask != null;
        idleTimeoutTask.cancel(false);
    }

    private void mayStartIdleTimeout() {
        if (sessionServices.isEmpty()) {
            idleTimeoutTask = scheduled().schedule(this::idleTimeout, idleTimeoutMillis, MILLISECONDS); // TODO: don't hard-code client idle timeout
        }
    }

    private void idleTimeout() {
        if (sessionServices.isEmpty()) {
            close();
            LOG.warn("Client with ID " + ID() + " timed out due to inactivity");
        } else resetIdleTimeout();
    }

    public void close() {
        if (isOpen.compareAndSet(true, false)) {
            if (idleTimeoutTask != null) idleTimeoutTask.cancel(false);
            sessionServices.values().parallelStream().forEach(SessionService::close);
        }
    }

    public void close(Throwable error) {
        if (isOpen.compareAndSet(true, false)) {
            if (idleTimeoutTask != null) idleTimeoutTask.cancel(false);
            sessionServices.values().parallelStream().forEach(sessionSvc -> sessionSvc.close(error));
        }
    }
}
