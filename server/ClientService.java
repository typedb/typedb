package com.vaticle.typedb.core.server;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;

import javax.annotation.Nullable;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Session.SESSION_NOT_FOUND;

public class ClientService {
    private final UUID ID;
    private final TypeDB typeDB;
    private final ConcurrentMap<UUID, SessionService> sessionServices;
    private final AtomicBoolean isOpen;

    public ClientService(TypeDB typeDB) {
        ID = UUID.randomUUID();
        this.typeDB = typeDB;
        sessionServices = new ConcurrentHashMap<>();
        isOpen = new AtomicBoolean(true);
    }

    public UUID ID() {
        return ID;
    }

    public boolean isOpen() {
        return isOpen.get();
    }

    public void resetIdleTimeout() {
        throw new UnsupportedOperationException();
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

    public void close() {
        if (isOpen.compareAndSet(true, false)) {
            sessionServices.values().parallelStream().forEach(SessionService::close);
        }
    }

    public void close(Throwable error) {
        if (isOpen.compareAndSet(true, false)) {
            sessionServices.values().parallelStream().forEach(sessionSvc -> sessionSvc.close(error));
        }
    }
}
