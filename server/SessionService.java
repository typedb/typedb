/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.server;

import com.vaticle.typedb.common.collection.ConcurrentSet;
import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.StampedLock;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Session.SESSION_CLOSED;
import static com.vaticle.typedb.core.concurrent.executor.Executors.scheduled;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SessionService implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SessionService.class);

    private final ConcurrentSet<TransactionService> transactionServices;
    private final TypeDBService typeDBSvc;
    private final Options.Session options;
    private final TypeDB.Session session;
    private final ReadWriteLock accessLock;
    private final AtomicBoolean isOpen;
    private final Instant openTime;
    private ScheduledFuture<?> idleTimeoutTask;

    public SessionService(TypeDBService typeDBSvc, TypeDB.Session session, Options.Session options) {
        this.typeDBSvc = typeDBSvc;
        this.session = session;
        this.options = options;
        this.accessLock = new StampedLock().asReadWriteLock();
        this.isOpen = new AtomicBoolean(true);
        this.transactionServices = new ConcurrentSet<>();
        startIdleTimeout();
        this.openTime = Instant.now();
    }

    void register(TransactionService transactionSvc) {
        try {
            accessLock.readLock().lock();
            if (isOpen.get()) {
                transactionServices.add(transactionSvc);
                cancelIdleTimeout();
            } else throw TypeDBException.of(SESSION_CLOSED);
        } finally {
            accessLock.readLock().unlock();
        }
    }

    void closed(TransactionService transactionSvc) {
        transactionServices.remove(transactionSvc);
        mayStartIdleTimeout();
    }

    public boolean isOpen() {
        return isOpen.get();
    }

    public UUID UUID() {
        return session.uuid();
    }

    public TypeDB.Session session() {
        return session;
    }

    public Options.Session options() {
        return options;
    }

    public synchronized void resetIdleTimeout() {
        cancelIdleTimeout();
        mayStartIdleTimeout();
    }

    private void cancelIdleTimeout() {
        idleTimeoutTask.cancel(false);
    }

    private synchronized void mayStartIdleTimeout() {
        if (isOpen() && transactionServices.isEmpty() && !isIdleTimeoutActive()) {
            startIdleTimeout();
        }
    }

    private synchronized void startIdleTimeout() {
        idleTimeoutTask = scheduled().schedule(this::idleTimeout, options.sessionIdleTimeoutMillis(), MILLISECONDS);
    }

    private boolean isIdleTimeoutActive() {
        return !idleTimeoutTask.isDone();
    }

    Instant openTime() {
        return openTime;
    }

    long transactionCount() {
        return transactionServices.size();
    }

    private void idleTimeout() {
        try {
            accessLock.writeLock().lock();
            if (transactionServices.isEmpty()) {
                if (isOpen.compareAndSet(true, false)) {
                    session.close();
                    typeDBSvc.closed(this);
                    LOG.warn("Session with ID " + session.uuid() + " timed out due to inactivity");
                }
            }
        } finally {
            accessLock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        try {
            accessLock.writeLock().lock();
            idleTimeoutTask.cancel(false);
            if (isOpen.compareAndSet(true, false)) {
                transactionServices.forEach(TransactionService::close);
                session.close();
                typeDBSvc.closed(this);
            }
        } finally {
            accessLock.writeLock().unlock();
        }
    }

    public void close(Throwable error) {
        try {
            accessLock.writeLock().lock();
            if (isOpen.compareAndSet(true, false)) {
                transactionServices.forEach(tr -> tr.close(error));
                session.close();
                typeDBSvc.closed(this);
            }
        } finally {
            accessLock.writeLock().unlock();
        }
    }

    @Nullable
    public String databaseName() {
        return session != null && session.database() != null
                ? session.database().name()
                : null;
    }
}
