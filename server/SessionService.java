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

import com.vaticle.typedb.common.collection.ConcurrentSet;
import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.StampedLock;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Session.SESSION_CLOSED;
import static com.vaticle.typedb.core.concurrent.executor.Executors.scheduled;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SessionService implements AutoCloseable {

    private final ConcurrentSet<TransactionService> transactionServices;
    private final ClientService clientSvc;
    private final Options.Session options;
    private final TypeDB.Session session;
    private final ReadWriteLock accessLock;
    private final AtomicBoolean isOpen;

    public SessionService(ClientService clientSvc, TypeDB.Session session, Options.Session options) {
        this.clientSvc = clientSvc;
        this.session = session;
        this.options = options;
        this.accessLock = new StampedLock().asReadWriteLock();
        this.isOpen = new AtomicBoolean(true);
        this.transactionServices = new ConcurrentSet<>();
    }

    void register(TransactionService transactionSvc) {
        try {
            accessLock.readLock().lock();
            if (isOpen.get()) {
                transactionServices.add(transactionSvc);
            } else throw TypeDBException.of(SESSION_CLOSED);
        } finally {
            accessLock.readLock().unlock();
        }
    }

    void unregister(TransactionService transactionSvc) {
        transactionServices.remove(transactionSvc);
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

    @Override
    public void close() {
        try {
            accessLock.writeLock().lock();
            if (isOpen.compareAndSet(true, false)) {
                transactionServices.forEach(TransactionService::close);
                session.close();
                clientSvc.sessionUnregister(UUID());
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
                clientSvc.sessionUnregister(UUID());
            }
        } finally {
            accessLock.writeLock().unlock();
        }
    }
}
