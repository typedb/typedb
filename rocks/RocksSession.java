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
 *
 */

package com.vaticle.typedb.core.rocks;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNEXPECTED_INTERRUPTION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Session.SCHEMA_ACQUIRE_LOCK_TIMEOUT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Session.SESSION_CLOSED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.DATA_ACQUIRE_LOCK_TIMEOUT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class RocksSession implements TypeDB.Session {

    private static final Logger LOG = LoggerFactory.getLogger(RocksSession.class);

    private final RocksDatabase database;
    private final UUID uuid;
    private final Context.Session context;
    protected final ConcurrentMap<RocksTransaction, Long> transactions;
    protected final AtomicBoolean isOpen;

    private RocksSession(RocksDatabase database, Arguments.Session.Type type, Options.Session options) {
        this.database = database;
        this.context = new Context.Session(database.options(), options).type(type);

        uuid = UUID.randomUUID();
        transactions = new ConcurrentHashMap<>();
        isOpen = new AtomicBoolean(true);
    }

    public Context.Session context() {
        return context;
    }

    boolean isSchema() {
        return false;
    }

    boolean isData() {
        return false;
    }

    RocksSession.Schema asSchema() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(RocksSession.Schema.class));
    }

    RocksSession.Data asData() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(RocksSession.Data.class));
    }

    abstract void remove(RocksTransaction transaction);

    @Override
    public Arguments.Session.Type type() {
        return context.sessionType();
    }

    @Override
    public RocksDatabase database() {
        return database;
    }

    @Override
    public abstract RocksTransaction transaction(Arguments.Transaction.Type type);

    @Override
    public abstract RocksTransaction transaction(Arguments.Transaction.Type type, Options.Transaction options);

    @Override
    public UUID uuid() {
        return uuid;
    }

    @Override
    public boolean isOpen() {
        return this.isOpen.get();
    }

    @Override
    public void close() {
        if (isOpen.compareAndSet(true, false)) {
            transactions.keySet().parallelStream().forEach(RocksTransaction::close);
            database().remove(this);
        }
    }

    public static class Schema extends RocksSession {
        protected final Factory.TransactionSchema txSchemaFactory;
        protected final Lock writeLock;

        public Schema(RocksDatabase database, Arguments.Session.Type type, Options.Session options,
                      Factory.TransactionSchema txSchemaFactory) {
            super(database, type, options);
            this.txSchemaFactory = txSchemaFactory;
            this.writeLock = new StampedLock().asReadWriteLock().writeLock();
        }

        @Override
        boolean isSchema() {
            return true;
        }

        @Override
        RocksSession.Schema asSchema() {
            return this;
        }

        @Override
        public RocksTransaction.Schema transaction(Arguments.Transaction.Type type) {
            return transaction(type, new Options.Transaction());
        }

        @Override
        public RocksTransaction.Schema transaction(Arguments.Transaction.Type type, Options.Transaction options) {
            if (!isOpen.get()) throw TypeDBException.of(SESSION_CLOSED);
            if (type.isWrite()) {
                try {
                    if (!writeLock.tryLock(options.schemaLockTimeoutMillis(), MILLISECONDS)) {
                        throw TypeDBException.of(SCHEMA_ACQUIRE_LOCK_TIMEOUT);
                    }
                } catch (InterruptedException e) {
                    throw TypeDBException.of(e);
                }
            }
            RocksTransaction.Schema transaction = txSchemaFactory.transaction(this, type, options);
            transactions.put(transaction, 0L);
            return transaction;

        }

        RocksTransaction.Schema initialisationTransaction() {
            if (!isOpen.get()) throw TypeDBException.of(SESSION_CLOSED);
            try {
                if (!writeLock.tryLock(new Options.Transaction().schemaLockTimeoutMillis(), MILLISECONDS)) {
                    throw TypeDBException.of(SCHEMA_ACQUIRE_LOCK_TIMEOUT);
                }
            } catch (InterruptedException e) {
                throw TypeDBException.of(e);
            }
            RocksTransaction.Schema transaction = txSchemaFactory.initialisationTransaction(this);
            transactions.put(transaction, 0L);
            return transaction;
        }

        @Override
        void remove(RocksTransaction transaction) {
            transactions.remove(transaction);
            if (transaction.type().isWrite()) writeLock.unlock();
        }
    }

    public static class Data extends RocksSession {

        protected final Factory.TransactionData txDataFactory;

        public Data(RocksDatabase database, Arguments.Session.Type type, Options.Session options, Factory.TransactionData txDataFactory) {
            super(database, type, options);
            this.txDataFactory = txDataFactory;
        }

        @Override
        boolean isData() {
            return true;
        }

        @Override
        RocksSession.Data asData() {
            return this;
        }

        @Override
        public RocksTransaction.Data transaction(Arguments.Transaction.Type type) {
            return transaction(type, new Options.Transaction());
        }

        @Override
        public RocksTransaction.Data transaction(Arguments.Transaction.Type type, Options.Transaction options) {
            if (!isOpen.get()) throw TypeDBException.of(SESSION_CLOSED);
            long lock = 0;
            if (type == Arguments.Transaction.Type.WRITE) {
                try {
                    int timeout = options.schemaLockTimeoutMillis();
                    lock = database().schemaLock().tryReadLock(timeout, MILLISECONDS);
                    if (lock == 0) throw TypeDBException.of(DATA_ACQUIRE_LOCK_TIMEOUT);
                } catch (InterruptedException e) {
                    throw TypeDBException.of(UNEXPECTED_INTERRUPTION);
                }
            }
            RocksTransaction.Data transaction = txDataFactory.transaction(this, type, options);
            transactions.put(transaction, lock);
            return transaction;
        }

        @Override
        void remove(RocksTransaction transaction) {
            long lock = transactions.remove(transaction);
            if (transaction.type().isWrite()) {
                assert lock != 0;
                database().schemaLock().unlockRead(lock);
            }
        }
    }
}
