/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.rocks;

import grakn.core.Grakn;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Context;
import grakn.core.common.parameters.Options;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static grakn.core.common.exception.ErrorMessage.Session.SESSION_CLOSED;

public abstract class RocksSession implements Grakn.Session {

    private final Context.Session context;
    private final UUID uuid;
    private RocksDatabase database;
    final ConcurrentMap<RocksTransaction, Long> transactions;
    final AtomicBoolean isOpen;

    private RocksSession(Context.Session context) {
        this.context = context;

        uuid = UUID.randomUUID();
        transactions = new ConcurrentHashMap<>();
        isOpen = new AtomicBoolean(true);
    }

    void database(RocksDatabase database) {
        this.database = database;
    }

    Context.Session context() {
        return context;
    }

    boolean isSchema() {
        return false;
    }

    boolean isData() {
        return false;
    }

    RocksSession.Schema asSchema() {
        throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(RocksSession.Schema.class));
    }

    RocksSession.Data asData() {
        throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(RocksSession.Data.class));
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
        private final Factory.TransactionSchema factory;

        public Schema(Context.Session context, Factory.TransactionSchema factory) {
            super(context);
            this.factory = factory;
        }

        public static RocksSession.Schema create(RocksDatabase rocksDatabase, Options.Session options, Factory.Session sessionFactory) {
            Context.Session context = new Context.Session(rocksDatabase.options(), options).type(Arguments.Session.Type.SCHEMA);
            RocksSession.Schema session = sessionFactory.sessionSchema(context);
            session.database(rocksDatabase);
            return session;
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
            if (!isOpen.get()) throw GraknException.of(SESSION_CLOSED);
            final RocksTransaction.Schema transaction = RocksTransaction.Schema.create(this, type, options, factory);
            transactions.put(transaction, 0L);
            return transaction;
        }

        @Override
        void remove(RocksTransaction transaction) {
            transactions.remove(transaction);
        }
    }

    public static class Data extends RocksSession {

        private final Factory.TransactionData factory;

        public Data(Context.Session context, Factory.TransactionData factory) {
            super(context);
            this.factory = factory;
        }

        public static RocksSession.Data create(RocksDatabase rocksDatabase, Options.Session options, Factory.Session sessionFactory) {
            Context.Session context = new Context.Session(rocksDatabase.options(), options).type(Arguments.Session.Type.DATA);
            RocksSession.Data session = sessionFactory.sessionData(context);
            session.database(rocksDatabase);
            return session;
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
            if (!isOpen.get()) throw GraknException.of(SESSION_CLOSED);
            long lock = 0;
            if (type.isWrite()) lock = database().dataWriteSchemaLock().readLock();
//            final RocksTransaction.Data transaction = factory.transactionData(this, type, options);
            final RocksTransaction.Data transaction = RocksTransaction.Data.create(this, type, options, factory);
            transactions.put(transaction, lock);
            return transaction;
        }

        @Override
        void remove(RocksTransaction transaction) {
            final long lock = transactions.remove(transaction);
            if (transaction.type().isWrite()) database().dataWriteSchemaLock().unlockRead(lock);
        }
    }
}
