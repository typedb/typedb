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
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Context;
import grakn.core.common.parameters.Options;
import grakn.core.graph.util.KeyGenerator;
import org.rocksdb.OptimisticTransactionDB;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class RocksSession implements Grakn.Session {
    private final RocksDatabase database;
    private final Arguments.Session.Type type;
    private final Context.Session context;
    private final ConcurrentMap<RocksTransaction, Long> transactions;
    private final AtomicBoolean isOpen;
    private final UUID uuid;

    RocksSession(RocksDatabase database, Arguments.Session.Type type, Options.Session options) {
        this.database = database;
        this.type = type;
        this.context = new Context.Session(database.options(), options).type(type);

        uuid = UUID.randomUUID();
        transactions = new ConcurrentHashMap<>();
        isOpen = new AtomicBoolean(true);
    }

    Context.Session context() {
        return context;
    }

    OptimisticTransactionDB rocks() {
        return database.rocks();
    }

    KeyGenerator.Schema schemaKeyGenerator() {
        return database.schemaKeyGenerator();
    }

    KeyGenerator.Data dataKeyGenerator() {
        return database.dataKeyGenerator();
    }

    void remove(RocksTransaction transaction) {
        long schemaReadLockStamp = transactions.remove(transaction);
        if (this.type.isData() && transaction.type().isWrite()) {
            database.releaseSchemaReadLock(schemaReadLockStamp);
        }
    }

    @Override
    public Arguments.Session.Type type() {
        return type;
    }

    @Override
    public RocksDatabase database() {
        return database;
    }

    @Override
    public RocksTransaction transaction(Arguments.Transaction.Type type) {
        return transaction(type, new Options.Transaction());
    }

    @Override
    public RocksTransaction transaction(Arguments.Transaction.Type type, Options.Transaction options) {
        long schemaReadLockStamp = 0;
        if (this.type.isData() && type.isWrite()) schemaReadLockStamp = database.acquireSchemaReadLock();
        RocksTransaction transaction = new RocksTransaction(this, type, options);
        transactions.put(transaction, schemaReadLockStamp);
        return transaction;
    }

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
            database.remove(this);
        }
    }
}
