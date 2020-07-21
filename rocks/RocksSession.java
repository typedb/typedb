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
import grakn.core.common.options.GraknOptions;
import grakn.core.graph.util.KeyGenerator;
import org.rocksdb.OptimisticTransactionDB;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class RocksSession implements Grakn.Session {

    private final RocksDatabase database;
    private final Type type;
    private final GraknOptions.Session options;
    private final ConcurrentMap<RocksTransaction, Long> transactions;
    private final AtomicBoolean isOpen;
    private final UUID uuid;

    RocksSession(RocksDatabase database, Type type, GraknOptions.Session options) {
        this.database = database;
        this.type = type;
        this.options = options;
        this.options.parent(database.options());

        uuid = UUID.randomUUID();
        transactions = new ConcurrentHashMap<>();
        isOpen = new AtomicBoolean();
        isOpen.set(true);
    }

    OptimisticTransactionDB rocks() {
        return database.rocks();
    }

    KeyGenerator keyGenerator() {
        return database.keyGenerator();
    }

    void remove(RocksTransaction transaction) {
        long schemaReadLockStamp = transactions.remove(transaction);
        if (this.type.equals(Type.DATA) && transaction.type().equals(Grakn.Transaction.Type.WRITE)) {
            database.releaseSchemaReadLock(schemaReadLockStamp);
        }
    }

    @Override
    public Type type() {
        return type;
    }

    @Override
    public GraknOptions.Session options() {
        return options;
    }

    @Override
    public RocksDatabase database() {
        return database;
    }

    @Override
    public RocksTransaction transaction(Grakn.Transaction.Type type) {
        return transaction(type, new GraknOptions.Transaction());
    }

    @Override
    public RocksTransaction transaction(Grakn.Transaction.Type type, GraknOptions.Transaction options) {
        long schemaReadLockStamp = 0;
        if (this.type.equals(Type.DATA) && type.equals(Grakn.Transaction.Type.WRITE)) {
            schemaReadLockStamp = database.acquireSchemaReadLock();
        }
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
