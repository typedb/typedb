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

package hypergraph.rocks;

import hypergraph.Hypergraph;
import hypergraph.graph.util.KeyGenerator;
import org.rocksdb.OptimisticTransactionDB;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class RocksSession implements Hypergraph.Session {

    private final RocksKeyspace keyspace;
    private final Type type;
    private final ConcurrentMap<RocksTransaction, Long> transactions;
    private final AtomicBoolean isOpen;

    RocksSession(RocksKeyspace keyspace, Type type) {
        this.keyspace = keyspace;
        this.type = type;

        transactions = new ConcurrentHashMap<>();
        isOpen = new AtomicBoolean();
        isOpen.set(true);
    }

    OptimisticTransactionDB rocks() {
        return keyspace.rocks();
    }

    KeyGenerator keyGenerator() {
        return keyspace.keyGenerator();
    }

    void remove(RocksTransaction transaction) {
        long schemaReadLockStamp = transactions.remove(transaction);
        if (this.type.equals(Type.DATA) && transaction.type().equals(Hypergraph.Transaction.Type.WRITE)) {
            keyspace.releaseSchemaReadLock(schemaReadLockStamp);
        }
    }

    @Override
    public Type type() {
        return type;
    }

    @Override
    public RocksKeyspace keyspace() {
        return keyspace;
    }

    @Override
    public RocksTransaction transaction(Hypergraph.Transaction.Type type) {
        long schemaReadLockStamp = 0;
        if (this.type.equals(Type.DATA) && type.equals(Hypergraph.Transaction.Type.WRITE)) {
            schemaReadLockStamp = keyspace.acquireSchemaReadLock();
        }
        RocksTransaction transaction = new RocksTransaction(this, type);
        transactions.put(transaction, schemaReadLockStamp);
        return transaction;
    }

    @Override
    public boolean isOpen() {
        return this.isOpen.get();
    }

    @Override
    public void close() {
        if (isOpen.compareAndSet(true, false)) {
            transactions.keySet().parallelStream().forEach(RocksTransaction::close);
            keyspace.remove(this);
        }
    }
}
