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

package hypergraph.core;

import hypergraph.Hypergraph;
import hypergraph.storage.Index;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.OptimisticTransactionOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

class CoreSession implements Hypergraph.Session {

    private final CoreKeyspace keyspace;
    private final OptimisticTransactionDB rocksSession;
    private final List<CoreTransaction> transactions;
    private final AtomicBoolean isOpen;

    CoreSession(CoreKeyspace keyspace, OptimisticTransactionDB rocksSession) {
        this.keyspace = keyspace;
        this.rocksSession = rocksSession;

        transactions = new ArrayList<>();
        isOpen = new AtomicBoolean();
        isOpen.set(true);
    }

    Index getIndex() {
        return keyspace.index();
    }

    @Override
    public CoreKeyspace keyspace() {
        return keyspace;
    }

    @Override
    public CoreTransaction transaction(Hypergraph.Transaction.Type type) {
        ReadOptions readOptions = new ReadOptions();
        WriteOptions writeOptions = new WriteOptions();
        Transaction rocksTransaction = rocksSession.beginTransaction(
                writeOptions, new OptimisticTransactionOptions().setSetSnapshot(true)
        );
        readOptions.setSnapshot(rocksTransaction.getSnapshot());

        CoreTransaction transaction = new CoreTransaction(type, this, rocksTransaction, writeOptions, readOptions);
        transactions.add(transaction);
        return transaction;
    }

    @Override
    public boolean isOpen() {
        return this.isOpen.get();
    }

    @Override
    public void close() {
        if (isOpen.compareAndSet(true, false)) {
            for (CoreTransaction transaction : transactions) {
                transaction.close();
            }
            rocksSession.close();
        }
    }

}
