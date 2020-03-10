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
import hypergraph.common.HypergraphException;
import hypergraph.concept.ConceptManager;
import hypergraph.graph.GraphManager;
import hypergraph.storage.Index;
import hypergraph.storage.Storage;
import hypergraph.traversal.Traversal;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

import java.util.concurrent.atomic.AtomicBoolean;

class CoreTransaction implements Hypergraph.Transaction {

    private final CoreSession session;
    private final WriteOptions writeOptions;
    private final ReadOptions readOptions;
    private final Transaction rocksTransaction;
    private final Type type;
    private final Storage storage;
    private final ConceptManager concepts;
    private final Traversal traversal;
    private final AtomicBoolean isOpen;

    CoreTransaction(Type type, CoreSession session, Transaction rocksTransaction, WriteOptions writeOptions, ReadOptions readOptions) {
        this.type = type;
        this.session = session;
        this.rocksTransaction = rocksTransaction;
        this.writeOptions = writeOptions;
        this.readOptions = readOptions;

        storage = new Storage(new CoreOperation());
        concepts = new ConceptManager(new GraphManager(storage));
        traversal = new Traversal(concepts);

        isOpen = new AtomicBoolean();
        isOpen.set(true);
    }

    @Override
    public boolean isOpen() {
        return this.isOpen.get();
    }

    @Override
    public Traversal read() {
        return traversal;
    }

    @Override
    public ConceptManager write() {
        return concepts;
    }

    @Override
    public void commit() {
        if (type.equals(Type.READ)) {
            throw new HypergraphException("Illegal Write Exception");
        }
        try {
            rocksTransaction.commit();
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new HypergraphException(e);
        }
    }

    @Override
    public void rollback() {
        try {
            rocksTransaction.rollback();
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new HypergraphException(e);
        }
    }

    @Override
    public Type type() {
        return this.type;
    }

    @Override
    public void close() {
        if (isOpen.compareAndSet(true, false)) {
            storage.persist();
            rocksTransaction.close();
            writeOptions.close();
        }
    }

    private class CoreOperation implements hypergraph.storage.Operation {

        @Override
        public Index getIndex() {
            return session.getIndex();
        }

        @Override
        public byte[] get(byte[] key) {
            try {
                return rocksTransaction.get(readOptions, key);
            } catch (RocksDBException e) {
                e.printStackTrace();
                throw new HypergraphException(e);
            }
        }

        @Override
        public void put(byte[] key, byte[] value) {
            try {
                rocksTransaction.put(key, value);
            } catch (RocksDBException e) {
                e.printStackTrace();
                throw new HypergraphException(e);
            }
        }
    }
}
