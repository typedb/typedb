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
import hypergraph.graph.KeyGenerator;
import hypergraph.graph.Storage;
import hypergraph.traversal.Traversal;
import org.rocksdb.OptimisticTransactionOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

import java.util.concurrent.atomic.AtomicBoolean;

class CoreTransaction implements Hypergraph.Transaction {

    private final CoreSession session;
    private final OptimisticTransactionOptions optOptions;
    private final WriteOptions writeOptions;
    private final ReadOptions readOptions;
    private final Transaction rocksTransaction;
    private final Type type;
    private final GraphManager graph;
    private final ConceptManager concepts;
    private final Traversal traversal;
    private final AtomicBoolean isOpen;

    private static final byte[] EMPTY_ARRAY = new byte[]{};

    CoreTransaction(CoreSession session, Type type) {
        this.type = type;
        this.session = session;

        readOptions = new ReadOptions();
        writeOptions = new WriteOptions();
        optOptions = new OptimisticTransactionOptions().setSetSnapshot(true);
        rocksTransaction = session.rocks().beginTransaction(writeOptions, optOptions);
        readOptions.setSnapshot(rocksTransaction.getSnapshot());

        graph = new GraphManager(new CoreStorage());
        concepts = new ConceptManager(graph);
        traversal = new Traversal(concepts);

        isOpen = new AtomicBoolean();
        isOpen.set(true);
    }

    GraphManager graph() {
        return graph;
    }

    @Override
    public boolean isOpen() {
        return this.isOpen.get();
    }

    @Override
    public Traversal traversal() {
        return traversal;
    }

    @Override
    public ConceptManager concepts() {
        return concepts;
    }

    @Override
    public void commit() {
        if (type.equals(Type.READ)) throw new HypergraphException("Illegal Write Exception");

        try {
            graph.persist();
            rocksTransaction.commit();
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new HypergraphException(e);
        }
        close();
    }

    @Override
    public void rollback() {
        try {
            graph.reset();
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
            optOptions.close();
            writeOptions.close();
            readOptions.close();
            rocksTransaction.close();
        }
    }

    private class CoreStorage implements Storage {

        private final AtomicBoolean isWriting;

        CoreStorage() {
            isWriting = new AtomicBoolean(false);
        }

        @Override
        public KeyGenerator keyGenerator() {
            return session.keyGenerator();
        }

        @Override
        public byte[] get(byte[] key) {
            try {
                return session.rocks().get(readOptions, key);
            } catch (RocksDBException e) {
                e.printStackTrace();
                throw new HypergraphException(e);
            }
        }

        @Override
        public void put(byte[] key) {
            put(key, EMPTY_ARRAY);
        }

        @Override
        public void put(byte[] key, byte[] value) {
            if (!isWriting.compareAndSet(false, true)) {
                throw new HypergraphException("Attempted multiple access to PUT operation concurrently");
            }

            try {
                rocksTransaction.put(key, value);
            } catch (RocksDBException e) {
                e.printStackTrace();
                throw new HypergraphException(e);
            }

            isWriting.set(false);
        }
    }
}
