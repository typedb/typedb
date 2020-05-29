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
import hypergraph.common.concurrent.ManagedReadWriteLock;
import hypergraph.common.exception.HypergraphException;
import hypergraph.concept.Concepts;
import hypergraph.graph.Graphs;
import hypergraph.graph.util.AttributeSync;
import hypergraph.graph.util.KeyGenerator;
import hypergraph.graph.util.Storage;
import hypergraph.traversal.Traversal;
import org.rocksdb.OptimisticTransactionOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import static hypergraph.common.collection.Bytes.bytesHavePrefix;

class CoreTransaction implements Hypergraph.Transaction {

    private final CoreSession session;
    private final OptimisticTransactionOptions optOptions;
    private final WriteOptions writeOptions;
    private final ReadOptions readOptions;
    private final Transaction rocksTransaction;
    private final Type type;
    private final CoreStorage storage;
    private final Graphs graph;
    private final Concepts concepts;
    private final Traversal traversal;
    private final AtomicBoolean isOpen;

    private static final byte[] EMPTY_ARRAY = new byte[]{};
    private final long snapshot;

    CoreTransaction(CoreSession session, Type type) {
        this.type = type;
        this.session = session;

        readOptions = new ReadOptions();
        writeOptions = new WriteOptions();
        optOptions = new OptimisticTransactionOptions().setSetSnapshot(true);
        rocksTransaction = session.rocks().beginTransaction(writeOptions, optOptions);
        readOptions.setSnapshot(rocksTransaction.getSnapshot());
        snapshot = readOptions.snapshot().getSequenceNumber();

        storage = new CoreStorage();
        graph = new Graphs(storage);
        concepts = new Concepts(graph);
        traversal = new Traversal(concepts);

        isOpen = new AtomicBoolean();
        isOpen.set(true);
    }

    Graphs graph() {
        return graph;
    }

    Long snapshot() {
        return snapshot;
    }

    public CoreStorage storage() {
        return storage;
    }

    @Override
    public Type type() {
        return type;
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
    public Concepts concepts() {
        return concepts;
    }

    /**
     * Commits any writes captured in the transaction into storage.
     *
     * If the transaction was opened as a {@code READ} transaction, then this
     * operation will throw an exception. If this transaction has been committed,
     * it cannot be committed again. If it has not been committed, then it will
     * flush all changes in the graph into storage by calling {@code graph.commit()},
     * which may result in acquiring a lock on the storage to confirm that the data
     * will be committed into storage. The operation will then continue to commit
     * all the writes into RocksDB by calling {@code rocksTransaction.commit()}.
     * If the operation reaches this state, then the RocksDB commit was successful.
     * We then need let go of the transaction that this resources of hold.
     *
     * If a lock was acquired from calling {@code graph.commit()} then we should
     * let inform the graph by confirming whether the RocksDB commit was successful
     * or not.
     */
    @Override
    public void commit() {
        if (type.equals(Type.READ)) {
            throw new HypergraphException("Illegal Write Exception");
        } else if (isOpen.compareAndSet(true, false)) {
            boolean locking = false;
            long newSnapshot = -1;
            try {
                locking = graph.commit();
                rocksTransaction.commit();
                newSnapshot = session.rocks().getLatestSequenceNumber();
                assert newSnapshot > this.snapshot;
            } catch (RocksDBException e) {
                throw new HypergraphException(e);
            } finally {
                if (locking) graph.confirm(newSnapshot > this.snapshot, newSnapshot);
                closeResources();
            }
        } else {
            throw new HypergraphException("Invalid Commit Exception");
        }
    }

    @Override
    public void rollback() {
        try {
            graph.clear();
            rocksTransaction.rollback();
        } catch (RocksDBException e) {
            throw new HypergraphException(e);
        }
    }

    @Override
    public void close() {
        if (isOpen.compareAndSet(true, false)) {
            closeResources();
        }
    }

    private void closeResources() {
        storage.close();
        optOptions.close();
        writeOptions.close();
        readOptions.close();
        rocksTransaction.close();
        session.remove(this);
    }

    class CoreStorage implements Storage {

        private final ManagedReadWriteLock readWriteLock;
        private final Set<CoreIterator<?>> iterators;

        CoreStorage() {
            readWriteLock = new ManagedReadWriteLock();
            iterators = ConcurrentHashMap.newKeySet();
        }

        @Override
        public long snapshot() {
            return snapshot;
        }

        @Override
        public KeyGenerator keyGenerator() {
            return session.keyGenerator();
        }

        @Override
        public AttributeSync attributeSync() {
            return session.attributeSync();
        }

        @Override
        public byte[] get(byte[] key) {
            try {
                if (type.isWrite()) readWriteLock.lockRead();
                return rocksTransaction.get(readOptions, key);
            } catch (RocksDBException | InterruptedException e) {
                throw new HypergraphException(e);
            } finally {
                if (type.isWrite()) readWriteLock.unlockRead();
            }
        }

        @Override
        public byte[] getLastKey(byte[] prefix) {
            byte[] upperBound = Arrays.copyOf(prefix, prefix.length);
            upperBound[upperBound.length-1] = (byte) (upperBound[upperBound.length-1] + 1);
            assert upperBound[upperBound.length-1] != Byte.MIN_VALUE;

            try (RocksIterator iterator = newRocksIterator()) {
                iterator.seekForPrev(upperBound);
                if (bytesHavePrefix(iterator.key(), prefix)) return iterator.key();
                else return null;
            }
        }

        @Override
        public void delete(byte[] key) {
            try {
                readWriteLock.lockWrite();
                rocksTransaction.delete(key);
            } catch (RocksDBException | InterruptedException e) {
                throw new HypergraphException(e);
            } finally {
                readWriteLock.unlockWrite();
            }
        }

        @Override
        public void put(byte[] key) {
            put(key, EMPTY_ARRAY);
        }

        @Override
        public void put(byte[] key, byte[] value) {
            try {
                readWriteLock.lockWrite();
                rocksTransaction.put(key, value);
            } catch (RocksDBException | InterruptedException e) {
                throw new HypergraphException(e);
            } finally {
                readWriteLock.unlockWrite();
            }
        }

        @Override
        public <G> Iterator<G> iterate(byte[] key, BiFunction<byte[], byte[], G> constructor) {
            CoreIterator<G> iterator = new CoreIterator<>(this, key, constructor);
            iterators.add(iterator);
            return iterator;
        }

        RocksIterator newRocksIterator() {
            return rocksTransaction.getIterator(readOptions);
        }

        void remove(CoreIterator<?> iterator) {
            iterators.remove(iterator);
        }

        void close() {
            iterators.parallelStream().forEach(CoreIterator::close);
        }
    }
}
