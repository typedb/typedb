/*
 * Copyright (C) 2021 Grakn Labs
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

import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.concurrent.common.ConcurrentSet;
import grakn.core.concurrent.lock.ManagedReadWriteLock;
import grakn.core.graph.common.KeyGenerator;
import grakn.core.graph.common.Storage;
import org.rocksdb.AbstractImmutableNativeReference;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.OptimisticTransactionOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import static grakn.core.common.collection.Bytes.bytesHavePrefix;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_OPERATION;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Transaction.TRANSACTION_DATA_READ_VIOLATION;
import static grakn.core.common.exception.ErrorMessage.Transaction.TRANSACTION_SCHEMA_READ_VIOLATION;

public abstract class RocksStorage implements Storage {

    private static final Logger LOG = LoggerFactory.getLogger(RocksStorage.class);
    private static final byte[] EMPTY_ARRAY = new byte[]{};

    protected final ConcurrentSet<RocksIterator<?>> iterators;
    protected final Transaction storageTransaction;
    protected final ReadOptions readOptions;
    protected final boolean isReadOnly;

    private final ConcurrentLinkedQueue<org.rocksdb.RocksIterator> recycled;
    private final OptimisticTransactionOptions transactionOptions;
    private final WriteOptions writeOptions;
    private final AtomicBoolean isOpen;
    private final Snapshot snapshot;

    private RocksStorage(OptimisticTransactionDB rocksDB, boolean isReadOnly) {
        this.isReadOnly = isReadOnly;
        iterators = new ConcurrentSet<>();
        recycled = new ConcurrentLinkedQueue<>();
        writeOptions = new WriteOptions();
        transactionOptions = new OptimisticTransactionOptions().setSetSnapshot(true);
        storageTransaction = rocksDB.beginTransaction(writeOptions, transactionOptions);
        snapshot = storageTransaction.getSnapshot();
        readOptions = new ReadOptions().setSnapshot(snapshot);
        isOpen = new AtomicBoolean(true);
    }

    @Override
    public boolean isOpen() {
        return isOpen.get();
    }

    @Override
    public byte[] getLastKey(byte[] prefix) {
        throw exception(ILLEGAL_OPERATION);
    }

    @Override
    public void delete(byte[] key) {
        throw exception(ILLEGAL_OPERATION);
    }

    @Override
    public void put(byte[] key) {
        put(key, EMPTY_ARRAY);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        throw exception(ILLEGAL_OPERATION);
    }

    @Override
    public void putUntracked(byte[] key) {
        putUntracked(key, EMPTY_ARRAY);
    }

    @Override
    public void putUntracked(byte[] key, byte[] value) {
        throw exception(ILLEGAL_OPERATION);
    }

    @Override
    public void mergeUntracked(byte[] key, byte[] value) {
        throw exception(ILLEGAL_OPERATION);
    }

    org.rocksdb.RocksIterator getInternalRocksIterator() {
        if (isReadOnly) {
            org.rocksdb.RocksIterator iterator = recycled.poll();
            if (iterator != null) return iterator;
        }
        return storageTransaction.getIterator(readOptions);
    }

    void recycle(org.rocksdb.RocksIterator rocksIterator) {
        recycled.add(rocksIterator);
    }

    void remove(RocksIterator<?> iterator) {
        iterators.remove(iterator);
    }

    @Override
    public GraknException exception(ErrorMessage error) {
        GraknException e = GraknException.of(error);
        LOG.error(e.getMessage(), e);
        return e;
    }

    @Override
    public GraknException exception(Exception exception) {
        GraknException e;
        if (exception instanceof GraknException) e = (GraknException) exception;
        else e = GraknException.of(exception);
        LOG.error(e.getMessage(), e);
        return e;
    }

    @Override
    public void close() {
        if (isOpen.compareAndSet(true, false)) {
            iterators.parallelStream().forEach(RocksIterator::close);
            recycled.forEach(AbstractImmutableNativeReference::close);
            snapshot.close();
            storageTransaction.close();
            transactionOptions.close();
            readOptions.close();
            writeOptions.close();
        }
    }

    static class Cache extends RocksStorage {

        public Cache(OptimisticTransactionDB rocksDB) {
            super(rocksDB, true);
        }

        @Override
        public byte[] get(byte[] key) {
            assert isOpen();
            try {
                return storageTransaction.get(readOptions, key);
            } catch (RocksDBException e) {
                throw exception(e);
            }
        }

        @Override
        public <G> ResourceIterator<G> iterate(byte[] key, BiFunction<byte[], byte[], G> constructor) {
            assert isOpen();
            RocksIterator<G> iterator = new RocksIterator<>(this, key, constructor);
            iterators.add(iterator);
            return iterator.onFinalise(iterator::close);
        }
    }

    static abstract class TransactionBounded extends RocksStorage {

        protected final ManagedReadWriteLock readWriteLock;
        protected final RocksTransaction transaction;

        TransactionBounded(OptimisticTransactionDB rocksDB, RocksTransaction transaction) {
            super(rocksDB, transaction.type().isRead());
            this.transaction = transaction;
            readWriteLock = new ManagedReadWriteLock();
        }

        @Override
        public byte[] get(byte[] key) {
            assert isOpen();
            try {
                if (!isReadOnly) readWriteLock.lockRead();
                return storageTransaction.get(readOptions, key);
            } catch (RocksDBException | InterruptedException e) {
                throw exception(e);
            } finally {
                if (!isReadOnly) readWriteLock.unlockRead();
            }
        }

        @Override
        public byte[] getLastKey(byte[] prefix) {
            assert isOpen();
            byte[] upperBound = Arrays.copyOf(prefix, prefix.length);
            upperBound[upperBound.length - 1] = (byte) (upperBound[upperBound.length - 1] + 1);
            assert upperBound[upperBound.length - 1] != Byte.MIN_VALUE;

            try (org.rocksdb.RocksIterator iterator = getInternalRocksIterator()) {
                iterator.seekForPrev(upperBound);
                if (bytesHavePrefix(iterator.key(), prefix)) return iterator.key();
                else return null;
            }
        }

        @Override
        public void delete(byte[] key) {
            assert isOpen() && transaction.isOpen();
            if (isReadOnly) {
                if (transaction.isSchema()) throw exception(TRANSACTION_SCHEMA_READ_VIOLATION);
                else if (transaction.isData()) throw exception(TRANSACTION_DATA_READ_VIOLATION);
                else throw exception(ILLEGAL_STATE);
            }
            try {
                readWriteLock.lockWrite();
                storageTransaction.delete(key);
            } catch (RocksDBException | InterruptedException e) {
                throw exception(e);
            } finally {
                readWriteLock.unlockWrite();
            }
        }

        @Override
        public <G> ResourceIterator<G> iterate(byte[] key, BiFunction<byte[], byte[], G> constructor) {
            assert isOpen();
            RocksIterator<G> iterator = new RocksIterator<>(this, key, constructor);
            iterators.add(iterator);
            return iterator;
        }

        @Override
        public GraknException exception(ErrorMessage errorMessage) {
            transaction.close();
            return super.exception(errorMessage);
        }

        @Override
        public GraknException exception(Exception exception) {
            transaction.close();
            return super.exception(exception);
        }

        public void commit() throws RocksDBException {
            // We disable RocksDB indexing of uncommitted writes, as we're only about to write and never again reading
            // TODO: We should benchmark this
            storageTransaction.disableIndexing();
            storageTransaction.commit();
        }

        public void rollback() throws RocksDBException {
            storageTransaction.rollback();
        }
    }

    public static class Schema extends TransactionBounded implements Storage.Schema {

        private final KeyGenerator.Schema schemaKeyGenerator;

        public Schema(RocksDatabase database, RocksTransaction transaction) {
            super(database.rocksSchema, transaction);
            this.schemaKeyGenerator = database.schemaKeyGenerator();
        }

        @Override
        public KeyGenerator.Schema schemaKeyGenerator() {
            return schemaKeyGenerator;
        }

        @Override
        public void put(byte[] key, byte[] value) {
            assert isOpen() && !isReadOnly;
            try {
                if (transaction.isOpen()) readWriteLock.lockWrite();
                storageTransaction.put(key, value);
            } catch (RocksDBException | InterruptedException e) {
                throw exception(e);
            } finally {
                if (transaction.isOpen()) readWriteLock.unlockWrite();
            }
        }

        @Override
        public void putUntracked(byte[] key, byte[] value) {
            assert isOpen() && !isReadOnly;
            try {
                if (transaction.isOpen()) readWriteLock.lockWrite();
                storageTransaction.putUntracked(key, value);
            } catch (RocksDBException | InterruptedException e) {
                throw exception(e);
            } finally {
                if (transaction.isOpen()) readWriteLock.unlockWrite();
            }
        }
    }

    public static class Data extends TransactionBounded implements Storage.Data {

        private final KeyGenerator.Data dataKeyGenerator;

        public Data(RocksDatabase database, RocksTransaction transaction) {
            super(database.rocksData, transaction);
            this.dataKeyGenerator = database.dataKeyGenerator();
        }

        @Override
        public KeyGenerator.Data dataKeyGenerator() {
            return dataKeyGenerator;
        }

        @Override
        public void put(byte[] key, byte[] value) {
            assert isOpen() && !isReadOnly;
            try {
                storageTransaction.put(key, value);
            } catch (RocksDBException e) {
                throw exception(e);
            }
        }

        @Override
        public void putUntracked(byte[] key, byte[] value) {
            assert isOpen() && !isReadOnly;
            try {
                storageTransaction.putUntracked(key, value);
            } catch (RocksDBException e) {
                throw exception(e);
            }
        }

        @Override
        public void mergeUntracked(byte[] key, byte[] value) {
            assert isOpen() && !isReadOnly;
            try {
                storageTransaction.mergeUntracked(key, value);
            } catch (RocksDBException e) {
                throw exception(e);
            }
        }
    }
}
