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

import com.vaticle.typedb.common.collection.ConcurrentSet;
import com.vaticle.typedb.core.common.exception.ErrorMessage;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.graph.common.KeyGenerator;
import com.vaticle.typedb.core.graph.common.Storage;
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

import javax.annotation.concurrent.NotThreadSafe;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiFunction;

import static com.vaticle.typedb.core.common.collection.Bytes.bytesHavePrefix;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_OPERATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_DATA_READ_VIOLATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_SCHEMA_READ_VIOLATION;

public abstract class RocksStorage implements Storage {

    private static final Logger LOG = LoggerFactory.getLogger(RocksStorage.class);
    static final byte[] EMPTY_ARRAY = new byte[]{};

    protected final ConcurrentSet<RocksIterator<?>> iterators;
    protected final Transaction storageTransaction;
    protected final ReadWriteLock deleteCloseSchemaWriteLock;
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
        deleteCloseSchemaWriteLock = new StampedLock().asReadWriteLock();
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
    public void mergeUntracked(byte[] key, byte[] value) {
        throw exception(ILLEGAL_OPERATION);
    }

    org.rocksdb.RocksIterator getInternalRocksIterator() {
        org.rocksdb.RocksIterator iterator = recycled.poll();
        if (iterator != null) return iterator;
        else return storageTransaction.getIterator(readOptions);
    }

    void recycle(org.rocksdb.RocksIterator rocksIterator) {
        recycled.add(rocksIterator);
    }

    void remove(RocksIterator<?> iterator) {
        iterators.remove(iterator);
    }

    @Override
    public TypeDBException exception(ErrorMessage error) {
        TypeDBException e = TypeDBException.of(error);
        LOG.debug(e.getMessage(), e);
        return e;
    }

    @Override
    public TypeDBException exception(Exception exception) {
        TypeDBException e;
        if (exception instanceof TypeDBException) e = (TypeDBException) exception;
        else e = TypeDBException.of(exception);
        LOG.debug(e.getMessage(), e);
        return e;
    }

    @Override
    public void close() {
        try {
            deleteCloseSchemaWriteLock.writeLock().lock();
            if (isOpen.compareAndSet(true, false)) {
                iterators.parallelStream().forEach(RocksIterator::close);
                recycled.forEach(AbstractImmutableNativeReference::close);
                snapshot.close();
                storageTransaction.close();
                transactionOptions.close();
                readOptions.close();
                writeOptions.close();
            }
        } finally {
            deleteCloseSchemaWriteLock.writeLock().unlock();
        }
    }

    static class Cache extends RocksStorage {

        public Cache(OptimisticTransactionDB rocksDB) {
            super(rocksDB, true);
        }

        @Override
        public byte[] get(byte[] key) {
            try {
                deleteCloseSchemaWriteLock.readLock().lock();
                if (!isOpen()) throw TypeDBException.of(RESOURCE_CLOSED);
                return storageTransaction.get(readOptions, key);
            } catch (RocksDBException e) {
                throw exception(e);
            } finally {
                deleteCloseSchemaWriteLock.readLock().unlock();
            }
        }

        @Override
        public <G> FunctionalIterator<G> iterate(byte[] key, BiFunction<byte[], byte[], G> constructor) {
            RocksIterator<G> iterator = new RocksIterator<>(this, key, constructor);
            iterators.add(iterator);
            if (!isOpen()) throw TypeDBException.of(RESOURCE_CLOSED); //guard against close() race conditions
            return iterator.onFinalise(iterator::close);
        }
    }

    static abstract class TransactionBounded extends RocksStorage {

        protected final RocksTransaction transaction;

        TransactionBounded(OptimisticTransactionDB rocksDB, RocksTransaction transaction) {
            super(rocksDB, transaction.type().isRead());
            this.transaction = transaction;
        }

        @Override
        public byte[] get(byte[] key) {
            try {
                deleteCloseSchemaWriteLock.readLock().lock();
                if (!isOpen()) throw TypeDBException.of(RESOURCE_CLOSED);
                return storageTransaction.get(readOptions, key);
            } catch (RocksDBException e) {
                throw exception(e);
            } finally {
                deleteCloseSchemaWriteLock.readLock().unlock();
            }
        }

        @Override
        public byte[] getLastKey(byte[] prefix) {
            assert isOpen();
            byte[] upperBound = Arrays.copyOf(prefix, prefix.length);
            upperBound[upperBound.length - 1] = (byte) (upperBound[upperBound.length - 1] + 1);
            assert upperBound[upperBound.length - 1] != Byte.MIN_VALUE;

            try (org.rocksdb.RocksIterator iterator = getInternalRocksIterator()) {
                deleteCloseSchemaWriteLock.readLock().lock();
                if (!isOpen()) throw TypeDBException.of(RESOURCE_CLOSED);
                iterator.seekForPrev(upperBound);
                if (bytesHavePrefix(iterator.key(), prefix)) return iterator.key();
                else return null;
            } finally {
                deleteCloseSchemaWriteLock.readLock().unlock();
            }
        }

        @Override
        public void delete(byte[] key) {
            if (isReadOnly) {
                if (transaction.isSchema()) throw exception(TRANSACTION_SCHEMA_READ_VIOLATION);
                else if (transaction.isData()) throw exception(TRANSACTION_DATA_READ_VIOLATION);
                else throw exception(ILLEGAL_STATE);
            }
            try {
                deleteCloseSchemaWriteLock.writeLock().lock();
                if (!isOpen() || (!transaction.isOpen() && transaction.isData())) {
                    throw TypeDBException.of(RESOURCE_CLOSED);
                }
                storageTransaction.deleteUntracked(key);
            } catch (RocksDBException e) {
                throw exception(e);
            } finally {
                deleteCloseSchemaWriteLock.writeLock().unlock();
            }
        }

        @Override
        public <G> FunctionalIterator<G> iterate(byte[] key, BiFunction<byte[], byte[], G> constructor) {
            RocksIterator<G> iterator = new RocksIterator<>(this, key, constructor);
            iterators.add(iterator);
            if (!isOpen()) throw TypeDBException.of(RESOURCE_CLOSED); //guard against close() race conditions
            return iterator;
        }


        @Override
        public TypeDBException exception(ErrorMessage errorMessage) {
            transaction.close();
            return super.exception(errorMessage);
        }

        @Override
        public TypeDBException exception(Exception exception) {
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
            boolean obtainedWriteLock = false;
            try {
                if (transaction.isOpen()) {
                    deleteCloseSchemaWriteLock.writeLock().lock();
                    obtainedWriteLock = true;
                }
                if (!isOpen()) throw TypeDBException.of(RESOURCE_CLOSED);
                storageTransaction.putUntracked(key, value);
            } catch (RocksDBException e) {
                throw exception(e);
            } finally {
                if (obtainedWriteLock) deleteCloseSchemaWriteLock.writeLock().unlock();
            }
        }
    }

    @NotThreadSafe
    public static class Data extends TransactionBounded implements Storage.Data {

        private final RocksDatabase database;
        private final KeyGenerator.Data dataKeyGenerator;

        private final ConcurrentNavigableMap<ByteBuffer, Boolean> modifiedKeys;
        private final ConcurrentSkipListSet<ByteBuffer> deletedKeys;
        private final ConcurrentSkipListSet<ByteBuffer> exclusiveInsertKeys;
        private final long snapshotStart;
        private volatile Long snapshotEnd;

        public Data(RocksDatabase database, RocksTransaction transaction) {
            super(database.rocksData, transaction);
            this.database = database;
            this.dataKeyGenerator = database.dataKeyGenerator();
            this.snapshotStart = storageTransaction.getSnapshot().getSequenceNumber();
            this.modifiedKeys = new ConcurrentSkipListMap<>();
            this.deletedKeys = new ConcurrentSkipListSet<>();
            this.exclusiveInsertKeys = new ConcurrentSkipListSet<>();
            this.snapshotEnd = null;
            this.database.writesManager().register(this);
        }

        @Override
        public KeyGenerator.Data dataKeyGenerator() {
            return dataKeyGenerator;
        }

        @Override
        public void put(byte[] key, byte[] value) {
            put(key, value, true);
        }

        @Override
        public void put(byte[] key, byte[] value, boolean checkConsistency) {
            putUntracked(key, value);
            setModified(key, checkConsistency);
        }

        @Override
        public void putUntracked(byte[] key) {
            putUntracked(key, EMPTY_ARRAY);
        }

        @Override
        public void putUntracked(byte[] key, byte[] value) {
            assert isOpen() && !isReadOnly;
            try {
                deleteCloseSchemaWriteLock.readLock().lock();
                if (!isOpen()) throw TypeDBException.of(RESOURCE_CLOSED);
                storageTransaction.putUntracked(key, value);
            } catch (RocksDBException e) {
                throw exception(e);
            } finally {
                deleteCloseSchemaWriteLock.readLock().unlock();
            }
        }

        @Override
        public void delete(byte[] key) {
            deleteUntracked(key);
            ByteBuffer bytes = ByteBuffer.wrap(key);
            this.deletedKeys.add(bytes);
            this.modifiedKeys.remove(bytes);
            this.exclusiveInsertKeys.remove(bytes);
        }

        @Override
        public void deleteUntracked(byte[] key) {
            super.delete(key);
        }

        @Override
        public void setModified(byte[] key, boolean checkConsistency) {
            assert isOpen();
            ByteBuffer bytes = ByteBuffer.wrap(key);
            this.modifiedKeys.put(bytes, checkConsistency);
            this.deletedKeys.remove(bytes);
        }

        @Override
        public void setExclusiveCreate(byte[] key) {
            assert isOpen();
            ByteBuffer bytes = ByteBuffer.wrap(key);
            this.exclusiveInsertKeys.add(bytes);
            this.deletedKeys.remove(bytes);
        }

        @Override
        public void commit() throws RocksDBException {
            database.writesManager().tryOptimisticCommit(this);
            super.commit();
            snapshotEnd = database.rocksData.getLatestSequenceNumber();
            database.writesManager().committed(this);
        }

        @Override
        public void close() {
            super.close();
            database.writesManager().closed(this);
        }

        @Override
        public void mergeUntracked(byte[] key, byte[] value) {
            assert isOpen() && !isReadOnly;
            try {
                deleteCloseSchemaWriteLock.readLock().lock();
                if (!isOpen()) throw TypeDBException.of(RESOURCE_CLOSED);
                storageTransaction.mergeUntracked(key, value);
            } catch (RocksDBException e) {
                throw exception(e);
            } finally {
                deleteCloseSchemaWriteLock.readLock().unlock();
            }
        }

        public long snapshotStart() {
            return snapshotStart;
        }

        public Long snapshotEnd() {
            return snapshotEnd;
        }

        public NavigableSet<ByteBuffer> deletedKeys() {
            return deletedKeys;
        }

        public NavigableSet<ByteBuffer> modifiedKeys() {
            return modifiedKeys.keySet();
        }

        public boolean isModifiedValidatedKey(ByteBuffer key) {
            return modifiedKeys.getOrDefault(key, false);
        }

        public NavigableSet<ByteBuffer> exclusiveInsertKeys() {
            return exclusiveInsertKeys;
        }
    }
}
