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

package com.vaticle.typedb.core.database;

import com.vaticle.typedb.common.collection.ConcurrentSet;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.collection.KeyValue;
import com.vaticle.typedb.core.common.exception.ErrorMessage;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;
import com.vaticle.typedb.core.graph.common.KeyGenerator;
import com.vaticle.typedb.core.graph.common.Storage;
import com.vaticle.typedb.core.graph.common.Storage.Key.Partition;
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
import java.util.Arrays;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.StampedLock;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_OPERATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_DATA_READ_VIOLATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_SCHEMA_READ_VIOLATION;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.ASC;
import static com.vaticle.typedb.core.graph.common.Encoding.System.TRANSACTION_DUMMY_WRITE;

public abstract class RocksStorage implements Storage {

    private static final Logger LOG = LoggerFactory.getLogger(RocksStorage.class);

    protected final Transaction rocksTransaction;
    // TODO: use a single read options when 'setAutoPrefixMode(true)' is available on ReadOptions API
    protected final ReadOptions readOptions;
    protected final ReadOptions readOptionsWithPrefixBloom;
    protected final CorePartitionManager partitionMgr;
    protected final Snapshot snapshot;
    protected final ReadWriteLock deleteCloseSchemaWriteLock;
    protected final ConcurrentSet<RocksIterator<?, ?>> iterators;
    // TODO: use a single set of iterators when 'setAutoPrefixMode(true)' is available on ReadOptions API
    protected final ConcurrentMap<Partition, ConcurrentLinkedQueue<org.rocksdb.RocksIterator>> recycled;
    protected final ConcurrentMap<Partition, ConcurrentLinkedQueue<org.rocksdb.RocksIterator>> recycledWithPrefixBloom;
    protected final boolean isReadOnly;
    private final OptimisticTransactionOptions transactionOptions;
    private final WriteOptions writeOptions;
    private final AtomicBoolean isOpen;

    private RocksStorage(OptimisticTransactionDB rocksDB, CorePartitionManager partitionMgr, boolean isReadOnly) {
        this.isReadOnly = isReadOnly;
        this.partitionMgr = partitionMgr;
        iterators = new ConcurrentSet<>();
        recycled = new ConcurrentHashMap<>();
        recycledWithPrefixBloom = new ConcurrentHashMap<>();
        partitionMgr.partitions().forEach(partition -> recycled.put(partition, new ConcurrentLinkedQueue<>()));
        partitionMgr.partitions().forEach(partition -> recycledWithPrefixBloom.put(partition, new ConcurrentLinkedQueue<>()));
        writeOptions = new WriteOptions();
        transactionOptions = new OptimisticTransactionOptions().setSetSnapshot(true);
        rocksTransaction = rocksDB.beginTransaction(writeOptions, transactionOptions);
        snapshot = rocksTransaction.getSnapshot();
        readOptions = new ReadOptions().setSnapshot(snapshot).setTotalOrderSeek(true);
        readOptionsWithPrefixBloom = new ReadOptions().setSnapshot(snapshot).setTotalOrderSeek(false);
        deleteCloseSchemaWriteLock = new StampedLock().asReadWriteLock();
        isOpen = new AtomicBoolean(true);
    }

    @Override
    public boolean isOpen() {
        return isOpen.get();
    }

    @Override
    public <T extends Key> T getLastKey(Key.Prefix<T> prefix) {
        throw exception(ILLEGAL_OPERATION);
    }

    @Override
    public void deleteUntracked(Key key) {
        throw exception(ILLEGAL_OPERATION);
    }

    @Override
    public void putUntracked(Key key) {
        putUntracked(key, ByteArray.empty());
    }

    @Override
    public void putUntracked(Key key, ByteArray value) {
        throw exception(ILLEGAL_OPERATION);
    }

    protected Logger logger() {
        return LOG;
    }

    org.rocksdb.RocksIterator getInternalRocksIterator(Partition partition, boolean usePrefixBloom) {
        if (usePrefixBloom) {
            org.rocksdb.RocksIterator iterator = recycledWithPrefixBloom.get(partition).poll();
            if (iterator != null) return iterator;
            else return rocksTransaction.getIterator(readOptionsWithPrefixBloom, partitionMgr.get(partition));
        } else {
            org.rocksdb.RocksIterator iterator = recycled.get(partition).poll();
            if (iterator != null) return iterator;
            else return rocksTransaction.getIterator(readOptions, partitionMgr.get(partition));
        }
    }

    <T extends Key, ORDER extends Order> RocksIterator<T, ORDER> createIterator(Key.Prefix<T> prefix, ORDER order) {
        RocksIterator<T, ORDER> iterator;
        // TODO how else can we convert an enumerated data tag ('order') into the type without casting
        if (order == ASC) iterator = (RocksIterator<T, ORDER>) new RocksIterator.Ascending<>(this, prefix);
        else iterator = (RocksIterator<T, ORDER>) new RocksIterator.Descending<>(this, prefix);
        iterators.add(iterator);
        if (!isOpen()) throw TypeDBException.of(RESOURCE_CLOSED); //guard against close() race conditions
        return iterator;
    }

    void recycle(RocksIterator<?, ?> rocksIterator) {
        if (rocksIterator.usePrefixBloom()) {
            recycledWithPrefixBloom.get(rocksIterator.partition()).add(rocksIterator.internalRocksIterator);
        } else {
            recycled.get(rocksIterator.partition()).add(rocksIterator.internalRocksIterator);
        }
    }

    void remove(RocksIterator<?, ?> iterator) {
        iterators.remove(iterator);
    }

    @Override
    public TypeDBException exception(ErrorMessage error) {
        TypeDBException e = TypeDBException.of(error);
        logger().debug(e.getMessage(), e);
        return e;
    }

    @Override
    public TypeDBException exception(Exception exception) {
        TypeDBException e;
        if (exception instanceof TypeDBException) e = (TypeDBException) exception;
        else e = TypeDBException.of(exception);
        logger().debug(e.getMessage(), e);
        return e;
    }

    @Override
    public void close() {
        try {
            deleteCloseSchemaWriteLock.writeLock().lock();
            if (isOpen.compareAndSet(true, false)) {
                iterators.parallelStream().forEach(RocksIterator::close);
                recycledWithPrefixBloom.values().forEach(iters -> iters.forEach(AbstractImmutableNativeReference::close));
                recycled.values().forEach(iters -> iters.forEach(AbstractImmutableNativeReference::close));
                rocksTransaction.close();
                snapshot.close();
                transactionOptions.close();
                readOptionsWithPrefixBloom.close();
                readOptions.close();
                writeOptions.close();
            }
        } finally {
            deleteCloseSchemaWriteLock.writeLock().unlock();
        }
    }

    static class Cache extends RocksStorage {

        Cache(OptimisticTransactionDB rocksDB, CorePartitionManager partitionMgr) {
            super(rocksDB, partitionMgr, true);
        }

        @Override
        public ByteArray get(Key key) {
            try {
                deleteCloseSchemaWriteLock.readLock().lock();
                if (!isOpen()) throw TypeDBException.of(RESOURCE_CLOSED);
                byte[] value = rocksTransaction.get(partitionMgr.get(key.partition()), readOptions, key.bytes().getBytes());
                if (value == null) return null;
                else return ByteArray.of(value);
            } catch (RocksDBException e) {
                throw exception(e);
            } finally {
                deleteCloseSchemaWriteLock.readLock().unlock();
            }
        }

        @Override
        public <T extends Key> SortedIterator.Forwardable<KeyValue<T, ByteArray>, Order.Asc> iterate(Key.Prefix<T> prefix) {
            return iterate(prefix, ASC);
        }

        @Override
        public <T extends Key, ORDER extends Order> SortedIterator.Forwardable<KeyValue<T, ByteArray>, ORDER> iterate(Key.Prefix<T> prefix, ORDER order) {
            RocksIterator<T, ORDER> iterator = createIterator(prefix, order);
            return iterator.onFinalise(iterator::close);
        }
    }

    static abstract class TransactionBounded extends RocksStorage {

        protected final CoreTransaction transaction;

        TransactionBounded(OptimisticTransactionDB rocksDB, CorePartitionManager partitionMgr, CoreTransaction transaction) {
            super(rocksDB, partitionMgr, transaction.type().isRead());
            this.transaction = transaction;
        }

        @Override
        public ByteArray get(Key key) {
            try {
                deleteCloseSchemaWriteLock.readLock().lock();
                if (!isOpen()) throw TypeDBException.of(RESOURCE_CLOSED);
                byte[] value = rocksTransaction.get(partitionMgr.get(key.partition()), readOptions, key.bytes().getBytes());
                if (value == null) return null;
                else return ByteArray.of(value);
            } catch (RocksDBException e) {
                throw exception(e);
            } finally {
                deleteCloseSchemaWriteLock.readLock().unlock();
            }
        }

        @Override
        public <T extends Key> T getLastKey(Key.Prefix<T> prefix) {
            assert isOpen();
            byte[] upperBound = Arrays.copyOf(prefix.bytes().getBytes(), prefix.bytes().length());
            upperBound[upperBound.length - 1] = (byte) (upperBound[upperBound.length - 1] + 1);
            assert upperBound[upperBound.length - 1] != Byte.MIN_VALUE;

            org.rocksdb.RocksIterator iterator;
            if (prefix.isFixedStartInPartition()) {
                iterator = rocksTransaction.getIterator(readOptionsWithPrefixBloom, partitionMgr.get(prefix.partition()));
            } else {
                iterator = rocksTransaction.getIterator(readOptions, partitionMgr.get(prefix.partition()));
            }
            try {
                deleteCloseSchemaWriteLock.readLock().lock();
                if (!isOpen()) throw TypeDBException.of(RESOURCE_CLOSED);
                iterator.seekForPrev(upperBound);
                if (!iterator.isValid()) return null;
                byte[] key = iterator.key();
                ByteArray array;
                if (key != null && (array = ByteArray.of(key)).hasPrefix(prefix.bytes())) {
                    return prefix.builder().build(array);
                } else return null;
            } finally {
                iterator.close();
                deleteCloseSchemaWriteLock.readLock().unlock();
            }
        }

        @Override
        public void deleteUntracked(Key key) {
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
                rocksTransaction.deleteUntracked(partitionMgr.get(key.partition()), key.bytes().getBytes());
            } catch (RocksDBException e) {
                throw exception(e);
            } finally {
                deleteCloseSchemaWriteLock.writeLock().unlock();
            }
        }

        @Override
        public <T extends Key> SortedIterator.Forwardable<KeyValue<T, ByteArray>, Order.Asc> iterate(Key.Prefix<T> prefix) {
            return iterate(prefix, ASC);
        }

        @Override
        public <T extends Key, ORDER extends Order>
        SortedIterator.Forwardable<KeyValue<T, ByteArray>, ORDER> iterate(Key.Prefix<T> prefix, ORDER order) {
            return createIterator(prefix, order);
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
            rocksTransaction.disableIndexing();
            rocksTransaction.commit();
        }

        public void rollback() throws RocksDBException {
            rocksTransaction.rollback();
        }
    }

    public static class Schema extends TransactionBounded implements Storage.Schema {

        private final KeyGenerator.Schema schemaKeyGenerator;

        public Schema(CoreDatabase database, CoreTransaction transaction) {
            super(database.rocksSchema, database.rocksSchemaPartitionMgr, transaction);
            this.schemaKeyGenerator = database.schemaKeyGenerator();
        }

        @Override
        public KeyGenerator.Schema schemaKeyGenerator() {
            return schemaKeyGenerator;
        }

        @Override
        public void putUntracked(Key key, ByteArray value) {
            assert isOpen() && !isReadOnly;
            boolean obtainedWriteLock = false;
            try {
                if (transaction.isOpen()) {
                    deleteCloseSchemaWriteLock.writeLock().lock();
                    obtainedWriteLock = true;
                }
                if (!isOpen()) throw TypeDBException.of(RESOURCE_CLOSED);
                rocksTransaction.putUntracked(partitionMgr.get(key.partition()), key.bytes().getBytes(), value.getBytes());
            } catch (RocksDBException e) {
                throw exception(e);
            } finally {
                if (obtainedWriteLock) deleteCloseSchemaWriteLock.writeLock().unlock();
            }
        }
    }

    @NotThreadSafe
    public static class Data extends TransactionBounded implements Storage.Data {

        private final CoreDatabase database;
        private final KeyGenerator.Data dataKeyGenerator;

        private final ConcurrentNavigableMap<ByteArray, Boolean> modifiedKeys;
        private final ConcurrentSkipListSet<ByteArray> deletedKeys;
        private final ConcurrentSkipListSet<ByteArray> exclusiveBytes; // these are not real keys, just reserved bytes
        private final long snapshotStart;
        private volatile Long snapshotEnd;
        private boolean hasWrite;

        public Data(CoreDatabase database, CoreTransaction transaction) {
            super(database.rocksData, database.rocksDataPartitionMgr, transaction);
            this.database = database;
            this.dataKeyGenerator = database.dataKeyGenerator();
            this.snapshotStart = snapshot.getSequenceNumber();
            this.modifiedKeys = new ConcurrentSkipListMap<>();
            this.deletedKeys = new ConcurrentSkipListSet<>();
            this.exclusiveBytes = new ConcurrentSkipListSet<>();
            this.snapshotEnd = null;
            this.hasWrite = false;
            if (transaction.type().isWrite()) this.database.consistencyMgr().register(this);
        }

        @Override
        public KeyGenerator.Data dataKeyGenerator() {
            return dataKeyGenerator;
        }

        @Override
        public void putTracked(Key key) {
            putTracked(key, ByteArray.empty());
        }

        @Override
        public void putTracked(Key key, ByteArray value) {
            putTracked(key, value, true);
        }

        @Override
        public void putTracked(Key key, ByteArray value, boolean checkConsistency) {
            putUntracked(key, value);
            trackModified(key.bytes(), checkConsistency);
        }

        @Override
        public void putUntracked(Key key) {
            putUntracked(key, ByteArray.empty());
        }

        @Override
        public void putUntracked(Key key, ByteArray value) {
            assert isOpen() && !isReadOnly;
            try {
                deleteCloseSchemaWriteLock.readLock().lock();
                if (!isOpen()) throw TypeDBException.of(RESOURCE_CLOSED);
                rocksTransaction.putUntracked(partitionMgr.get(key.partition()), key.bytes().getBytes(), value.getBytes());
            } catch (RocksDBException e) {
                throw exception(e);
            } finally {
                deleteCloseSchemaWriteLock.readLock().unlock();
            }
            hasWrite = true;
        }

        @Override
        public void deleteTracked(Key key) {
            deleteUntracked(key);
            this.deletedKeys.add(key.bytes());
            this.modifiedKeys.remove(key.bytes());
        }

        @Override
        public void deleteUntracked(Key key) {
            super.deleteUntracked(key);
            hasWrite = true;
        }

        @Override
        public void trackModified(ByteArray key) {
            trackModified(key, true);
        }

        @Override
        public void trackModified(ByteArray key, boolean checkConsistency) {
            assert isOpen();
            this.modifiedKeys.put(key, checkConsistency);
            this.deletedKeys.remove(key);
        }

        @Override
        public void trackExclusiveBytes(ByteArray bytes) {
            assert isOpen();
            this.exclusiveBytes.add(bytes);
        }

        @Override
        public void commit() throws RocksDBException {
            database.consistencyMgr().validateAndBeginCommit(this);
            if (!hasWrite) {
                // guarantee at least 1 write per tx to ensure we get a snapshotEnd greater than the start
                rocksTransaction.putUntracked(
                        partitionMgr.get(Partition.DEFAULT),
                        TRANSACTION_DUMMY_WRITE.bytes().getBytes(),
                        ByteArray.empty().getBytes()
                );
            }
            super.commit();
            snapshotEnd = database.rocksData.getLatestSequenceNumber();
            database.consistencyMgr().endCommit(this);
        }

        @Override
        public void close() {
            super.close();
            if (transaction.type().isWrite()) database.consistencyMgr().closed(this);
        }

        @Override
        public void mergeUntracked(Key key, ByteArray value) {
            assert isOpen() && !isReadOnly;
            try {
                deleteCloseSchemaWriteLock.readLock().lock();
                if (!isOpen()) throw TypeDBException.of(RESOURCE_CLOSED);
                rocksTransaction.mergeUntracked(partitionMgr.get(key.partition()), key.bytes().getBytes(), value.getBytes());
            } catch (RocksDBException e) {
                throw exception(e);
            } finally {
                deleteCloseSchemaWriteLock.readLock().unlock();
            }
            hasWrite = true;
        }

        public long snapshotStart() {
            return snapshotStart;
        }

        public Optional<Long> snapshotEnd() {
            return Optional.ofNullable(snapshotEnd);
        }

        public NavigableSet<ByteArray> deletedKeys() {
            return deletedKeys;
        }

        public NavigableSet<ByteArray> modifiedKeys() {
            return modifiedKeys.keySet();
        }

        public boolean isModifiedValidatedKey(ByteArray key) {
            return modifiedKeys.getOrDefault(key, false);
        }

        public NavigableSet<ByteArray> exclusiveBytes() {
            return exclusiveBytes;
        }
    }
}
