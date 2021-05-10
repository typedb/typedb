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

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.graph.common.KeyGenerator;
import com.vaticle.typedb.core.graph.common.Storage;
import org.rocksdb.RocksDBException;

import javax.annotation.concurrent.NotThreadSafe;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;

@NotThreadSafe
public class RocksDataStorage extends RocksStorage.TransactionBounded implements Storage.Data {

    private final RocksDatabase database;
    private final KeyGenerator.Data dataKeyGenerator;

    private final ConcurrentNavigableMap<ByteBuffer, Boolean> modifiedKeys;
    private final ConcurrentNavigableMap<ByteBuffer, Boolean> deletedKeys;
    private final ConcurrentNavigableMap<ByteBuffer, Boolean> exclusiveInsertKeys;
    private final long snapshotStart;
    private volatile Long snapshotEnd;

    public RocksDataStorage(RocksDatabase database, RocksTransaction transaction) {
        super(database.rocksData, transaction);
        this.database = database;
        this.dataKeyGenerator = database.dataKeyGenerator();
        this.snapshotStart = storageTransaction.getSnapshot().getSequenceNumber();
        this.modifiedKeys = new ConcurrentSkipListMap<>();
        this.deletedKeys = new ConcurrentSkipListMap<>();
        this.exclusiveInsertKeys = new ConcurrentSkipListMap<>();
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
        this.deletedKeys.put(bytes, true);
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
        this.exclusiveInsertKeys.put(bytes, true);
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
        return deletedKeys.keySet();
    }

    public FunctionalIterator<ByteBuffer> modifiedValidatedKeys() {
        return Iterators.iterate(modifiedKeys.entrySet()).filter(Map.Entry::getValue).map(Map.Entry::getKey);
    }

    public NavigableSet<ByteBuffer> modifiedKeys() {
        return modifiedKeys.keySet();
    }

    public boolean isModifiedValidatedKey(ByteBuffer key) {
        return modifiedKeys.getOrDefault(key, false);
    }

    public NavigableSet<ByteBuffer> exclusiveInsertKeys() {
        return exclusiveInsertKeys.keySet();
    }
}
