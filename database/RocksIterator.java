/*
 * Copyright (C) 2022 Vaticle
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

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.collection.KeyValue;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.sorted.AbstractSortedIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterators;
import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typedb.core.encoding.key.Key;

import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;
import static com.vaticle.typedb.core.common.parameters.Order.Asc.ASC;
import static com.vaticle.typedb.core.common.parameters.Order.Desc.DESC;

public abstract class RocksIterator<T extends Key, ORDER extends Order>
        extends AbstractSortedIterator<KeyValue<T, ByteArray>, ORDER>
        implements SortedIterator.Forwardable<KeyValue<T, ByteArray>, ORDER>, AutoCloseable {

    final Key.Prefix<T> prefix;
    final RocksStorage storage;
    State state;
    private KeyValue<T, ByteArray> next;
    private boolean isClosed;
    org.rocksdb.RocksIterator internalRocksIterator;

    private enum State {INIT, OPENED, UNFETCHED, FORWARDED, FETCHED, COMPLETED}

    private RocksIterator(RocksStorage storage, Key.Prefix<T> prefix, ORDER order) {
        super(order);
        this.storage = storage;
        this.prefix = prefix;
        state = State.INIT;
        isClosed = false;
    }

    @Override
    public synchronized final KeyValue<T, ByteArray> peek() {
        if (!hasNext()) {
            if (isClosed) throw TypeDBException.of(RESOURCE_CLOSED);
            else throw new NoSuchElementException();
        }
        return next;
    }

    @Override
    public synchronized final KeyValue<T, ByteArray> next() {
        if (!hasNext()) {
            if (isClosed) throw TypeDBException.of(RESOURCE_CLOSED);
            else throw new NoSuchElementException();
        }
        state = State.UNFETCHED;
        return next;
    }

    @Override
    public final boolean hasNext() {
        switch (state) {
            case COMPLETED:
                return false;
            case FETCHED:
                return true;
            case UNFETCHED:
                return fetchAndCheck();
            case FORWARDED:
                return hasValidNext();
            case INIT:
                return initialiseAndCheck();
            default: // This should never be reached
                return false;
        }
    }

    private synchronized boolean initialiseAndCheck() {
        if (state != State.COMPLETED) {
            initialiseInternalIterator();
            seekToFirst();
            return hasValidNext();
        } else {
            return false;
        }
    }

    void initialiseInternalIterator() {
        assert state == State.INIT;
        this.internalRocksIterator = storage.getInternalRocksIterator(prefix.partition(), usePrefixBloom());
        state = State.OPENED;
    }

    @Override
    public abstract void forward(KeyValue<T, ByteArray> target);

    abstract void seekToFirst();

    abstract boolean fetchAndCheck();

    synchronized boolean hasValidNext() {
        ByteArray key;
        if (!internalRocksIterator.isValid() || !((key = ByteArray.of(internalRocksIterator.key())).hasPrefix(prefix.bytes()))) {
            recycle();
            return false;
        }
        next = KeyValue.of(prefix.builder().build(key), ByteArray.of(internalRocksIterator.value()));
        state = State.FETCHED;
        return true;
    }

    @Override
    public void recycle() {
        close();
    }

    @Override
    public synchronized void close() {
        if (state != State.COMPLETED) {
            if (state != State.INIT) storage.recycle(this);
            state = State.COMPLETED;
            isClosed = true;
            storage.remove(this);
        }
    }

    Key.Partition partition() {
        return prefix.partition();
    }

    boolean usePrefixBloom() {
        return prefix.isFixedStartInPartition();
    }

    @Override
    public final Forwardable<KeyValue<T, ByteArray>, ORDER> merge(
            Forwardable<KeyValue<T, ByteArray>, ORDER> iterator) {
        return SortedIterators.Forwardable.merge(this, iterator);
    }

    @Override
    public SortedIterator.Forwardable<KeyValue<T, ByteArray>, ORDER> intersect(
            SortedIterator.Forwardable<KeyValue<T, ByteArray>, ORDER> iterator) {
        return SortedIterators.Forwardable.intersect(this, iterator);
    }

    @Override
    public <V extends Comparable<? super V>, ORD extends Order> Forwardable<V, ORD> mapSorted(
            Function<KeyValue<T, ByteArray>, V> mappingFn, Function<V, KeyValue<T, ByteArray>> reverseMappingFn, ORD order) {
        return SortedIterators.Forwardable.mapSorted(order, this, mappingFn, reverseMappingFn);
    }

    @Override
    public Forwardable<KeyValue<T, ByteArray>, ORDER> distinct() {
        return SortedIterators.Forwardable.distinct(this);
    }

    @Override
    public Forwardable<KeyValue<T, ByteArray>, ORDER> filter(Predicate<KeyValue<T, ByteArray>> predicate) {
        return SortedIterators.Forwardable.filter(this, predicate);
    }

    @Override
    public Forwardable<KeyValue<T, ByteArray>, ORDER> limit(long limit) {
        return SortedIterators.Forwardable.limit(this, limit);
    }

    @Override
    public Forwardable<KeyValue<T, ByteArray>, ORDER> stopWhen(Function<KeyValue<T, ByteArray>, Boolean> stopCondition) {
        return SortedIterators.Forwardable.stopWhen(this, stopCondition);
    }

    @Override
    public Forwardable<KeyValue<T, ByteArray>, ORDER> onConsumed(Runnable function) {
        return SortedIterators.Forwardable.onConsume(this, function);
    }

    @Override
    public Forwardable<KeyValue<T, ByteArray>, ORDER> onFinalise(Runnable finalise) {
        return SortedIterators.Forwardable.onFinalise(this, finalise);
    }

    static class Ascending<T extends Key> extends RocksIterator<T, Order.Asc> {

        Ascending(RocksStorage storage, Key.Prefix<T> prefix) {
            super(storage, prefix, ASC);
        }

        synchronized void seekToFirst() {
            assert state == State.OPENED;
            this.internalRocksIterator.seek(prefix.bytes().getBytes());
            state = State.FORWARDED;
        }

        @Override
        synchronized boolean fetchAndCheck() {
            if (state != State.COMPLETED) {
                internalRocksIterator.next();
                return hasValidNext();
            } else {
                return false;
            }
        }

        @Override
        public synchronized void forward(KeyValue<T, ByteArray> target) {
            if (state == State.COMPLETED || !ASC.inOrder(prefix.bytes(), target.key().bytes())) return;
            if (state == State.INIT) initialiseInternalIterator();
            internalRocksIterator.seek(target.key().bytes().getBytes());
            state = State.FORWARDED;
        }
    }

    static class Descending<T extends Key> extends RocksIterator<T, Order.Desc> {

        Descending(RocksStorage storage, Key.Prefix<T> prefix) {
            super(storage, prefix, DESC);
        }

        synchronized void seekToFirst() {
            assert state == State.OPENED;
            T lastKey = storage.getLastKey(prefix);
            this.internalRocksIterator.seek(lastKey.bytes().getBytes());
            state = State.FORWARDED;
        }

        @Override
        synchronized boolean fetchAndCheck() {
            if (state != State.COMPLETED) {
                internalRocksIterator.prev();
                return hasValidNext();
            } else {
                return false;
            }
        }

        @Override
        public synchronized void forward(KeyValue<T, ByteArray> target) {
            // TODO: this prefix is not the right one to compare to -- it is the last value of this prefix we want to check against?
            if (state == State.COMPLETED || !DESC.inOrder(prefix.bytes(), target.key().bytes())) return;
            if (state == State.INIT) initialiseInternalIterator();
            internalRocksIterator.seekForPrev(target.key().bytes().getBytes());
            state = State.FORWARDED;
        }
    }
}
