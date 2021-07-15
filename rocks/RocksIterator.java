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

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.collection.KeyValue;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.AbstractFunctionalIterator;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;

import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;

public final class RocksIterator extends AbstractFunctionalIterator.Sorted<KeyValue<ByteArray, ByteArray>>
        implements FunctionalIterator.Sorted.Forwardable<KeyValue<ByteArray, ByteArray>>, AutoCloseable {

    private final ByteArray prefix;
    private final RocksStorage storage;
    private org.rocksdb.RocksIterator internalRocksIterator;
    private State state;
    private KeyValue<ByteArray, ByteArray> next;
    private boolean isClosed;

    private enum State {INIT, EMPTY, FORWARDED_EMPTY, FETCHED, COMPLETED}

    RocksIterator(RocksStorage storage, ByteArray prefix) {
        this.storage = storage;
        this.prefix = prefix;
        state = State.INIT;
        isClosed = false;
    }

    @Override
    public synchronized final KeyValue<ByteArray, ByteArray> peek() {
        if (!hasNext()) {
            if (isClosed) throw TypeDBException.of(RESOURCE_CLOSED);
            else throw new NoSuchElementException();
        }
        return next;
    }

    @Override
    public synchronized final KeyValue<ByteArray, ByteArray> next() {
        if (!hasNext()) {
            if (isClosed) throw TypeDBException.of(RESOURCE_CLOSED);
            else throw new NoSuchElementException();
        }
        state = State.EMPTY;
        return next;
    }

    @Override
    public final boolean hasNext() {
        switch (state) {
            case COMPLETED:
                return false;
            case FETCHED:
                return true;
            case EMPTY:
                return fetchAndCheck();
            case FORWARDED_EMPTY:
                return checkValidNext();
            case INIT:
                return initialiseAndCheck();
            default: // This should never be reached
                return false;
        }
    }

    @Override
    public synchronized void forward(KeyValue<ByteArray, ByteArray> target) {
        if (state == State.INIT) initialise(target.key());
        else internalRocksIterator.seek(target.key().getBytes());
        state = State.FORWARDED_EMPTY;
    }

    private synchronized boolean fetchAndCheck() {
        if (state != State.COMPLETED) {
            internalRocksIterator.next();
            return checkValidNext();
        } else {
            return false;
        }
    }

    private synchronized boolean initialiseAndCheck() {
        if (state != State.COMPLETED) {
            initialise(prefix);
            return checkValidNext();
        } else {
            return false;
        }
    }

    private synchronized void initialise(ByteArray prefix) {
        assert state == State.INIT;
        this.internalRocksIterator = storage.getInternalRocksIterator();
        this.internalRocksIterator.seek(prefix.getBytes());
        state = State.FORWARDED_EMPTY;
    }

    private synchronized boolean checkValidNext() {
        ByteArray key;
        if (!internalRocksIterator.isValid() || !((key = ByteArray.of(internalRocksIterator.key())).hasPrefix(prefix))) {
            recycle();
            return false;
        }
        next = KeyValue.of(key, ByteArray.of(internalRocksIterator.value()));
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
            if (state != State.INIT) storage.recycle(internalRocksIterator);
            state = State.COMPLETED;
            isClosed = true;
            storage.remove(this);
        }
    }

    @SafeVarargs
    @Override
    public final FunctionalIterator.Sorted.Forwardable<KeyValue<ByteArray, ByteArray>> merge(
            FunctionalIterator.Sorted.Forwardable<KeyValue<ByteArray, ByteArray>>... iterators) {
        return Iterators.Sorted.merge(this, iterators);
    }

    @Override
    public <V extends Comparable<? super V>> FunctionalIterator.Sorted.Forwardable<V> mapSorted(
            Function<KeyValue<ByteArray, ByteArray>, V> mappingFn,
            Function<V, KeyValue<ByteArray, ByteArray>> reverseMappingFn) {
        return Iterators.Sorted.mapSorted(this, mappingFn, reverseMappingFn);
    }

    @Override
    public FunctionalIterator.Sorted.Forwardable<KeyValue<ByteArray, ByteArray>> distinct() {
        return Iterators.Sorted.distinct(this);
    }

    @Override
    public FunctionalIterator.Sorted.Forwardable<KeyValue<ByteArray, ByteArray>> filter(Predicate<KeyValue<ByteArray, ByteArray>> predicate) {
        return Iterators.Sorted.filter(this, predicate);
    }

    @Override
    public FunctionalIterator.Sorted.Forwardable<KeyValue<ByteArray, ByteArray>> onFinalise(Runnable finalise) {
        return Iterators.Sorted.onFinalise(this, finalise);
    }

}
