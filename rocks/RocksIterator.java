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
import com.vaticle.typedb.core.common.iterator.AbstractFunctionalIterator;

import java.util.NoSuchElementException;
import java.util.function.BiFunction;

import static com.vaticle.typedb.core.common.collection.Bytes.bytesHavePrefix;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;

public final class RocksIterator<T> extends AbstractFunctionalIterator<T> implements AutoCloseable {

    private final byte[] prefix;
    private final RocksStorage storage;
    private final BiFunction<byte[], byte[], T> constructor;
    private org.rocksdb.RocksIterator internalRocksIterator;
    private State state;
    private T next;
    private boolean isClosed;

    private enum State {INIT, EMPTY, FETCHED, COMPLETED}

    RocksIterator(RocksStorage storage, byte[] prefix, BiFunction<byte[], byte[], T> constructor) {
        this.storage = storage;
        this.prefix = prefix;
        this.constructor = constructor;
        state = State.INIT;
        isClosed = false;
    }

    public final T peek() {
        if (!hasNext()) {
            if (isClosed) throw TypeDBException.of(RESOURCE_CLOSED);
            else throw new NoSuchElementException();
        }

        return next;
    }

    @Override
    public synchronized final T next() {
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
            case INIT:
                return initialiseAndCheck();
            default: // This should never be reached
                return false;
        }
    }

    private synchronized boolean initialiseAndCheck() {
        if (state != State.COMPLETED) {
            this.internalRocksIterator = storage.getInternalRocksIterator();
            this.internalRocksIterator.seek(prefix);
            state = State.EMPTY;
            return hasValidNext();
        } else {
            return false;
        }
    }

    private synchronized boolean fetchAndCheck() {
        if (state != State.COMPLETED) {
            internalRocksIterator.next();
            return hasValidNext();
        } else {
            return false;
        }
    }

    private synchronized boolean hasValidNext() {
        byte[] key;
        if (!internalRocksIterator.isValid() || !bytesHavePrefix(key = internalRocksIterator.key(), prefix)) {
            recycle();
            return false;
        }
        next = constructor.apply(key, internalRocksIterator.value());
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
}