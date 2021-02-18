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

import grakn.core.common.iterator.AbstractResourceIterator;

import java.util.NoSuchElementException;
import java.util.function.BiFunction;

import static grakn.core.common.collection.Bytes.bytesHavePrefix;

public final class RocksIterator<T> extends AbstractResourceIterator<T> implements AutoCloseable {

    private final byte[] prefix;
    private final RocksStorage storage;
    private final BiFunction<byte[], byte[], T> constructor;
    private org.rocksdb.RocksIterator internalRocksIterator;
    private State state;
    private T next;

    private enum State {INIT, EMPTY, FETCHED, COMPLETED}

    RocksIterator(RocksStorage storage, byte[] prefix, BiFunction<byte[], byte[], T> constructor) {
        this.storage = storage;
        this.prefix = prefix;
        this.constructor = constructor;
        state = State.INIT;
    }

    private synchronized boolean initialise() {
        if (state != State.COMPLETED) {
            this.internalRocksIterator = storage.getInternalRocksIterator();
            this.internalRocksIterator.seek(prefix);
            if (!internalRocksIterator.isValid()) System.out.println("INVALID ITERATOR");
            state = State.EMPTY;
            byte[] key;
            if (!internalRocksIterator.isValid() || !bytesHavePrefix(key = internalRocksIterator.key(), prefix)) {
                recycle();
                return false;
            }
            next = constructor.apply(key, internalRocksIterator.value());
            state = State.FETCHED;
            return true;
        } else {
            return false;
        }
    }

    public synchronized final T peek() {
        if (!hasNext()) throw new NoSuchElementException();
        return next;
    }

    @Override
    public synchronized final T next() {
        if (!hasNext()) throw new NoSuchElementException();
        state = State.EMPTY;
        return next;
    }

    @Override
    public synchronized final boolean hasNext() {
        switch (state) {
            case COMPLETED:
                return false;
            case FETCHED:
                return true;
            case EMPTY:
                return fetchAndCheck();
            case INIT:
                return initialise();
            default: // This should never be reached
                return false;
        }
    }

    private synchronized boolean fetchAndCheck() {
        if (state != State.COMPLETED) {
            internalRocksIterator.next();
            byte[] key;
            if (!internalRocksIterator.isValid() || !bytesHavePrefix(key = internalRocksIterator.key(), prefix)) {
                recycle();
                return false;
            }
            next = constructor.apply(key, internalRocksIterator.value());
            state = State.FETCHED;
            return true;
        } else {
            return false;
        }
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
            storage.remove(this);
        }
    }
}