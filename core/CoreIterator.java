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

import org.rocksdb.RocksIterator;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

public class CoreIterator<G> implements Iterator<G> {

    private final byte[] prefix;
    private final CoreTransaction.CoreStorage storage;
    private final AtomicBoolean isOpen;
    private final BiFunction<byte[], byte[], G> constructor;
    private RocksIterator rocksIterator;
    private State state;
    private G next;

    private enum State {INIT, EMPTY, RETRIEVED, COMPLETED}

    CoreIterator(CoreTransaction.CoreStorage storage, byte[] prefix, BiFunction<byte[], byte[], G> constructor) {
        this.storage = storage;
        this.prefix = prefix;
        this.constructor = constructor;

        isOpen = new AtomicBoolean(false);
        state = State.INIT;
    }

    private void initalise() {
        this.rocksIterator = storage.newRocksIterator();
        this.rocksIterator.seek(prefix);
    }

    private boolean retrieveAndCheckHasNext() {
        byte[] key;
        if (!rocksIterator.isValid() || !keyHasPrefix(key = rocksIterator.key(), prefix)) {
            state = State.COMPLETED;
            rocksIterator.close();
            return false;
        }

        next = constructor.apply(key, rocksIterator.value());
        state = State.RETRIEVED;
        return true;
    }

    private boolean keyHasPrefix(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) return false;
        }
        return true;
    }

    public final G peek() {
        if (!hasNext()) throw new NoSuchElementException();
        return next;
    }

    void close() {
        if (isOpen.compareAndSet(true, false)) {
            if (state != State.INIT) rocksIterator.close();
            storage.remove(this);
        }
    }

    @Override
    public final boolean hasNext() {
        switch (state) {
            case COMPLETED:
                return false;
            case RETRIEVED:
                return true;
            case EMPTY:
                return retrieveAndCheckHasNext();
            case INIT:
                initalise();
                return retrieveAndCheckHasNext();
            default: // This should never be reached
                return false;
        }
    }

    @Override
    public final G next() {
        if (!hasNext()) throw new NoSuchElementException();
        state = State.EMPTY;
        return next;
    }
}