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

import hypergraph.graph.Storage;
import org.rocksdb.RocksIterator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.StreamSupport;

public class CoreIterator implements Iterator<CoreIterator.CoreKeyValue> {

    private final byte[] prefix;
    private final CoreTransaction.CoreStorage storage;
    private final AtomicBoolean isOpen;
    private RocksIterator rocksIterator;
    private State state;
    private CoreKeyValue next;

    private enum State {INIT, EMPTY, RETRIEVED, COMPLETED}

    CoreIterator(CoreTransaction.CoreStorage storage, byte[] prefix) {
        this.storage = storage;
        this.prefix = prefix;

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

        next = new CoreKeyValue(key, rocksIterator.value());
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

    public final CoreKeyValue peek() {
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
    public final CoreKeyValue next() {
        if (!hasNext()) throw new NoSuchElementException();
        CoreKeyValue result = next;
        next = null;
        state = State.EMPTY;
        return result;
    }

    public static class CoreKeyValue implements Storage.KeyValue {

        private final byte[] key;
        private final byte[] value;
        private final int hash;

        CoreKeyValue(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
            hash = Objects.hash(Arrays.hashCode(key), Arrays.hashCode(value));
        }

        @Override
        public byte[] key() { return key; }

        @Override
        public byte[] value() { return value; }

        @Override
        public String toString() {
            return "{ " + Arrays.toString(key) + " -> " + Arrays.toString(value) + " }";
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || this.getClass() != obj.getClass()) return false;
            if (obj == this) return true;
            CoreKeyValue that = (CoreKeyValue) obj;
            return (Arrays.equals(this.key, that.key) &&
                    Arrays.equals(this.value, that.value));
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}