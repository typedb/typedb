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
 */

package grakn.core.graph.diskstorage;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * Store-specific (Column-family-specific) options passed between
 * JanusGraph core and its underlying KeyColumnValueStore implementation.
 * This is part of JanusGraph's internals and is not user-facing in
 * ordinary operation.
 */
public interface StoreMetaData<T> {

    Class<? extends T> getDataType();

    StoreMetaData<Integer> TTL = TTLImpl.INSTANCE;

    Container EMPTY = new Container(false);

    class Container {

        private final boolean mutable;

        private final Map<StoreMetaData<?>, Object> md = new HashMap<>();

        public Container() {
            this(true);
        }

        public Container(boolean mutable) {
            this.mutable = mutable;
        }

        public <V, K extends StoreMetaData<V>> void put(K type, V value) {
            Preconditions.checkState(mutable);
            md.put(type, value);
        }

        public <V, K extends StoreMetaData<V>> V get(K type) {
            return type.getDataType().cast(md.get(type));
        }

        public <K extends StoreMetaData<?>> boolean contains(K type) {
            return md.containsKey(type);
        }

        public int size() {
            return md.size();
        }

        public boolean isEmpty() {
            return md.isEmpty();
        }
    }

    /**
     * Time-to-live for all data written to the store.  Values associated
     * with this enum will be expressed in seconds.  The TTL is only required
     * to be honored when the associated store is opened for the first time.
     * Subsequent re-openings of an existing store need not check for or
     * modify the existing TTL (though implementations are free to do so).
     */
    enum TTLImpl implements StoreMetaData<Integer> {
        INSTANCE;

        @Override
        public Class<? extends Integer> getDataType() {
            return Integer.class;
        }
    }
}


