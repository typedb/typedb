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

package grakn.core.graph.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.EntryList;
import grakn.core.graph.diskstorage.StaticBuffer;

import java.util.List;
import java.util.Map;

/**
 * Wraps a KeyColumnValueStore as a proxy as a basis for
 * other wrappers
 */
public class KCVSProxy implements KeyColumnValueStore {

    protected final KeyColumnValueStore store;

    public KCVSProxy(KeyColumnValueStore store) {
        this.store = Preconditions.checkNotNull(store);
    }

    protected StoreTransaction unwrapTx(StoreTransaction txh) {
        return txh;
    }

    @Override
    public void close() throws BackendException {
        store.close();
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery keyQuery, StoreTransaction txh) throws BackendException {
        return store.getKeys(keyQuery, unwrapTx(txh));
    }

    @Override
    public KeyIterator getKeys(SliceQuery columnQuery, StoreTransaction txh) throws BackendException {
        return store.getKeys(columnQuery, unwrapTx(txh));
    }

    @Override
    public String getName() {
        return store.getName();
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        store.mutate(key, additions, deletions, unwrapTx(txh));
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        return store.getSlice(query, unwrapTx(txh));
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        return store.getSlice(keys, query, unwrapTx(txh));
    }
}
