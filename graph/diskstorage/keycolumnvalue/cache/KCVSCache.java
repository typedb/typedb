/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graph.diskstorage.keycolumnvalue.cache;

import com.google.common.collect.ImmutableList;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.EntryList;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.keycolumnvalue.KCVSProxy;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import grakn.core.graph.diskstorage.keycolumnvalue.KeySliceQuery;
import grakn.core.graph.diskstorage.keycolumnvalue.SliceQuery;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;
import grakn.core.graph.diskstorage.keycolumnvalue.cache.CacheTransaction;
import grakn.core.graph.diskstorage.util.CacheMetricsAction;
import grakn.core.graph.util.stats.MetricManager;

import java.util.List;
import java.util.Map;


public abstract class KCVSCache extends KCVSProxy {

    public static final List<Entry> NO_DELETIONS = ImmutableList.of();

    private final String metricsName;

    protected KCVSCache(KeyColumnValueStore store, String metricsName) {
        super(store);
        this.metricsName = metricsName;
    }

    protected boolean hasValidateKeysOnly() {
        return true;
    }

    protected void incActionBy(int by, CacheMetricsAction action, StoreTransaction txh) {
//        if (metricsName != null && txh.getConfiguration().hasGroupName()) {
            //TODO-reenable
//            MetricManager.INSTANCE.getCounter(txh.getConfiguration().getGroupName(), metricsName, action.getName()).inc(by);
//        }
    }

    public abstract void clearCache();

    protected abstract void invalidate(StaticBuffer key, List<StaticBuffer> entries);

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException("Only supports mutateEntries()");
    }

    public void mutateEntries(StaticBuffer key, List<Entry> additions, List<Entry> deletions, StoreTransaction txh) throws BackendException {
        ((CacheTransaction) txh).mutate(this, key, additions, deletions);
    }

    @Override
    protected final StoreTransaction unwrapTx(StoreTransaction txh) {
        return ((CacheTransaction) txh).getWrappedTransaction();
    }

    public EntryList getSliceNoCache(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        return store.getSlice(query, unwrapTx(txh));
    }

    public Map<StaticBuffer, EntryList> getSliceNoCache(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        return store.getSlice(keys, query, unwrapTx(txh));
    }

}
