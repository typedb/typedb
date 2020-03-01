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

package grakn.core.graph.diskstorage.keycolumnvalue.cache;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.BaseTransactionConfig;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.LoggableTransaction;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.keycolumnvalue.KCVMutation;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;
import grakn.core.graph.diskstorage.util.BackendOperation;
import grakn.core.graph.diskstorage.util.BufferUtil;
import grakn.core.graph.graphdb.database.idhandling.VariableLong;
import grakn.core.graph.graphdb.database.serialize.DataOutput;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class CacheTransaction implements StoreTransaction, LoggableTransaction {

    private final StoreTransaction tx;
    private final KeyColumnValueStoreManager manager;
    private final boolean batchLoading;

    /**
     * Buffers graph mutations locally up to the specified number before persisting them against the storage backend.
     */
    private final int bufferSize;
    private final Duration maxWriteTime;

    private int numMutations;
    private final Map<KCVSCache, Map<StaticBuffer, KCVEntryMutation>> mutations;

    public CacheTransaction(StoreTransaction tx, KeyColumnValueStoreManager manager, int bufferSize, Duration maxWriteTime, boolean batchLoading) {
        Preconditions.checkArgument(bufferSize > 0, "Buffer size must be positive");
        this.tx = tx;
        this.manager = manager;
        this.batchLoading = batchLoading;
        this.numMutations = 0;
        this.bufferSize = bufferSize;
        this.maxWriteTime = maxWriteTime;
        this.mutations = new HashMap<>(2);
    }

    public StoreTransaction getWrappedTransaction() {
        return tx;
    }

    void mutate(KCVSCache store, StaticBuffer key, List<Entry> additions, List<Entry> deletions) throws BackendException {
        Preconditions.checkNotNull(store);
        if (additions.isEmpty() && deletions.isEmpty()) return;

        KCVEntryMutation m = new KCVEntryMutation(additions, deletions);
        Map<StaticBuffer, KCVEntryMutation> storeMutation = mutations.computeIfAbsent(store, k -> new HashMap<>());
        KCVEntryMutation existingM = storeMutation.get(key);
        if (existingM != null) {
            existingM.merge(m);
        } else {
            storeMutation.put(key, m);
        }

        numMutations += m.getTotalMutations();

        if (batchLoading && numMutations >= bufferSize) {
            flushInternal();
        }
    }

    private int persist(Map<String, Map<StaticBuffer, KCVMutation>> subMutations) {
        BackendOperation.execute(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                manager.mutateMany(subMutations, tx);
                return true;
            }

            @Override
            public String toString() {
                return "CacheMutation";
            }
        }, maxWriteTime);
        subMutations.clear();
        return 0;
    }

    private KCVMutation convert(KCVEntryMutation mutation) {
        if (!mutation.hasDeletions()) {
            return new KCVMutation(mutation.getAdditions(), KeyColumnValueStore.NO_DELETIONS);
        } else {
            return new KCVMutation(mutation.getAdditions(), Lists.newArrayList(Iterables.transform(mutation.getDeletions(), KCVEntryMutation.ENTRY2COLUMN_FCT)));
        }
    }

    private void flushInternal() {
        if (numMutations > 0) {
            //Consolidate all mutations prior to persistence to ensure that no addition accidentally gets swallowed by a delete
            for (Map<StaticBuffer, KCVEntryMutation> store : mutations.values()) {
                for (KCVEntryMutation mut : store.values()) mut.consolidate();
            }

            //Chunk up mutations
            Map<String, Map<StaticBuffer, KCVMutation>> subMutations = new HashMap<>(mutations.size());
            int numSubMutations = 0;
            for (Map.Entry<KCVSCache, Map<StaticBuffer, KCVEntryMutation>> storeMutations : mutations.entrySet()) {
                Map<StaticBuffer, KCVMutation> sub = new HashMap<>();
                subMutations.put(storeMutations.getKey().getName(), sub);
                for (Map.Entry<StaticBuffer, KCVEntryMutation> mutationsForKey : storeMutations.getValue().entrySet()) {
                    if (mutationsForKey.getValue().isEmpty()) continue;
                    sub.put(mutationsForKey.getKey(), convert(mutationsForKey.getValue()));
                    numSubMutations += mutationsForKey.getValue().getTotalMutations();
                    if (numSubMutations >= bufferSize) {
                        numSubMutations = persist(subMutations);
                        sub.clear();
                        subMutations.put(storeMutations.getKey().getName(), sub);
                    }
                }
            }
            if (numSubMutations > 0) persist(subMutations);


            for (Map.Entry<KCVSCache, Map<StaticBuffer, KCVEntryMutation>> storeMutations : mutations.entrySet()) {
                KCVSCache cache = storeMutations.getKey();
                for (Map.Entry<StaticBuffer, KCVEntryMutation> mutationsForKey : storeMutations.getValue().entrySet()) {
                    if (cache.hasValidateKeysOnly()) {
                        cache.invalidate(mutationsForKey.getKey(), Collections.EMPTY_LIST);
                    } else {
                        KCVEntryMutation m = mutationsForKey.getValue();
                        List<StaticBuffer> entries = new ArrayList<>(m.getTotalMutations());
                        for (Entry e : m.getAdditions()) {
                            entries.add(e);
                        }
                        for (StaticBuffer e : m.getDeletions()) {
                            entries.add(e);
                        }
                        cache.invalidate(mutationsForKey.getKey(), entries);
                    }
                }
            }
            clear();
        }
    }

    private void clear() {
        for (Map.Entry<KCVSCache, Map<StaticBuffer, KCVEntryMutation>> entry : mutations.entrySet()) {
            entry.getValue().clear();
        }
        numMutations = 0;
    }

    @Override
    public void logMutations(DataOutput out) {
        Preconditions.checkArgument(!batchLoading, "Cannot LOG entire mutation set when batch-loading is enabled");
        VariableLong.writePositive(out, mutations.size());
        for (Map.Entry<KCVSCache, Map<StaticBuffer, KCVEntryMutation>> storeMutations : mutations.entrySet()) {
            out.writeObjectNotNull(storeMutations.getKey().getName());
            VariableLong.writePositive(out, storeMutations.getValue().size());
            for (Map.Entry<StaticBuffer, KCVEntryMutation> mutationsForKey : storeMutations.getValue().entrySet()) {
                BufferUtil.writeBuffer(out, mutationsForKey.getKey());
                KCVEntryMutation mut = mutationsForKey.getValue();
                logMutatedEntries(out, mut.getAdditions());
                logMutatedEntries(out, mut.getDeletions());
            }
        }
    }

    private void logMutatedEntries(DataOutput out, List<Entry> entries) {
        VariableLong.writePositive(out, entries.size());
        for (Entry add : entries) BufferUtil.writeEntry(out, add);
    }

    @Override
    public void commit() throws BackendException {
        flushInternal();
        tx.commit();
    }

    @Override
    public void rollback() throws BackendException {
        clear();
        tx.rollback();
    }

    @Override
    public BaseTransactionConfig getConfiguration() {
        return tx.getConfiguration();
    }

}
