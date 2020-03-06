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

package grakn.core.graph.diskstorage.indexing;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.BaseTransaction;
import grakn.core.graph.diskstorage.BaseTransactionConfig;
import grakn.core.graph.diskstorage.LoggableTransaction;
import grakn.core.graph.diskstorage.util.BackendOperation;
import grakn.core.graph.graphdb.database.idhandling.VariableLong;
import grakn.core.graph.graphdb.database.serialize.DataOutput;
import grakn.core.graph.graphdb.util.StreamIterable;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Wraps the transaction handle of an index and buffers all mutations against an index for efficiency.
 * Also acts as a proxy to the IndexProvider methods.
 */

public class IndexTransaction implements LoggableTransaction {

    private static final int DEFAULT_OUTER_MAP_SIZE = 3;
    private static final int DEFAULT_INNER_MAP_SIZE = 5;

    private final IndexProvider index;
    private final BaseTransaction indexTx;
    private final KeyInformation.IndexRetriever keyInformation;

    private final Duration maxWriteTime;

    private Map<String, Map<String, IndexMutation>> mutations;

    public IndexTransaction(IndexProvider index, KeyInformation.IndexRetriever keyInformation,
                            BaseTransactionConfig config,
                            Duration maxWriteTime) throws BackendException {
        Preconditions.checkNotNull(index);
        Preconditions.checkNotNull(keyInformation);
        this.index = index;
        this.keyInformation = keyInformation;
        this.indexTx = index.beginTransaction(config);
        Preconditions.checkNotNull(indexTx);
        this.maxWriteTime = maxWriteTime;
        this.mutations = new HashMap<>(DEFAULT_OUTER_MAP_SIZE);
    }

    public void add(String store, String documentId, IndexEntry entry, boolean isNew) {
        getIndexMutation(store, documentId, isNew, false).addition(new IndexEntry(entry.field, entry.value, entry.getMetaData()));
    }

    public void add(String store, String documentId, String key, Object value, boolean isNew) {
        getIndexMutation(store, documentId, isNew, false).addition(new IndexEntry(key, value));
    }

    public void delete(String store, String documentId, String key, Object value, boolean deleteAll) {
        getIndexMutation(store, documentId, false, deleteAll).deletion(new IndexEntry(key, value));
    }

    private IndexMutation getIndexMutation(String store, String documentId, boolean isNew, boolean isDeleted) {
        final Map<String, IndexMutation> storeMutations = mutations.computeIfAbsent(store, k -> new HashMap<>(DEFAULT_INNER_MAP_SIZE));
        IndexMutation m = storeMutations.get(documentId);
        if (m == null) {
            m = new IndexMutation(keyInformation.get(store), isNew, isDeleted);
            storeMutations.put(documentId, m);
        } else {
            //IndexMutation already exists => if we deleted and re-created it we need to remove the deleted flag
            if (isNew && m.isDeleted()) {
                m.resetDelete();
            }
        }
        return m;
    }


    public void register(String store, String key, KeyInformation information) throws BackendException {
        index.register(store, key, information, indexTx);
    }

    /**
     * @deprecated use #queryStream(IndexQuery query) instead.
     */
    @Deprecated
    public List<String> query(IndexQuery query) throws BackendException {
        return queryStream(query).collect(Collectors.toList());
    }

    public Stream<String> queryStream(IndexQuery query) throws BackendException {
        return index.query(query, keyInformation, indexTx);
    }

    /**
     * @deprecated use #queryStream(RawQuery query) instead.
     */
    @Deprecated
    public Iterable<RawQuery.Result<String>> query(RawQuery query) throws BackendException {
        return new StreamIterable<>(index.query(query, keyInformation, indexTx));
    }

    public Stream<RawQuery.Result<String>> queryStream(RawQuery query) throws BackendException {
        return index.query(query, keyInformation, indexTx);
    }

    public Long totals(RawQuery query) throws BackendException {
        return index.totals(query, keyInformation, indexTx);
    }

    public void restore(Map<String, Map<String, List<IndexEntry>>> documents) throws BackendException {
        index.restore(documents, keyInformation, indexTx);
    }

    @Override
    public void commit() throws BackendException {
        flushInternal();
        indexTx.commit();
    }

    @Override
    public void rollback() throws BackendException {
        mutations = null;
        indexTx.rollback();
    }

    private void flushInternal() {
        if (mutations != null && !mutations.isEmpty()) {
            //Consolidate all mutations prior to persistence to ensure that no addition accidentally gets swallowed by a delete
            for (Map<String, IndexMutation> store : mutations.values()) {
                for (IndexMutation mut : store.values()) mut.consolidate();
            }

            BackendOperation.execute(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    index.mutate(mutations, keyInformation, indexTx);
                    return true;
                }

                @Override
                public String toString() {
                    return "IndexMutation";
                }
            }, maxWriteTime);

            mutations = null;
        }
    }

    @Override
    public void logMutations(DataOutput out) {
        VariableLong.writePositive(out, mutations.size());
        for (Map.Entry<String, Map<String, IndexMutation>> store : mutations.entrySet()) {
            out.writeObjectNotNull(store.getKey());
            VariableLong.writePositive(out, store.getValue().size());
            for (Map.Entry<String, IndexMutation> doc : store.getValue().entrySet()) {
                out.writeObjectNotNull(doc.getKey());
                IndexMutation mut = doc.getValue();
                out.putByte((byte) (mut.isNew() ? 1 : (mut.isDeleted() ? 2 : 0)));
                List<IndexEntry> additions = mut.getAdditions();
                VariableLong.writePositive(out, additions.size());
                for (IndexEntry add : additions) writeIndexEntry(out, add);
                List<IndexEntry> deletions = mut.getDeletions();
                VariableLong.writePositive(out, deletions.size());
                for (IndexEntry del : deletions) writeIndexEntry(out, del);
            }
        }
    }

    private void writeIndexEntry(DataOutput out, IndexEntry entry) {
        out.writeObjectNotNull(entry.field);
        out.writeClassAndObject(entry.value);
    }

}
