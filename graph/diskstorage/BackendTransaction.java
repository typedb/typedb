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
import grakn.core.graph.core.JanusGraphException;
import grakn.core.graph.diskstorage.indexing.IndexQuery;
import grakn.core.graph.diskstorage.indexing.IndexTransaction;
import grakn.core.graph.diskstorage.indexing.RawQuery;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyIterator;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyRangeQuery;
import grakn.core.graph.diskstorage.keycolumnvalue.KeySliceQuery;
import grakn.core.graph.diskstorage.keycolumnvalue.SliceQuery;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreFeatures;
import grakn.core.graph.diskstorage.keycolumnvalue.cache.CacheTransaction;
import grakn.core.graph.diskstorage.keycolumnvalue.cache.KCVSCache;
import grakn.core.graph.diskstorage.log.kcvs.ExternalCachePersistor;
import grakn.core.graph.diskstorage.util.BackendOperation;
import grakn.core.graph.diskstorage.util.BufferUtil;
import grakn.core.graph.graphdb.database.serialize.DataOutput;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Bundles all storage/index transactions and provides a proxy for some of their
 * methods for convenience. Also increases robustness of read call by attempting
 * read calls multiple times on failure.
 */

public class BackendTransaction implements LoggableTransaction {

    private static final Logger LOG = LoggerFactory.getLogger(BackendTransaction.class);
    private static final int MIN_TASKS_TO_PARALLELIZE = 2;

    //Assumes 64 bit key length as specified in IDManager
    public static final StaticBuffer EDGESTORE_MIN_KEY = BufferUtil.zeroBuffer(8);
    public static final StaticBuffer EDGESTORE_MAX_KEY = BufferUtil.oneBuffer(8);

    private final CacheTransaction storeTx;
    private final BaseTransactionConfig txConfig;
    private final StoreFeatures storeFeatures;

    private final KCVSCache edgeStore;
    private final KCVSCache indexStore;
    private final KCVSCache txLogStore;

    private final Duration maxReadTime;

    private final Executor threadPool;

    private final Map<String, IndexTransaction> indexTx;

    private boolean cacheEnabled = true;

    public BackendTransaction(CacheTransaction storeTx, BaseTransactionConfig txConfig, StoreFeatures features,
                              KCVSCache edgeStore, KCVSCache indexStore, KCVSCache txLogStore, Duration maxReadTime,
                              Map<String, IndexTransaction> indexTx, Executor threadPool) {
        this.storeTx = storeTx;
        this.txConfig = txConfig;
        this.storeFeatures = features;
        this.edgeStore = edgeStore;
        this.indexStore = indexStore;
        this.txLogStore = txLogStore;
        this.maxReadTime = maxReadTime;
        this.indexTx = indexTx;
        this.threadPool = threadPool;
    }

    public ExternalCachePersistor getTxLogPersistor() {
        return new ExternalCachePersistor(txLogStore, storeTx);
    }

    public BaseTransactionConfig getBaseTransactionConfig() {
        return txConfig;
    }

    public IndexTransaction getIndexTransaction(String index) {
        Preconditions.checkArgument(StringUtils.isNotBlank(index));
        IndexTransaction itx = indexTx.get(index);
        return Preconditions.checkNotNull(itx, "Unknown index: " + index);
    }

    public void disableCache() {
        this.cacheEnabled = false;
    }

    public void enableCache() {
        this.cacheEnabled = true;
    }

    public void commitStorage() throws BackendException {
        storeTx.commit();
    }

    public Map<String, Throwable> commitIndexes() {
        final Map<String, Throwable> exceptions = new HashMap<>(indexTx.size());
        for (Map.Entry<String, IndexTransaction> indexTransactionEntry : indexTx.entrySet()) {
            try {
                indexTransactionEntry.getValue().commit();
            } catch (Throwable e) {
                exceptions.put(indexTransactionEntry.getKey(), e);
            }
        }
        return exceptions;
    }

    @Override
    public void commit() throws BackendException {
        storeTx.commit();
        for (IndexTransaction itx : indexTx.values()) itx.commit();
    }

    /**
     * Rolls back all transactions and makes sure that this does not get cut short
     * by exceptions. If exceptions occur, the storage exception takes priority on re-throw.
     */
    @Override
    public void rollback() throws BackendException {
        Throwable exception = null;
        for (IndexTransaction itx : indexTx.values()) {
            try {
                itx.rollback();
            } catch (Throwable e) {
                exception = e;
            }
        }
        storeTx.rollback();
        if (exception != null) { //throw any encountered index transaction rollback exceptions
            if (exception instanceof BackendException) throw (BackendException) exception;
            else throw new PermanentBackendException("Unexpected exception", exception);
        }
    }


    @Override
    public void logMutations(DataOutput out) {
        //Write
        storeTx.logMutations(out);
        for (Map.Entry<String, IndexTransaction> itx : indexTx.entrySet()) {
            out.writeObjectNotNull(itx.getKey());
            itx.getValue().logMutations(out);
        }
    }

    /* ###################################################
            Convenience Write Methods
     */

    /**
     * Applies the specified insertion and deletion mutations on the edge store to the provided key.
     * Both, the list of additions or deletions, may be empty or NULL if there is nothing to be added and/or deleted.
     *
     * @param key       Key
     * @param additions List of entries (column + value) to be added
     * @param deletions List of columns to be removed
     */
    public void mutateEdges(StaticBuffer key, List<Entry> additions, List<Entry> deletions) throws BackendException {
        edgeStore.mutateEntries(key, additions, deletions, storeTx);
    }

    /**
     * Applies the specified insertion and deletion mutations on the property index to the provided key.
     * Both, the list of additions or deletions, may be empty or NULL if there is nothing to be added and/or deleted.
     *
     * @param key       Key
     * @param additions List of entries (column + value) to be added
     * @param deletions List of columns to be removed
     */
    public void mutateIndex(StaticBuffer key, List<Entry> additions, List<Entry> deletions) throws BackendException {
        indexStore.mutateEntries(key, additions, deletions, storeTx);
    }

    /* ###################################################
            Convenience Read Methods
     */

    public EntryList edgeStoreQuery(KeySliceQuery query) {
        return executeRead(new Callable<EntryList>() {
            @Override
            public EntryList call() throws Exception {
                return cacheEnabled ? edgeStore.getSlice(query, storeTx) : edgeStore.getSliceNoCache(query, storeTx);
            }

            @Override
            public String toString() {
                return "EdgeStoreQuery";
            }
        });
    }

    public Map<StaticBuffer, EntryList> edgeStoreMultiQuery(List<StaticBuffer> keys, SliceQuery query) {
        if (storeFeatures.hasMultiQuery()) {
            return executeRead(new Callable<Map<StaticBuffer, EntryList>>() {
                @Override
                public Map<StaticBuffer, EntryList> call() throws Exception {
                    return cacheEnabled ? edgeStore.getSlice(keys, query, storeTx) : edgeStore.getSliceNoCache(keys, query, storeTx);
                }

                @Override
                public String toString() {
                    return "MultiEdgeStoreQuery";
                }
            });
        } else {
            Map<StaticBuffer, EntryList> results = new HashMap<>(keys.size());
            if (threadPool == null || keys.size() < MIN_TASKS_TO_PARALLELIZE) {
                for (StaticBuffer key : keys) {
                    results.put(key, edgeStoreQuery(new KeySliceQuery(key, query)));
                }
            } else {
                CountDownLatch doneSignal = new CountDownLatch(keys.size());
                AtomicInteger failureCount = new AtomicInteger(0);
                EntryList[] resultArray = new EntryList[keys.size()];
                for (int i = 0; i < keys.size(); i++) {
                    threadPool.execute(new SliceQueryRunner(new KeySliceQuery(keys.get(i), query), doneSignal, failureCount, resultArray, i));
                }
                try {
                    doneSignal.await();
                } catch (InterruptedException e) {
                    throw new JanusGraphException("Interrupted while waiting for multi-query to complete", e);
                }
                if (failureCount.get() > 0) {
                    throw new JanusGraphException("Could not successfully complete multi-query. " + failureCount.get() + " individual queries failed.");
                }
                for (int i = 0; i < keys.size(); i++) {
                    results.put(keys.get(i), resultArray[i]);
                }
            }
            return results;
        }
    }

    private class SliceQueryRunner implements Runnable {
        final KeySliceQuery kq;
        final CountDownLatch doneSignal;
        final AtomicInteger failureCount;
        final Object[] resultArray;
        final int resultPosition;

        private SliceQueryRunner(KeySliceQuery kq, CountDownLatch doneSignal, AtomicInteger failureCount,
                                 Object[] resultArray, int resultPosition) {
            this.kq = kq;
            this.doneSignal = doneSignal;
            this.failureCount = failureCount;
            this.resultArray = resultArray;
            this.resultPosition = resultPosition;
        }

        @Override
        public void run() {
            try {
                List<Entry> result;
                result = edgeStoreQuery(kq);
                resultArray[resultPosition] = result;
            } catch (Exception e) {
                failureCount.incrementAndGet();
                LOG.warn("Individual query in multi-transaction failed: ", e);
            } finally {
                doneSignal.countDown();
            }
        }
    }

    public KeyIterator edgeStoreKeys(SliceQuery sliceQuery) {
        if (!storeFeatures.hasScan()) {
            throw new UnsupportedOperationException("The configured storage backend does not support global graph operations - use Faunus instead");
        }

        return executeRead(new Callable<KeyIterator>() {
            @Override
            public KeyIterator call() throws Exception {
                return (storeFeatures.isKeyOrdered())
                        ? edgeStore.getKeys(new KeyRangeQuery(EDGESTORE_MIN_KEY, EDGESTORE_MAX_KEY, sliceQuery), storeTx)
                        : edgeStore.getKeys(sliceQuery, storeTx);
            }

            @Override
            public String toString() {
                return "EdgeStoreKeys";
            }
        });
    }

    public KeyIterator edgeStoreKeys(KeyRangeQuery range) {
        Preconditions.checkArgument(storeFeatures.hasOrderedScan(), "The configured storage backend does not support ordered scans");

        return executeRead(new Callable<KeyIterator>() {
            @Override
            public KeyIterator call() throws Exception {
                return edgeStore.getKeys(range, storeTx);
            }

            @Override
            public String toString() {
                return "EdgeStoreKeys";
            }
        });
    }

    public EntryList indexQuery(KeySliceQuery query) {
        return executeRead(new Callable<EntryList>() {
            @Override
            public EntryList call() throws Exception {
                return cacheEnabled ? indexStore.getSlice(query, storeTx) : indexStore.getSliceNoCache(query, storeTx);
            }

            @Override
            public String toString() {
                return "VertexIndexQuery";
            }
        });

    }

    public Stream<String> indexQuery(String index, IndexQuery query) {
        IndexTransaction indexTx = getIndexTransaction(index);
        return executeRead(new Callable<Stream<String>>() {
            @Override
            public Stream<String> call() throws Exception {
                return indexTx.queryStream(query);
            }

            @Override
            public String toString() {
                return "IndexQuery";
            }
        });
    }

    public Stream<RawQuery.Result<String>> rawQuery(String index, RawQuery query) {
        IndexTransaction indexTx = getIndexTransaction(index);
        return executeRead(new Callable<Stream<RawQuery.Result<String>>>() {
            @Override
            public Stream<RawQuery.Result<String>> call() throws Exception {
                return indexTx.queryStream(query);
            }

            @Override
            public String toString() {
                return "RawQuery";
            }
        });
    }

    private class TotalsCallable implements Callable<Long> {
        final private RawQuery query;
        final private IndexTransaction indexTx;

        TotalsCallable(RawQuery query, IndexTransaction indexTx) {
            this.query = query;
            this.indexTx = indexTx;
        }

        @Override
        public Long call() throws Exception {
            return indexTx.totals(this.query);
        }

        @Override
        public String toString() {
            return "Totals";
        }
    }

    public Long totals(String index, RawQuery query) {
        IndexTransaction indexTx = getIndexTransaction(index);
        return executeRead(new TotalsCallable(query, indexTx));
    }

    private <V> V executeRead(Callable<V> exe) throws JanusGraphException {
        try {
            return BackendOperation.execute(exe, maxReadTime);
        } catch (JanusGraphException e) {
            // support traversal interruption
            // TODO: Refactor to allow direct propagation of underlying interrupt exception
            if (Thread.interrupted()) throw new TraversalInterruptedException();
            throw e;
        }
    }

}
