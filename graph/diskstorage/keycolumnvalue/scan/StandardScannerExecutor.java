// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grakn.core.graph.diskstorage.keycolumnvalue.scan;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractFuture;
import grakn.core.graph.core.schema.JanusGraphManagement;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.EntryList;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.TemporaryBackendException;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.keycolumnvalue.KCVSUtil;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyIterator;
import grakn.core.graph.diskstorage.keycolumnvalue.SliceQuery;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreFeatures;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;
import grakn.core.graph.diskstorage.keycolumnvalue.scan.ScanJob;
import grakn.core.graph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import grakn.core.graph.diskstorage.keycolumnvalue.scan.StandardScanMetrics;
import grakn.core.graph.diskstorage.util.BufferUtil;
import grakn.core.graph.diskstorage.util.RecordIterator;
import grakn.core.graph.diskstorage.util.StaticArrayEntry;
import grakn.core.graph.diskstorage.util.StaticArrayEntryList;
import grakn.core.graph.util.system.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;


class StandardScannerExecutor extends AbstractFuture<ScanMetrics> implements JanusGraphManagement.IndexJobFuture, Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(org.janusgraph.diskstorage.keycolumnvalue.scan.StandardScannerExecutor.class);

    private static final int QUEUE_SIZE = 1000;
    private static final int TIMEOUT_MS = 180000; // 60 seconds
    private static final int TIME_PER_TRY = 10; // 10 milliseconds
    private static final int MAX_KEY_LENGTH = 128; //in bytes

    private final ScanJob job;
    private final Consumer<ScanMetrics> finishJob;
    private final StoreFeatures storeFeatures;
    private final StoreTransaction storeTx;
    private final KeyColumnValueStore store;
    private final int numProcessors;
    private final int workBlockSize;
    private final Configuration jobConfiguration;
    private final Configuration graphConfiguration;
    private final ScanMetrics metrics;

    private boolean hasCompleted = false;
    private boolean interrupted = false;

    private List<BlockingQueue<SliceResult>> dataQueues;
    private DataPuller[] pullThreads;

    StandardScannerExecutor(ScanJob job, Consumer<ScanMetrics> finishJob,
                            final KeyColumnValueStore store, StoreTransaction storeTx,
                            final StoreFeatures storeFeatures,
                            final int numProcessors, int workBlockSize,
                            final Configuration jobConfiguration,
                            final Configuration graphConfiguration) {
        this.job = job;
        this.finishJob = finishJob;
        this.store = store;
        this.storeTx = storeTx;
        this.storeFeatures = storeFeatures;
        this.numProcessors = numProcessors;
        this.workBlockSize = workBlockSize;
        this.jobConfiguration = jobConfiguration;
        this.graphConfiguration = graphConfiguration;

        metrics = new StandardScanMetrics();

    }

    private DataPuller addDataPuller(SliceQuery sq, StoreTransaction stx) throws BackendException {
        final BlockingQueue<SliceResult> queue = new LinkedBlockingQueue<>(QUEUE_SIZE);
        dataQueues.add(queue);

        DataPuller dp = new DataPuller(sq, queue,
                KCVSUtil.getKeys(store, sq, storeFeatures, MAX_KEY_LENGTH, stx), job.getKeyFilter());
        dp.start();
        return dp;
    }

    @Override
    public void run() {
        List<SliceQuery> queries;
        int numQueries;
        try {
            job.workerIterationStart(jobConfiguration, graphConfiguration, metrics);

            queries = job.getQueries();
            numQueries = queries.size();
            Preconditions.checkArgument(numQueries > 0, "Must at least specify one query for job: %s", job);
            if (numQueries > 1) {
                //It is assumed that the first query is the grounding query if multiple queries exist
                SliceQuery ground = queries.get(0);
                StaticBuffer start = ground.getSliceStart();
                Preconditions.checkArgument(start.equals(BufferUtil.zeroBuffer(1)),
                        "Expected start of first query to be a single 0s: %s", start);
                StaticBuffer end = ground.getSliceEnd();
                Preconditions.checkArgument(end.equals(BufferUtil.oneBuffer(end.length())),
                        "Expected end of first query to be all 1s: %s", end);
            }
            dataQueues = new ArrayList<>(numQueries);
            pullThreads = new DataPuller[numQueries];

            for (int pos = 0; pos < numQueries; pos++) {
                pullThreads[pos] = addDataPuller(queries.get(pos), storeTx);
            }
        } catch (Throwable e) {
            LOG.error("Exception trying to setup the job:", e);
            cleanupSilent();
            job.workerIterationEnd(metrics);
            setException(e);
            return;
        }

        BlockingQueue<Row> processorQueue = new LinkedBlockingQueue<>(QUEUE_SIZE);

        Processor[] processors = new Processor[numProcessors];
        for (int i = 0; i < processors.length; i++) {
            processors[i] = new Processor(job.clone(), processorQueue);
            processors[i].start();
        }

        try {
            SliceResult[] currentResults = new SliceResult[numQueries];
            while (!interrupted) {
                for (int i = 0; i < numQueries; i++) {
                    if (currentResults[i] != null) continue;
                    BlockingQueue<SliceResult> queue = dataQueues.get(i);

                    SliceResult qr = queue.poll(TIME_PER_TRY, TimeUnit.MILLISECONDS); //Try very short time to see if we are done
                    if (qr == null) {
                        if (pullThreads[i].isFinished()) continue; //No more data to be expected
                        int retryCount = 0;
                        while (!pullThreads[i].isFinished() && retryCount < TIMEOUT_MS / TIME_PER_TRY && qr == null) {
                            retryCount++;
                            qr = queue.poll(TIME_PER_TRY, TimeUnit.MILLISECONDS);
                        }
                        if (qr == null && !pullThreads[i].isFinished())
                            throw new TemporaryBackendException("Timed out waiting for next row data - storage error likely");
                    }
                    currentResults[i] = qr;
                }
                SliceResult conditionQuery = currentResults[0];
                if (conditionQuery == null) break; //Termination condition - primary query has no more data
                StaticBuffer key = conditionQuery.key;

                Map<SliceQuery, EntryList> queryResults = new HashMap<>(numQueries);
                for (int i = 0; i < currentResults.length; i++) {
                    SliceQuery query = queries.get(i);
                    EntryList entries = EntryList.EMPTY_LIST;
                    if (currentResults[i] != null && currentResults[i].key.equals(key)) {
                        entries = currentResults[i].entries;
                        currentResults[i] = null;
                    }
                    queryResults.put(query, entries);
                }
                processorQueue.put(new Row(key, queryResults));
            }

            for (int i = 0; i < pullThreads.length; i++) {
                pullThreads[i].join(10);
                if (pullThreads[i].isAlive()) {
                    LOG.warn("Data pulling thread [{}] did not terminate. Forcing termination", i);
                    pullThreads[i].interrupt();
                }
            }

            for (Processor processor : processors) {
                processor.finish();
            }
            if (!Threads.waitForCompletion(processors, TIMEOUT_MS)) LOG.error("Processor did not terminate in time");

            cleanup();
            try {
                job.workerIterationEnd(metrics);
            } catch (IllegalArgumentException e) {
                // https://github.com/JanusGraph/janusgraph/pull/891
                LOG.warn("Exception occurred processing worker iteration end. See PR 891.", e);
            }

            if (interrupted) {
                setException(new InterruptedException("Scanner got interrupted"));
            } else {
                finishJob.accept(metrics);
                set(metrics);
            }
        } catch (Throwable e) {
            LOG.error("Exception occurred during job execution:", e);
            job.workerIterationEnd(metrics);
            setException(e);
        } finally {
            Threads.terminate(processors);
            cleanupSilent();
        }
    }

    @Override
    protected void interruptTask() {
        interrupted = true;
    }

    private void cleanup() throws BackendException {
        if (!hasCompleted) {
            hasCompleted = true;
            if (pullThreads != null) {
                for (DataPuller pullThread : pullThreads) {
                    if (pullThread.isAlive()) {
                        pullThread.interrupt();
                    }
                }
            }
            storeTx.rollback();
        }
    }

    private void cleanupSilent() {
        try {
            cleanup();
        } catch (BackendException ex) {
            LOG.error("Encountered exception when trying to clean up after failure", ex);
        }
    }

    @Override
    public ScanMetrics getIntermediateResult() {
        return metrics;
    }

    private static class Row {

        final StaticBuffer key;
        final Map<SliceQuery, EntryList> entries;

        private Row(StaticBuffer key, Map<SliceQuery, EntryList> entries) {
            this.key = key;
            this.entries = entries;
        }
    }


    private class Processor extends Thread {

        private ScanJob job;
        private final BlockingQueue<Row> processorQueue;

        private volatile boolean finished;
        private int numProcessed;


        private Processor(ScanJob job, BlockingQueue<Row> processorQueue) {
            this.job = job;
            this.processorQueue = processorQueue;

            this.finished = false;
            this.numProcessed = 0;
        }

        @Override
        public void run() {
            try {
                job.workerIterationStart(jobConfiguration, graphConfiguration, metrics);
                while (!finished || !processorQueue.isEmpty()) {
                    Row row;
                    while ((row = processorQueue.poll(100, TimeUnit.MILLISECONDS)) != null) {
                        if (numProcessed >= workBlockSize) {
                            //Setup new chunk of work
                            job.workerIterationEnd(metrics);
                            job = job.clone();
                            job.workerIterationStart(jobConfiguration, graphConfiguration, metrics);
                            numProcessed = 0;
                        }
                        try {
                            job.process(row.key, row.entries, metrics);
                            metrics.increment(ScanMetrics.Metric.SUCCESS);
                        } catch (Throwable ex) {
                            LOG.error("Exception processing row [" + row.key + "]: ", ex);
                            metrics.increment(ScanMetrics.Metric.FAILURE);
                        }
                        numProcessed++;
                    }
                }
            } catch (InterruptedException e) {
                LOG.error("Processing thread interrupted while waiting on queue or processing data", e);
            } catch (Throwable e) {
                LOG.error("Unexpected error processing data: {}", e);
            } finally {
                job.workerIterationEnd(metrics);
            }
        }

        public void finish() {
            this.finished = true;
        }
    }


    private static class DataPuller extends Thread {

        private final BlockingQueue<SliceResult> queue;
        private final KeyIterator keyIterator;
        private final SliceQuery query;
        private final Predicate<StaticBuffer> keyFilter;
        private volatile boolean finished;

        private DataPuller(SliceQuery query, BlockingQueue<SliceResult> queue,
                           KeyIterator keyIterator, Predicate<StaticBuffer> keyFilter) {
            this.query = query;
            this.queue = queue;
            this.keyIterator = keyIterator;
            this.keyFilter = keyFilter;
            this.finished = false;
        }

        @Override
        public void run() {
            try {
                while (keyIterator.hasNext()) {
                    StaticBuffer key = keyIterator.next();
                    RecordIterator<Entry> entries = keyIterator.getEntries();
                    if (!keyFilter.test(key)) continue;
                    EntryList entryList = StaticArrayEntryList.ofStaticBuffer(entries, StaticArrayEntry.ENTRY_GETTER);
                    queue.put(new SliceResult(query, key, entryList));
                }
                finished = true;
            } catch (InterruptedException e) {
                LOG.error("Data-pulling thread interrupted while waiting on queue or data", e);
            } catch (Throwable e) {
                LOG.error("Could not load data from storage: {}", e);
            } finally {
                try {
                    keyIterator.close();
                } catch (IOException e) {
                    LOG.warn("Could not close storage iterator ", e);
                }
            }
        }

        boolean isFinished() {
            return finished;
        }
    }

    private static class SliceResult {
        final SliceQuery query;
        final StaticBuffer key;
        final EntryList entries;

        private SliceResult(SliceQuery query, StaticBuffer key, EntryList entries) {
            this.query = query;
            this.key = key;
            this.entries = entries;
        }
    }
}



