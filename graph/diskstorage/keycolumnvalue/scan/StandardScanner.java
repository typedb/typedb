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

package grakn.core.graph.diskstorage.keycolumnvalue.scan;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.schema.JanusGraphManagement;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.configuration.MergedConfiguration;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;
import grakn.core.graph.diskstorage.util.StandardBaseTransactionConfig;
import grakn.core.graph.diskstorage.util.time.TimestampProvider;
import org.apache.commons.lang.StringUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;


public class StandardScanner {

    private final KeyColumnValueStoreManager manager;
    private final Set<KeyColumnValueStore> openStores;
    private final ConcurrentMap<Object, StandardScannerExecutor> runningJobs;
    private final AtomicLong jobCounter;

    public StandardScanner(KeyColumnValueStoreManager manager) {
        Preconditions.checkNotNull(manager);
        Preconditions.checkArgument(manager.getFeatures().hasScan(), "Provided data store does not support scans: %s", manager);

        this.manager = manager;
        this.openStores = new HashSet<>(4);
        this.runningJobs = new ConcurrentHashMap<>();
        this.jobCounter = new AtomicLong(0);
    }

    public Builder build() {
        return new Builder();
    }

    public void close() throws BackendException {
        //Interrupt running jobs
        for (StandardScannerExecutor exe : runningJobs.values()) {
            if (exe.isCancelled() || exe.isDone()) continue;
            exe.cancel(true);
        }
        for (KeyColumnValueStore kcvs : openStores) kcvs.close();
    }

    private void addJob(Object jobId, StandardScannerExecutor executor) {
        for (Map.Entry<Object, StandardScannerExecutor> jobs : runningJobs.entrySet()) {
            StandardScannerExecutor exe = jobs.getValue();
            if (exe.isDone() || exe.isCancelled()) {
                runningJobs.remove(jobs.getKey(), exe);
            }
        }
        Preconditions.checkArgument(runningJobs.putIfAbsent(jobId, executor) == null, "Another job with the same id is already running: %s", jobId);
    }

    public JanusGraphManagement.IndexJobFuture getRunningJob(Object jobId) {
        return runningJobs.get(jobId);
    }

    public class Builder {

        private static final int DEFAULT_WORKBLOCK_SIZE = 10000;

        private ScanJob job;
        private int numProcessingThreads;
        private int workBlockSize;
        private TimestampProvider times;
        private Configuration graphConfiguration;
        private Configuration jobConfiguration;
        private String dbName;
        private Consumer<ScanMetrics> finishJob;
        private Object jobId;

        private Builder() {
            numProcessingThreads = 1;
            workBlockSize = DEFAULT_WORKBLOCK_SIZE;
            job = null;
            times = null;
            graphConfiguration = Configuration.EMPTY;
            jobConfiguration = Configuration.EMPTY;
            dbName = null;
            jobId = jobCounter.incrementAndGet();
            finishJob = m -> {
            };
        }

        public Builder setNumProcessingThreads(int numThreads) {
            Preconditions.checkArgument(numThreads > 0,
                    "Need to specify a positive number of processing threads: %s", numThreads);
            this.numProcessingThreads = numThreads;
            return this;
        }

        public Builder setWorkBlockSize(int size) {
            Preconditions.checkArgument(size > 0, "Need to specify a positive work block size: %s", size);
            this.workBlockSize = size;
            return this;
        }

        public Builder setTimestampProvider(TimestampProvider times) {
            this.times = Preconditions.checkNotNull(times);
            return this;
        }

        public Builder setStoreName(String name) {
            Preconditions.checkArgument(StringUtils.isNotBlank(name), "Invalid name: %s", name);
            this.dbName = name;
            return this;
        }

        public Builder setJobId(Object id) {
            this.jobId = Preconditions.checkNotNull(id, "Need to provide a valid id: %s", id);
            return this;
        }

        public Builder setJob(ScanJob job) {
            this.job = Preconditions.checkNotNull(job);
            return this;
        }

        public Builder setGraphConfiguration(Configuration config) {
            this.graphConfiguration = Preconditions.checkNotNull(config);
            return this;
        }

        public Builder setJobConfiguration(Configuration config) {
            this.jobConfiguration = Preconditions.checkNotNull(config);
            return this;
        }

        public Builder setFinishJob(Consumer<ScanMetrics> finishJob) {
            this.finishJob = Preconditions.checkNotNull(finishJob);
            return this;
        }

        public JanusGraphManagement.IndexJobFuture execute() throws BackendException {
            Preconditions.checkNotNull(job, "Need to specify a job to execute");
            Preconditions.checkArgument(StringUtils.isNotBlank(dbName), "Need to specify a database to execute against");
            Preconditions.checkNotNull(times, "Need to configure the timestamp provider for this job");
            StandardBaseTransactionConfig.Builder txBuilder = new StandardBaseTransactionConfig.Builder();
            txBuilder.timestampProvider(times);

            Configuration scanConfig = manager.getFeatures().getScanTxConfig();
            if (Configuration.EMPTY != graphConfiguration) {
                scanConfig = null == scanConfig ?
                        graphConfiguration :
                        new MergedConfiguration(graphConfiguration, scanConfig);
            }
            if (null != scanConfig) {
                txBuilder.customOptions(scanConfig);
            }

            StoreTransaction storeTx = manager.beginTransaction(txBuilder.build());
            KeyColumnValueStore kcvs = manager.openDatabase(dbName);

            openStores.add(kcvs);
            try {
                StandardScannerExecutor executor = new StandardScannerExecutor(job, finishJob, kcvs, storeTx,
                        manager.getFeatures(), numProcessingThreads, workBlockSize, jobConfiguration, graphConfiguration);
                addJob(jobId, executor);
                new Thread(executor).start();
                return executor;
            } catch (Throwable e) {
                storeTx.rollback();
                throw e;
            }
        }

    }
}
