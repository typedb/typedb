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

package grakn.core.graph.diskstorage;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.lang.StringUtils;
import org.janusgraph.core.JanusGraphConfigurationException;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.diskstorage.StandardIndexProvider;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.configuration.backend.KCVSConfiguration;
import org.janusgraph.diskstorage.configuration.backend.builder.KCVSConfigurationBuilder;
import org.janusgraph.diskstorage.indexing.IndexFeatures;
import org.janusgraph.diskstorage.indexing.IndexInformation;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.IndexTransaction;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.cache.CacheTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.cache.ExpirationKCVSCache;
import org.janusgraph.diskstorage.keycolumnvalue.cache.KCVSCache;
import org.janusgraph.diskstorage.keycolumnvalue.cache.NoKCVSCache;
import org.janusgraph.diskstorage.keycolumnvalue.scan.StandardScanner;
import org.janusgraph.diskstorage.locking.Locker;
import org.janusgraph.diskstorage.locking.LockerProvider;
import org.janusgraph.diskstorage.locking.consistentkey.ConsistentKeyLocker;
import org.janusgraph.diskstorage.locking.consistentkey.ExpectedValueCheckingStoreManager;
import org.janusgraph.diskstorage.log.Log;
import org.janusgraph.diskstorage.log.LogManager;
import org.janusgraph.diskstorage.log.kcvs.KCVSLog;
import org.janusgraph.diskstorage.log.kcvs.KCVSLogManager;
import org.janusgraph.diskstorage.util.BackendOperation;
import org.janusgraph.diskstorage.util.MetricInstrumentedStoreManager;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.graphdb.transaction.TransactionConfiguration;
import org.janusgraph.util.system.ConfigurationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.BASIC_METRICS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.BUFFER_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DB_CACHE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DB_CACHE_CLEAN_WAIT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DB_CACHE_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DB_CACHE_TIME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_NS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.JOB_NS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.JOB_START_TIME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.MANAGEMENT_LOG;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.METRICS_MERGE_STORES;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.PARALLEL_BACKEND_OPS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BATCH;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_READ_WAITTIME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_WRITE_WAITTIME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.SYSTEM_PROPERTIES_STORE_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.TIMESTAMP_PROVIDER;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.TRANSACTION_LOG;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.USER_LOG;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.USER_LOG_PREFIX;

/**
 * Orchestrates and configures all backend systems:
 * The primary backend storage ({@link KeyColumnValueStore}) and all external indexing providers ({@link IndexProvider}).
 */

public class Backend implements LockerProvider, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(org.janusgraph.diskstorage.Backend.class);

    /**
     * These are the names for the edge store and property index databases, respectively.
     * The edge store contains all edges and properties. The property index contains an
     * inverted index from attribute value to vertex.
     * <p>
     * These names are fixed and should NEVER be changed. Changing these strings can
     * disrupt storage adapters that rely on these names for specific configurations.
     * In the past, the store name for the ID table, janusgraph_ids, was also marked here,
     * but to clear the upgrade path from Titan to JanusGraph, we had to pull it into
     * configuration.
     */
    public static final String EDGESTORE_NAME = "edgestore";
    public static final String INDEXSTORE_NAME = "graphindex";

    public static final String METRICS_STOREMANAGER_NAME = "storeManager";
    private static final String METRICS_MERGED_STORE = "stores";
    private static final String METRICS_MERGED_CACHE = "caches";
    public static final String METRICS_CACHE_SUFFIX = ".cache";
    public static final String LOCK_STORE_SUFFIX = "_lock_";

    public static final String SYSTEM_TX_LOG_NAME = "txlog";
    private static final String SYSTEM_MGMT_LOG_NAME = "systemlog";

    private static final double EDGESTORE_CACHE_PERCENT = 0.8;
    private static final double INDEXSTORE_CACHE_PERCENT = 0.2;

    private static final long ETERNAL_CACHE_EXPIRATION = 1000L * 3600 * 24 * 365 * 200; //200 years

    private static final int THREAD_POOL_SIZE_SCALE_FACTOR = 2;

    private final KeyColumnValueStoreManager storeManager;
    private final KeyColumnValueStoreManager storeManagerLocking;
    private final StoreFeatures storeFeatures;

    private KCVSCache edgeStore;
    private KCVSCache indexStore;
    private KCVSCache txLogStore;
    private KCVSConfiguration systemConfig;
    private boolean hasAttemptedClose;

    private final StandardScanner scanner;

    private final KCVSLogManager managementLogManager;
    private final KCVSLogManager txLogManager;
    private final LogManager userLogManager;

    private final Map<String, IndexProvider> indexes;

    private final int bufferSize;
    private final Duration maxWriteTime;
    private final Duration maxReadTime;
    private final boolean cacheEnabled;
    private final ExecutorService threadPool;

    private final ConcurrentHashMap<String, Locker> lockers = new ConcurrentHashMap<>();
    private final Configuration configuration;

    public Backend(Configuration configuration, KeyColumnValueStoreManager manager) {
        this.configuration = configuration;

        storeManager = configuration.get(BASIC_METRICS) ? new MetricInstrumentedStoreManager(manager, METRICS_STOREMANAGER_NAME, configuration.get(METRICS_MERGE_STORES), METRICS_MERGED_STORE) : manager;

        indexes = getIndexes(configuration);
        storeFeatures = storeManager.getFeatures();

        managementLogManager = new KCVSLogManager(storeManager, configuration.restrictTo(MANAGEMENT_LOG));
        txLogManager = new KCVSLogManager(storeManager, configuration.restrictTo(TRANSACTION_LOG));
        userLogManager = new KCVSLogManager(storeManager, configuration.restrictTo(USER_LOG));

        cacheEnabled = !configuration.get(STORAGE_BATCH) && configuration.get(DB_CACHE);

        int bufferSizeTmp = configuration.get(BUFFER_SIZE);
        Preconditions.checkArgument(bufferSizeTmp > 0, "Buffer size must be positive");
        if (!storeFeatures.hasBatchMutation()) {
            bufferSize = Integer.MAX_VALUE;
        } else bufferSize = bufferSizeTmp;

        maxWriteTime = configuration.get(STORAGE_WRITE_WAITTIME);
        maxReadTime = configuration.get(STORAGE_READ_WAITTIME);

        if (!storeFeatures.hasLocking()) {
            Preconditions.checkArgument(storeFeatures.isKeyConsistent(), "Store needs to support some form of locking");
            storeManagerLocking = new ExpectedValueCheckingStoreManager(storeManager, LOCK_STORE_SUFFIX, this, maxReadTime);
        } else {
            storeManagerLocking = storeManager;
        }

        if (configuration.get(PARALLEL_BACKEND_OPS)) {
            int poolSize = Runtime.getRuntime().availableProcessors() * THREAD_POOL_SIZE_SCALE_FACTOR;
            threadPool = Executors.newFixedThreadPool(poolSize);
            LOG.debug("Initiated backend operations thread pool of size {}", poolSize);
        } else {
            threadPool = null;
        }

        scanner = new StandardScanner(storeManager);
        initialize();
    }

    //Method invoked by ExpectedValueCheckingStoreManager, which is only used when Backend does not support native locking.
    @Override
    public Locker getLocker(String lockerName) {
        Locker l = lockers.get(lockerName);
        if (null == l) {
            Locker locker = createLocker(lockerName);
            lockers.put(lockerName, locker);
            return locker;
        }
        return l;
    }

    /**
     * Initializes this backend with the given configuration.
     */
    private void initialize() {
        try {
            KeyColumnValueStore edgeStoreRaw = storeManagerLocking.openDatabase(EDGESTORE_NAME);
            KeyColumnValueStore indexStoreRaw = storeManagerLocking.openDatabase(INDEXSTORE_NAME);

            //Configure caches
            if (cacheEnabled) {
                long expirationTime = configuration.get(DB_CACHE_TIME);
                Preconditions.checkArgument(expirationTime >= 0, "Invalid cache expiration time: %s", expirationTime);
                if (expirationTime == 0) expirationTime = ETERNAL_CACHE_EXPIRATION;

                long cacheSizeBytes;
                double cacheSize = configuration.get(DB_CACHE_SIZE);
                Preconditions.checkArgument(cacheSize > 0.0, "Invalid cache size specified: %s", cacheSize);
                if (cacheSize < 1.0) {
                    //Its a percentage
                    Runtime runtime = Runtime.getRuntime();
                    cacheSizeBytes = (long) ((runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory())) * cacheSize);
                } else {
                    Preconditions.checkArgument(cacheSize > 1000, "Cache size is too small: %s", cacheSize);
                    cacheSizeBytes = (long) cacheSize;
                }
                LOG.debug("Configuring total store cache size: {}", cacheSizeBytes);
                long cleanWaitTime = configuration.get(DB_CACHE_CLEAN_WAIT);
                Preconditions.checkArgument(EDGESTORE_CACHE_PERCENT + INDEXSTORE_CACHE_PERCENT == 1.0, "Cache percentages don't add up!");
                long edgeStoreCacheSize = Math.round(cacheSizeBytes * EDGESTORE_CACHE_PERCENT);
                long indexStoreCacheSize = Math.round(cacheSizeBytes * INDEXSTORE_CACHE_PERCENT);

                edgeStore = new ExpirationKCVSCache(edgeStoreRaw, getMetricsCacheName(EDGESTORE_NAME), expirationTime, cleanWaitTime, edgeStoreCacheSize);
                indexStore = new ExpirationKCVSCache(indexStoreRaw, getMetricsCacheName(INDEXSTORE_NAME), expirationTime, cleanWaitTime, indexStoreCacheSize);
            } else {
                edgeStore = new NoKCVSCache(edgeStoreRaw);
                indexStore = new NoKCVSCache(indexStoreRaw);
            }

            //Just open them so that they are cached
            txLogManager.openLog(SYSTEM_TX_LOG_NAME);
            managementLogManager.openLog(SYSTEM_MGMT_LOG_NAME);
            txLogStore = new NoKCVSCache(storeManager.openDatabase(SYSTEM_TX_LOG_NAME));

            //Open global configuration
            KeyColumnValueStore systemConfigStore = storeManagerLocking.openDatabase(SYSTEM_PROPERTIES_STORE_NAME);
            KCVSConfigurationBuilder kcvsConfigurationBuilder = new KCVSConfigurationBuilder();
            systemConfig = kcvsConfigurationBuilder.buildGlobalConfiguration(new BackendOperation.TransactionalProvider() {
                @Override
                public StoreTransaction openTx() throws BackendException {
                    return storeManagerLocking.beginTransaction(StandardBaseTransactionConfig.of(configuration.get(TIMESTAMP_PROVIDER), storeFeatures.getKeyConsistentTxConfig()));
                }

                @Override
                public void close() {
                    //Do nothing, storeManager is closed explicitly by Backend
                }
            }, systemConfigStore, configuration);

        } catch (BackendException e) {
            throw new JanusGraphException("Could not initialize backend", e);
        }
    }

    /**
     * Get information about all registered {@link IndexProvider}s.
     */
    public Map<String, IndexInformation> getIndexInformation() {
        ImmutableMap.Builder<String, IndexInformation> copy = ImmutableMap.builder();
        copy.putAll(indexes);
        return copy.build();
    }

    public KCVSLog getSystemTxLog() {
        try {
            return txLogManager.openLog(SYSTEM_TX_LOG_NAME);
        } catch (BackendException e) {
            throw new JanusGraphException("Could not re-open transaction LOG", e);
        }
    }

    public Log getSystemMgmtLog() {
        try {
            return managementLogManager.openLog(SYSTEM_MGMT_LOG_NAME);
        } catch (BackendException e) {
            throw new JanusGraphException("Could not re-open management LOG", e);
        }
    }

    public StandardScanner.Builder buildEdgeScanJob() {
        return buildStoreIndexScanJob(EDGESTORE_NAME);
    }

    public StandardScanner.Builder buildGraphIndexScanJob() {
        return buildStoreIndexScanJob(INDEXSTORE_NAME);
    }

    private StandardScanner.Builder buildStoreIndexScanJob(String storeName) {
        TimestampProvider provider = configuration.get(TIMESTAMP_PROVIDER);
        ModifiableConfiguration jobConfig = buildJobConfiguration();
        jobConfig.set(JOB_START_TIME, provider.getTime().toEpochMilli());
        return scanner.build()
                .setStoreName(storeName)
                .setTimestampProvider(provider)
                .setJobConfiguration(jobConfig)
                .setGraphConfiguration(configuration)
                .setNumProcessingThreads(1)
                .setWorkBlockSize(10000);
    }

    public JanusGraphManagement.IndexJobFuture getScanJobStatus(Object jobId) {
        return scanner.getRunningJob(jobId);
    }

    public Log getUserLog(String identifier) throws BackendException {
        return userLogManager.openLog(getUserLogName(identifier));
    }

    private static String getUserLogName(String identifier) {
        Preconditions.checkArgument(StringUtils.isNotBlank(identifier));
        return USER_LOG_PREFIX + identifier;
    }

    public KCVSConfiguration getGlobalSystemConfig() {
        return systemConfig;
    }

    private String getMetricsCacheName(String storeName) {
        if (!configuration.get(BASIC_METRICS)) return null;
        return configuration.get(METRICS_MERGE_STORES) ? METRICS_MERGED_CACHE : storeName + METRICS_CACHE_SUFFIX;
    }

    private static Map<String, IndexProvider> getIndexes(Configuration config) {
        ImmutableMap.Builder<String, IndexProvider> builder = ImmutableMap.builder();
        for (String index : config.getContainedNamespaces(INDEX_NS)) {
            Preconditions.checkArgument(StringUtils.isNotBlank(index), "Invalid index name [%s]", index);
            LOG.debug("Configuring index [{}]", index);
            IndexProvider provider = getImplementationClass(config.restrictTo(index), config.get(INDEX_BACKEND, index),
                    StandardIndexProvider.getAllProviderClasses());
            Preconditions.checkNotNull(provider);
            builder.put(index, provider);
        }
        return builder.build();
    }

    public static <T> T getImplementationClass(Configuration config, String className, Map<String, String> registeredImplementations) {
        if (registeredImplementations.containsKey(className.toLowerCase())) {
            className = registeredImplementations.get(className.toLowerCase());
        }

        return ConfigurationUtil.instantiate(className, new Object[]{config}, new Class[]{Configuration.class});
    }

    public StoreFeatures getStoreFeatures() {
        return storeFeatures;
    }

    public Class<? extends KeyColumnValueStoreManager> getStoreManagerClass() {
        return storeManager.getClass();
    }

    public KeyColumnValueStoreManager getStoreManager() {
        return storeManager;
    }

    /**
     * Returns the {@link IndexFeatures} of all configured index backends
     */
    public Map<String, IndexFeatures> getIndexFeatures() {
        return Maps.transformValues(indexes, new Function<IndexProvider, IndexFeatures>() {
            @Nullable
            @Override
            public IndexFeatures apply(@Nullable IndexProvider indexProvider) {
                return indexProvider.getFeatures();
            }
        });
    }

    /**
     * Opens a new transaction against all registered backend system wrapped in one {@link BackendTransaction}.
     */
    public BackendTransaction beginTransaction(TransactionConfiguration configuration, KeyInformation.Retriever indexKeyRetriever) throws BackendException {

        StoreTransaction tx = storeManagerLocking.beginTransaction(configuration);

        // Cache
        CacheTransaction cacheTx = new CacheTransaction(tx, storeManagerLocking, bufferSize, maxWriteTime, configuration.hasEnabledBatchLoading());

        // Index transactions
        Map<String, IndexTransaction> indexTx = new HashMap<>(indexes.size());
        for (Map.Entry<String, IndexProvider> entry : indexes.entrySet()) {
            indexTx.put(entry.getKey(), new IndexTransaction(entry.getValue(), indexKeyRetriever.get(entry.getKey()), configuration, maxWriteTime));
        }

        return new BackendTransaction(cacheTx, configuration, storeFeatures, edgeStore, indexStore, txLogStore, maxReadTime, indexTx, threadPool);
    }

    public synchronized void close() throws BackendException {
        if (!hasAttemptedClose) {
            try {
                hasAttemptedClose = true;
                managementLogManager.close();
                txLogManager.close();
                userLogManager.close();

                scanner.close();
                if (edgeStore != null) edgeStore.close();
                if (indexStore != null) indexStore.close();
                if (systemConfig != null) systemConfig.close();
                //Indexes
                for (IndexProvider index : indexes.values()) index.close();
            } finally {
                storeManager.close();
                if (threadPool != null) {
                    threadPool.shutdown();
                }
            }
        } else {
            LOG.debug("Backend {} has already been closed or cleared", this);
        }
    }

    /**
     * Clears the storage of all registered backend data providers. This includes backend storage engines and index providers.
     * <p>
     * IMPORTANT: Clearing storage means that ALL data will be lost and cannot be recovered.
     */
    public synchronized void clearStorage() throws BackendException {
        if (!hasAttemptedClose) {
            hasAttemptedClose = true;
            managementLogManager.close();
            txLogManager.close();
            userLogManager.close();

            scanner.close();
            edgeStore.close();
            indexStore.close();
            systemConfig.close();
            storeManager.clearStorage();
            storeManager.close();
            //Indexes
            for (IndexProvider index : indexes.values()) {
                index.clearStorage();
                index.close();
            }
        } else {
            LOG.warn("Backend {} has already been closed or cleared", this);
        }
    }

    private ModifiableConfiguration buildJobConfiguration() {
        return new ModifiableConfiguration(JOB_NS, new CommonsConfiguration(new BaseConfiguration()),
                BasicConfiguration.Restriction.NONE);
    }

    private Locker createLocker(String lockerName) {
        KeyColumnValueStore lockerStore;
        try {
            lockerStore = storeManager.openDatabase(lockerName);
        } catch (BackendException e) {
            throw new JanusGraphConfigurationException("Could not retrieve store named " + lockerName + " for locker configuration", e);
        }
        return new ConsistentKeyLocker.Builder(lockerStore, storeManager).fromConfig(configuration).build();
    }
}
