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
import com.google.common.collect.ImmutableMap;
import grakn.core.graph.core.JanusGraphException;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.configuration.backend.KCVSConfiguration;
import grakn.core.graph.diskstorage.configuration.backend.builder.KCVSConfigurationBuilder;
import grakn.core.graph.diskstorage.indexing.IndexInformation;
import grakn.core.graph.diskstorage.indexing.IndexProvider;
import grakn.core.graph.diskstorage.indexing.IndexTransaction;
import grakn.core.graph.diskstorage.indexing.KeyInformation;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreFeatures;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;
import grakn.core.graph.diskstorage.keycolumnvalue.cache.CacheTransaction;
import grakn.core.graph.diskstorage.keycolumnvalue.cache.KCVSCache;
import grakn.core.graph.diskstorage.keycolumnvalue.cache.KCVSExpirationCache;
import grakn.core.graph.diskstorage.keycolumnvalue.cache.KCVSNoCache;
import grakn.core.graph.diskstorage.log.Log;
import grakn.core.graph.diskstorage.log.LogManager;
import grakn.core.graph.diskstorage.log.kcvs.KCVSLog;
import grakn.core.graph.diskstorage.log.kcvs.KCVSLogManager;
import grakn.core.graph.diskstorage.util.BackendOperation;
import grakn.core.graph.diskstorage.util.StandardBaseTransactionConfig;
import grakn.core.graph.graphdb.transaction.TransactionConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.BUFFER_SIZE;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.DB_CACHE;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.DB_CACHE_CLEAN_WAIT;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.DB_CACHE_SIZE;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.DB_CACHE_TIME;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_NS;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.PARALLEL_BACKEND_OPS;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BATCH;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_READ_WAITTIME;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_WRITE_WAITTIME;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.SYSTEM_PROPERTIES_STORE_NAME;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.TIMESTAMP_PROVIDER;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.TRANSACTION_LOG;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.USER_LOG;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.USER_LOG_PREFIX;

/**
 * Orchestrates and configures all backend systems:
 * The primary backend storage (KeyColumnValueStore) and all external indexing providers (IndexProvider).
 */
public class Backend {

    private static final Logger LOG = LoggerFactory.getLogger(Backend.class);
    /**
     * These are the names for the edge store and property index databases, respectively.
     * The edge store contains all edges and properties. The property index contains an
     * inverted index from attribute value to vertex.
     * <p>
     * These names are fixed and should NEVER be changed. Changing these strings can
     * disrupt storage adapters that rely on these names for specific configurations.
     */
    public static final String EDGESTORE_NAME = "edgestore";
    public static final String INDEXSTORE_NAME = "graphindex";
    public static final String IDSTORE_NAME = "janusgraph_ids";

    public static final String SYSTEM_TX_LOG_NAME = "txlog";

    // The sum of the following 2 fields should be 1
    private static final double EDGESTORE_CACHE_PERCENT = 0.8;
    private static final double INDEXSTORE_CACHE_PERCENT = 0.2;
    private static final long ETERNAL_CACHE_EXPIRATION = 1000L * 3600 * 24 * 365 * 200; //200 years

    private final KeyColumnValueStoreManager storeManager;
    private final StoreFeatures storeFeatures;
    private final KCVSCache edgeStore;
    private final KCVSCache indexStore;
    private final KCVSCache txLogStore;
    private final KCVSConfiguration systemConfig;
    private final KCVSLogManager txLogManager;
    private final LogManager userLogManager;
    private final Map<String, IndexProvider> indexes;
    private final int bufferSize;
    private final Duration maxWriteTime;
    private final Duration maxReadTime;
    private final ExecutorService threadPool;
    private final Configuration config;

    private boolean hasAttemptedClose;

    public Backend(Configuration configuration, KeyColumnValueStoreManager manager) {
        config = configuration;
        storeManager = manager;
        indexes = getIndexes(configuration);
        storeFeatures = storeManager.getFeatures(); // features describing actual capabilities of actual backend engine
        txLogManager = new KCVSLogManager(storeManager, configuration.restrictTo(TRANSACTION_LOG)); //KCVStore where tx LOG will be persisted
        userLogManager = new KCVSLogManager(storeManager, configuration.restrictTo(USER_LOG));
        bufferSize = configuration.get(BUFFER_SIZE);
        maxWriteTime = configuration.get(STORAGE_WRITE_WAITTIME);
        maxReadTime = configuration.get(STORAGE_READ_WAITTIME);

        //Potentially useless threadpool: investigate!
        if (configuration.get(PARALLEL_BACKEND_OPS)) {
            int poolSize = Runtime.getRuntime().availableProcessors() * 2;
            threadPool = Executors.newFixedThreadPool(poolSize);
        } else {
            threadPool = null;
        }

        try {
            KeyColumnValueStore edgeStoreRaw = storeManager.openDatabase(EDGESTORE_NAME);
            KeyColumnValueStore indexStoreRaw = storeManager.openDatabase(INDEXSTORE_NAME);

            //If DB cache is enabled (and we are not batch loading) initialise caches and use KCVStore with inner cache
            if (!configuration.get(STORAGE_BATCH) && configuration.get(DB_CACHE)) {
                long expirationTime = configuration.get(DB_CACHE_TIME);
                if (expirationTime == 0) expirationTime = ETERNAL_CACHE_EXPIRATION;

                long cleanWaitTime = configuration.get(DB_CACHE_CLEAN_WAIT);
                long cacheSizeBytes = computeCacheSizeBytes();
                long edgeStoreCacheSize = Math.round(cacheSizeBytes * EDGESTORE_CACHE_PERCENT);
                long indexStoreCacheSize = Math.round(cacheSizeBytes * INDEXSTORE_CACHE_PERCENT);

                edgeStore = new KCVSExpirationCache(edgeStoreRaw, expirationTime, cleanWaitTime, edgeStoreCacheSize);
                indexStore = new KCVSExpirationCache(indexStoreRaw, expirationTime, cleanWaitTime, indexStoreCacheSize);
            } else {
                edgeStore = new KCVSNoCache(edgeStoreRaw);
                indexStore = new KCVSNoCache(indexStoreRaw);
            }

            txLogStore = new KCVSNoCache(storeManager.openDatabase(SYSTEM_TX_LOG_NAME));

            //Open global configuration
            KeyColumnValueStore systemConfigStore = storeManager.openDatabase(SYSTEM_PROPERTIES_STORE_NAME);
            StandardBaseTransactionConfig txConfig = StandardBaseTransactionConfig.of(configuration.get(TIMESTAMP_PROVIDER), storeFeatures.getKeyConsistentTxConfig());
            BackendOperation.TransactionalProvider txProvider = BackendOperation.buildTxProvider(storeManager, txConfig);
            systemConfig = new KCVSConfigurationBuilder().buildGlobalConfiguration(txProvider, systemConfigStore, configuration);

        } catch (BackendException e) {
            throw new JanusGraphException("Could not initialize backend", e);
        }
    }

    private long computeCacheSizeBytes() {
        long cacheSizeBytes;
        double cacheSize = config.get(DB_CACHE_SIZE);
        Preconditions.checkArgument(cacheSize > 0.0, "Invalid cache size specified: %s", cacheSize);
        if (cacheSize < 1.0) {
            //Its a percentage
            Runtime runtime = Runtime.getRuntime();
            cacheSizeBytes = (long) ((runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory())) * cacheSize);
        } else {
            Preconditions.checkArgument(cacheSize > 1000, "Cache size is too small: %s", cacheSize);
            cacheSizeBytes = (long) cacheSize;
        }
        return cacheSizeBytes;
    }

    /**
     * Get information about all registered IndexProviders.
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

    public KeyColumnValueStore getIDsStore() {
        try {
            return storeManager.openDatabase(IDSTORE_NAME);
        } catch (BackendException e) {
            throw new JanusGraphException("Could not open IDs Store", e);
        }
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

    private Map<String, IndexProvider> getIndexes(Configuration config) {
        ImmutableMap.Builder<String, IndexProvider> builder = ImmutableMap.builder();
        for (String index : config.getContainedNamespaces(INDEX_NS)) {
            Preconditions.checkArgument(StringUtils.isNotBlank(index), "Invalid index name [%s]", index);
            LOG.debug("Configuring index [{}]", index);
            IndexProvider provider = getIndexProviderClass(config.restrictTo(index), config.get(INDEX_BACKEND, index),
                    StandardIndexProvider.getAllProviderClasses());
            builder.put(index, provider);
        }
        return builder.build();
    }

    private IndexProvider getIndexProviderClass(Configuration config, String className, Map<String, String> registeredImplementations) {
        if (registeredImplementations.containsKey(className.toLowerCase())) {
            className = registeredImplementations.get(className.toLowerCase());
        }

        try {
            Class clazz = Class.forName(className);
            Constructor constructor = clazz.getConstructor(Configuration.class);
            return (IndexProvider) constructor.newInstance(new Object[]{config});
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not find IndexProvider class: " + className, e);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("IndexProvider class does not have required constructor: " + className, e);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | ClassCastException e) {
            throw new IllegalArgumentException("Could not instantiate IndexProvider: " + className, e);
        }
    }

    public StoreFeatures getStoreFeatures() {
        return storeFeatures;
    }

    public KeyColumnValueStoreManager getStoreManager() {
        return storeManager;
    }

    /**
     * Opens a new transaction against all registered backend system wrapped in one BackendTransaction.
     */
    public BackendTransaction beginTransaction(TransactionConfiguration configuration, KeyInformation.Retriever indexKeyRetriever) throws BackendException {
        StoreTransaction tx = storeManager.beginTransaction(configuration);
        CacheTransaction cacheTx = new CacheTransaction(tx, storeManager, bufferSize, maxWriteTime, configuration.hasEnabledBatchLoading());

        // Index transactions
        Map<String, IndexTransaction> indexTx = new HashMap<>(indexes.size());
        for (Map.Entry<String, IndexProvider> entry : indexes.entrySet()) {
            indexTx.put(entry.getKey(), new IndexTransaction(entry.getValue(), indexKeyRetriever.get(entry.getKey()), configuration, maxWriteTime));
        }

        return new BackendTransaction(cacheTx, configuration, storeFeatures, edgeStore, indexStore, txLogStore, maxReadTime, indexTx, threadPool);
    }

    public synchronized void close() {
        if (!hasAttemptedClose) {
            try {
                hasAttemptedClose = true;
                txLogManager.close();
                userLogManager.close();
                edgeStore.close();
                indexStore.close();
                systemConfig.close();
                //Indexes
                for (IndexProvider index : indexes.values()) index.close();
            } catch (Exception e) {
                LOG.warn("Exception while closing Backend." + e);
            } finally {
                try {
                    storeManager.close();
                } catch (BackendException e) {
                    LOG.warn("Exception while closing StoreManager." + e);
                }
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
     * IMPORTANT: Clearing storage means that ALL data will be lost and cannot be recovered.
     */
    public synchronized void clearStorage() throws BackendException {
        if (!hasAttemptedClose) {
            hasAttemptedClose = true;
            txLogManager.close();
            userLogManager.close();
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
}
