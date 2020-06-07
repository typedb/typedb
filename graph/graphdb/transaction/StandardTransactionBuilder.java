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

package grakn.core.graph.graphdb.transaction;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.TransactionBuilder;
import grakn.core.graph.core.schema.DefaultSchemaMaker;
import grakn.core.graph.diskstorage.BaseTransactionConfig;
import grakn.core.graph.diskstorage.configuration.ConfigOption;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.util.StandardBaseTransactionConfig;
import grakn.core.graph.diskstorage.util.time.TimestampProvider;
import grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration;
import grakn.core.graph.graphdb.database.StandardJanusGraph;

import java.time.Instant;

/**
 * Used to configure a JanusGraphTransaction.
 */
public class StandardTransactionBuilder implements TransactionConfiguration, TransactionBuilder {

    private boolean isReadOnly = false;

    private boolean hasEnabledBatchLoading = false;

    private final boolean assignIDsImmediately;

    private final DefaultSchemaMaker defaultSchemaMaker;

    private boolean verifyExternalVertexExistence = true;

    private boolean verifyInternalVertexExistence = false;

    private final boolean propertyPrefetching;

    private boolean singleThreaded = false;

    private boolean threadBound = false;

    private int vertexCacheSize;

    private int dirtyVertexSize;

    private long indexCacheWeight;

    private String logIdentifier;

    private int[] restrictedPartitions = new int[0];

    private Instant userCommitTime = null;

    private final Configuration customOptions;

    private final StandardJanusGraph graph;

    /**
     * Constructs a new JanusGraphTransaction configuration with default configuration parameters.
     */
    public StandardTransactionBuilder(GraphDatabaseConfiguration graphConfig, StandardJanusGraph graph) {
        if (graphConfig.isBatchLoading()) enableBatchLoading();
        this.graph = graph;
        this.defaultSchemaMaker = graphConfig.getDefaultSchemaMaker();
        this.assignIDsImmediately = graphConfig.hasFlushIDs();
        this.logIdentifier = null;
        this.propertyPrefetching = graphConfig.hasPropertyPrefetching();
        this.customOptions = graphConfig.getConfiguration();
        vertexCacheSize(graphConfig.getTxVertexCacheSize());
        dirtyVertexSize(graphConfig.getTxDirtyVertexSize());
    }

    public StandardTransactionBuilder(GraphDatabaseConfiguration graphConfig, StandardJanusGraph graph, Configuration customOptions) {
        if (graphConfig.isBatchLoading()) enableBatchLoading();
        this.graph = graph;
        this.defaultSchemaMaker = graphConfig.getDefaultSchemaMaker();
        this.assignIDsImmediately = graphConfig.hasFlushIDs();
        this.logIdentifier = null;
        this.propertyPrefetching = graphConfig.hasPropertyPrefetching();
        this.customOptions = customOptions;
        vertexCacheSize(graphConfig.getTxVertexCacheSize());
        dirtyVertexSize(graphConfig.getTxDirtyVertexSize());
    }

    public StandardTransactionBuilder threadBound() {
        this.threadBound = true;
        this.singleThreaded = true;
        return this;
    }

    @Override
    public StandardTransactionBuilder readOnly() {
        this.isReadOnly = true;
        return this;
    }

    @Override
    public StandardTransactionBuilder enableBatchLoading() {
        hasEnabledBatchLoading = true;
        checkExternalVertexExistence(false);
        return this;
    }

    @Override
    public StandardTransactionBuilder disableBatchLoading() {
        hasEnabledBatchLoading = false;
        checkExternalVertexExistence(true);
        return this;
    }

    @Override
    public StandardTransactionBuilder vertexCacheSize(int size) {
        Preconditions.checkArgument(size >= 0);
        this.vertexCacheSize = size;
        this.indexCacheWeight = size / 2;
        return this;
    }

    @Override
    public TransactionBuilder dirtyVertexSize(int size) {
        this.dirtyVertexSize = size;
        return this;
    }

    @Override
    public StandardTransactionBuilder checkInternalVertexExistence(boolean enabled) {
        this.verifyInternalVertexExistence = enabled;
        return this;
    }

    @Override
    public StandardTransactionBuilder checkExternalVertexExistence(boolean enabled) {
        this.verifyExternalVertexExistence = enabled;
        return this;
    }

    @Override
    public StandardTransactionBuilder commitTime(Instant timestampSinceEpoch) {
        this.userCommitTime = timestampSinceEpoch;
        return this;
    }

    @Override
    public void setCommitTime(Instant time) {
        throw new UnsupportedOperationException("Use setCommitTime(long,TimeUnit)");
    }

    @Override
    public StandardTransactionBuilder logIdentifier(String logName) {
        this.logIdentifier = logName;
        return this;
    }

    @Override
    public TransactionBuilder restrictedPartitions(int[] partitions) {
        Preconditions.checkNotNull(partitions);
        this.restrictedPartitions = partitions;
        return this;
    }

    @Override
    public StandardJanusGraphTx start() {
        TransactionConfiguration immutable = new ImmutableTxCfg(isReadOnly, hasEnabledBatchLoading,
                assignIDsImmediately, verifyExternalVertexExistence,
                verifyInternalVertexExistence,
                propertyPrefetching, singleThreaded, threadBound, getTimestampProvider(), userCommitTime,
                indexCacheWeight, getVertexCacheSize(), getDirtyVertexSize(),
                logIdentifier, restrictedPartitions,
                defaultSchemaMaker, customOptions);
        return graph.newTransaction(immutable);
    }

    /* ##############################################
                    TransactionConfig
    ############################################## */


    @Override
    public final boolean isReadOnly() {
        return isReadOnly;
    }

    @Override
    public final boolean hasAssignIDsImmediately() {
        return assignIDsImmediately;
    }

    @Override
    public boolean hasEnabledBatchLoading() {
        return hasEnabledBatchLoading;
    }

    @Override
    public final boolean hasVerifyExternalVertexExistence() {
        return verifyExternalVertexExistence;
    }

    @Override
    public final boolean hasVerifyInternalVertexExistence() {
        return verifyInternalVertexExistence;
    }

    @Override
    public final DefaultSchemaMaker getAutoSchemaMaker() {
        return defaultSchemaMaker;
    }

    public boolean hasPropertyPrefetching() {
        return propertyPrefetching;
    }

    @Override
    public final boolean isSingleThreaded() {
        return singleThreaded;
    }

    @Override
    public final boolean isThreadBound() {
        return threadBound;
    }

    @Override
    public final int getVertexCacheSize() {
        return vertexCacheSize;
    }

    @Override
    public final int getDirtyVertexSize() {
        return dirtyVertexSize;
    }

    @Override
    public final long getIndexCacheWeight() {
        return indexCacheWeight;
    }

    @Override
    public String getLogIdentifier() {
        return logIdentifier;
    }

    @Override
    public int[] getRestrictedPartitions() {
        return restrictedPartitions;
    }

    @Override
    public boolean hasRestrictedPartitions() {
        return restrictedPartitions.length > 0;
    }

    @Override
    public Instant getCommitTime() {
        return userCommitTime;
    }

    @Override
    public boolean hasCommitTime() {
        return userCommitTime != null;
    }

    @Override
    public <V> V getCustomOption(ConfigOption<V> opt) {
        return getCustomOptions().get(opt);
    }

    @Override
    public Configuration getCustomOptions() {
        return customOptions;
    }

    @Override
    public TimestampProvider getTimestampProvider() {
        return graph.getConfiguration().getTimestampProvider();
    }

    private static class ImmutableTxCfg implements TransactionConfiguration {

        private final boolean isReadOnly;
        private final boolean hasEnabledBatchLoading;
        private final boolean hasAssignIDsImmediately;
        private final boolean hasVerifyExternalVertexExistence;
        private final boolean hasVerifyInternalVertexExistence;
        private final boolean hasPropertyPrefetching;
        private final boolean isSingleThreaded;
        private final boolean isThreadBound;
        private final long indexCacheWeight;
        private final int vertexCacheSize;
        private final int dirtyVertexSize;
        private final String logIdentifier;
        private final int[] restrictedPartitions;
        private final DefaultSchemaMaker defaultSchemaMaker;

        private final BaseTransactionConfig handleConfig;

        ImmutableTxCfg(boolean isReadOnly,
                       boolean hasEnabledBatchLoading,
                       boolean hasAssignIDsImmediately,
                       boolean hasVerifyExternalVertexExistence,
                       boolean hasVerifyInternalVertexExistence,
                       boolean hasPropertyPrefetching, boolean isSingleThreaded,
                       boolean isThreadBound, TimestampProvider times, Instant commitTime,
                       long indexCacheWeight, int vertexCacheSize, int dirtyVertexSize, String logIdentifier,
                       int[] restrictedPartitions,
                       DefaultSchemaMaker defaultSchemaMaker,
                       Configuration customOptions) {
            this.isReadOnly = isReadOnly;
            this.hasEnabledBatchLoading = hasEnabledBatchLoading;
            this.hasAssignIDsImmediately = hasAssignIDsImmediately;
            this.hasVerifyExternalVertexExistence = hasVerifyExternalVertexExistence;
            this.hasVerifyInternalVertexExistence = hasVerifyInternalVertexExistence;
            this.hasPropertyPrefetching = hasPropertyPrefetching;
            this.isSingleThreaded = isSingleThreaded;
            this.isThreadBound = isThreadBound;
            this.indexCacheWeight = indexCacheWeight;
            this.vertexCacheSize = vertexCacheSize;
            this.dirtyVertexSize = dirtyVertexSize;
            this.logIdentifier = logIdentifier;
            this.restrictedPartitions = restrictedPartitions;
            this.defaultSchemaMaker = defaultSchemaMaker;
            this.handleConfig = new StandardBaseTransactionConfig.Builder()
                    .commitTime(commitTime)
                    .timestampProvider(times)
                    .customOptions(customOptions).build();
        }

        @Override
        public boolean hasEnabledBatchLoading() {
            return hasEnabledBatchLoading;
        }

        @Override
        public boolean isReadOnly() {
            return isReadOnly;
        }

        @Override
        public boolean hasAssignIDsImmediately() {
            return hasAssignIDsImmediately;
        }

        @Override
        public boolean hasVerifyExternalVertexExistence() {
            return hasVerifyExternalVertexExistence;
        }

        @Override
        public boolean hasVerifyInternalVertexExistence() {
            return hasVerifyInternalVertexExistence;
        }

        @Override
        public DefaultSchemaMaker getAutoSchemaMaker() {
            return defaultSchemaMaker;
        }

        @Override
        public boolean hasPropertyPrefetching() {
            return hasPropertyPrefetching;
        }

        @Override
        public boolean isSingleThreaded() {
            return isSingleThreaded;
        }

        @Override
        public boolean isThreadBound() {
            return isThreadBound;
        }

        @Override
        public int getVertexCacheSize() {
            return vertexCacheSize;
        }

        @Override
        public int getDirtyVertexSize() {
            return dirtyVertexSize;
        }

        @Override
        public long getIndexCacheWeight() {
            return indexCacheWeight;
        }

        @Override
        public String getLogIdentifier() {
            return logIdentifier;
        }

        @Override
        public int[] getRestrictedPartitions() {
            return restrictedPartitions;
        }

        @Override
        public boolean hasRestrictedPartitions() {
            return restrictedPartitions.length > 0;
        }

        @Override
        public Instant getCommitTime() {
            return handleConfig.getCommitTime();
        }

        @Override
        public void setCommitTime(Instant time) {
            handleConfig.setCommitTime(time);
        }

        @Override
        public boolean hasCommitTime() {
            return handleConfig.hasCommitTime();
        }

        @Override
        public <V> V getCustomOption(ConfigOption<V> opt) {
            return handleConfig.getCustomOption(opt);
        }

        @Override
        public Configuration getCustomOptions() {
            return handleConfig.getCustomOptions();
        }

        @Override
        public TimestampProvider getTimestampProvider() {
            return handleConfig.getTimestampProvider();
        }
    }
}
