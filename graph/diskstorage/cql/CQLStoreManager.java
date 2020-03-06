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

package grakn.core.graph.diskstorage.cql;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import com.datastax.oss.driver.internal.core.ssl.DefaultSslEngineFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import grakn.core.graph.core.JanusGraphException;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.BaseTransactionConfig;
import grakn.core.graph.diskstorage.PermanentBackendException;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.StoreMetaData.Container;
import grakn.core.graph.diskstorage.common.AbstractStoreManager;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.configuration.ModifiableConfiguration;
import grakn.core.graph.diskstorage.configuration.backend.CommonsConfiguration;
import grakn.core.graph.diskstorage.keycolumnvalue.KCVMutation;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyRange;
import grakn.core.graph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreFeatures;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;
import grakn.core.graph.diskstorage.util.time.TimestampProvider;
import io.vavr.Tuple;
import io.vavr.collection.Array;
import io.vavr.collection.HashMap;
import io.vavr.collection.Iterator;
import io.vavr.collection.Seq;
import io.vavr.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.truncate;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createKeyspace;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.dropKeyspace;
import static grakn.core.graph.diskstorage.cql.CQLConfigOptions.ATOMIC_BATCH_MUTATE;
import static grakn.core.graph.diskstorage.cql.CQLConfigOptions.KEYSPACE;
import static grakn.core.graph.diskstorage.cql.CQLConfigOptions.LOCAL_DATACENTER;
import static grakn.core.graph.diskstorage.cql.CQLConfigOptions.LOCAL_MAX_CONNECTIONS_PER_HOST;
import static grakn.core.graph.diskstorage.cql.CQLConfigOptions.MAX_REQUESTS_PER_CONNECTION;
import static grakn.core.graph.diskstorage.cql.CQLConfigOptions.PROTOCOL_VERSION;
import static grakn.core.graph.diskstorage.cql.CQLConfigOptions.READ_CONSISTENCY;
import static grakn.core.graph.diskstorage.cql.CQLConfigOptions.REMOTE_MAX_CONNECTIONS_PER_HOST;
import static grakn.core.graph.diskstorage.cql.CQLConfigOptions.REPLICATION_FACTOR;
import static grakn.core.graph.diskstorage.cql.CQLConfigOptions.REPLICATION_OPTIONS;
import static grakn.core.graph.diskstorage.cql.CQLConfigOptions.REPLICATION_STRATEGY;
import static grakn.core.graph.diskstorage.cql.CQLConfigOptions.SESSION_NAME;
import static grakn.core.graph.diskstorage.cql.CQLConfigOptions.SSL_ENABLED;
import static grakn.core.graph.diskstorage.cql.CQLConfigOptions.SSL_TRUSTSTORE_LOCATION;
import static grakn.core.graph.diskstorage.cql.CQLConfigOptions.SSL_TRUSTSTORE_PASSWORD;
import static grakn.core.graph.diskstorage.cql.CQLConfigOptions.WRITE_CONSISTENCY;
import static grakn.core.graph.diskstorage.cql.CQLKeyColumnValueStore.EXCEPTION_MAPPER;
import static grakn.core.graph.diskstorage.cql.CQLTransaction.getTransaction;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.AUTH_PASSWORD;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.AUTH_USERNAME;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.CONNECTION_TIMEOUT;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.DROP_ON_CLEAR;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.ROOT_NS;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_HOSTS;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_PORT;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.TIMESTAMP_PROVIDER;
import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.API.Match;

/**
 * This class creates see CQLKeyColumnValueStore CQLKeyColumnValueStores and handles Cassandra-backed allocation of vertex IDs for JanusGraph (when so
 * configured).
 */
public class CQLStoreManager extends AbstractStoreManager implements KeyColumnValueStoreManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(CQLStoreManager.class);

    private static final String CONSISTENCY_LOCAL_QUORUM = "LOCAL_QUORUM";
    static final String CONSISTENCY_QUORUM = "QUORUM";
    private static final int DEFAULT_PORT = 9042;

    private final String keyspace;
    private final boolean atomicBatch;
    private final TimestampProvider times;

    private CqlSession session;
    private final StoreFeatures storeFeatures;
    private final Map<String, CQLKeyColumnValueStore> openStores;
    private final Semaphore semaphore;

    /**
     * Constructor for the CQLStoreManager given a JanusGraph Configuration.
     */
    public CQLStoreManager(Configuration configuration) throws PermanentBackendException {
        super(configuration);
        this.keyspace = configuration.get(KEYSPACE);
        this.atomicBatch = configuration.get(ATOMIC_BATCH_MUTATE);
        this.times = configuration.get(TIMESTAMP_PROVIDER);
        this.semaphore = new Semaphore(configuration.get(MAX_REQUESTS_PER_CONNECTION));
        this.session = initialiseSession();

        initialiseKeyspace();

        Configuration global = buildGraphConfiguration()
                .set(READ_CONSISTENCY, CONSISTENCY_QUORUM)
                .set(WRITE_CONSISTENCY, CONSISTENCY_QUORUM);

        Configuration local = buildGraphConfiguration()
                .set(READ_CONSISTENCY, CONSISTENCY_LOCAL_QUORUM)
                .set(WRITE_CONSISTENCY, CONSISTENCY_LOCAL_QUORUM);


        StandardStoreFeatures.Builder fb = new StandardStoreFeatures.Builder();

        fb.batchMutation(true).distributed(true);
        fb.timestamps(true).cellTTL(true);
        fb.keyConsistent(global, local);
        fb.locking(false);
        fb.optimisticLocking(true);
        fb.multiQuery(false);

        String partitioner = this.session.getMetadata().getTokenMap().get().getPartitionerName();
        switch (partitioner.substring(partitioner.lastIndexOf('.') + 1)) {
            case "RandomPartitioner":
            case "Murmur3Partitioner": {
                fb.keyOrdered(false).orderedScan(false).unorderedScan(true);
                break;
            }
            case "ByteOrderedPartitioner": {
                fb.keyOrdered(true).orderedScan(true).unorderedScan(false);
                break;
            }
            default: {
                throw new IllegalArgumentException("Unrecognized partitioner: " + partitioner);
            }
        }
        this.storeFeatures = fb.build();
        this.openStores = new ConcurrentHashMap<>();
    }

    private CqlSession initialiseSession() throws PermanentBackendException {
        Configuration configuration = getStorageConfig();
        List<InetSocketAddress> contactPoints;
        String[] hostnames = configuration.get(STORAGE_HOSTS);
        int port = configuration.has(STORAGE_PORT) ? configuration.get(STORAGE_PORT) : DEFAULT_PORT;
        try {
            contactPoints = Array.of(hostnames)
                    .map(hostName -> hostName.split(":"))
                    .map(array -> Tuple.of(array[0], array.length == 2 ? Integer.parseInt(array[1]) : port))
                    .map(tuple -> new InetSocketAddress(tuple._1, tuple._2))
                    .toJavaList();
        } catch (SecurityException | ArrayIndexOutOfBoundsException | NumberFormatException e) {
            throw new PermanentBackendException("Error initialising cluster contact points", e);
        }

        CqlSessionBuilder builder = CqlSession.builder()
                .addContactPoints(contactPoints)
                .withLocalDatacenter(configuration.get(LOCAL_DATACENTER));

        ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder = DriverConfigLoader.programmaticBuilder();

        configLoaderBuilder.withString(DefaultDriverOption.SESSION_NAME, configuration.get(SESSION_NAME));
        configLoaderBuilder.withDuration(DefaultDriverOption.REQUEST_TIMEOUT, configuration.get(CONNECTION_TIMEOUT));

        if (configuration.get(PROTOCOL_VERSION) != 0) {
            configLoaderBuilder.withInt(DefaultDriverOption.PROTOCOL_VERSION, configuration.get(PROTOCOL_VERSION));
        }

        if (configuration.has(AUTH_USERNAME) && configuration.has(AUTH_PASSWORD)) {
            configLoaderBuilder
                    .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, PlainTextAuthProvider.class)
                    .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, configuration.get(AUTH_USERNAME))
                    .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, configuration.get(AUTH_PASSWORD));
        }

        if (configuration.get(SSL_ENABLED)) {
            configLoaderBuilder
                    .withClass(DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS, DefaultSslEngineFactory.class)
                    .withString(DefaultDriverOption.SSL_TRUSTSTORE_PATH, configuration.get(SSL_TRUSTSTORE_LOCATION))
                    .withString(DefaultDriverOption.SSL_TRUSTSTORE_PASSWORD, configuration.get(SSL_TRUSTSTORE_PASSWORD));
        }

        configLoaderBuilder.withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, configuration.get(LOCAL_MAX_CONNECTIONS_PER_HOST));
        configLoaderBuilder.withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, configuration.get(REMOTE_MAX_CONNECTIONS_PER_HOST));
        configLoaderBuilder.withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, configuration.get(MAX_REQUESTS_PER_CONNECTION));

        // Keep to 0 for the time being: https://groups.google.com/a/lists.datastax.com/forum/#!topic/java-driver-user/Bc0gQuOVVL0
        // Ideally we want to batch all tables initialisations to happen together when opening a new keyspace
        configLoaderBuilder.withInt(DefaultDriverOption.METADATA_SCHEMA_WINDOW, 0);

        // The following sets the size of Netty ThreadPool executor used by Cassandra driver:
        // https://docs.datastax.com/en/developer/java-driver/4.0/manual/core/async/#threading-model
        configLoaderBuilder.withInt(DefaultDriverOption.NETTY_IO_SIZE, 0); // size of threadpool scales with number of available CPUs when set to 0
        configLoaderBuilder.withInt(DefaultDriverOption.NETTY_ADMIN_SIZE, 0); // size of threadpool scales with number of available CPUs when set to 0


        // Keep the following values to 0 so that when we close the session we don't have to wait for the
        // so called "quiet period", setting this to a different value will slow down Graph.close()
        configLoaderBuilder.withInt(DefaultDriverOption.NETTY_ADMIN_SHUTDOWN_QUIET_PERIOD, 0);
        configLoaderBuilder.withInt(DefaultDriverOption.NETTY_IO_SHUTDOWN_QUIET_PERIOD, 0);

        builder.withConfigLoader(configLoaderBuilder.build());

        return builder.build();
    }

    private void initialiseKeyspace() {
        // if the keyspace already exists, just return
        if (this.session.getMetadata().getKeyspace(this.keyspace).isPresent()) {
            return;
        }

        Configuration configuration = getStorageConfig();

        // Setting replication strategy based on value reading from the configuration: either "SimpleStrategy" or "NetworkTopologyStrategy"
        Map<String, Object> replication = Match(configuration.get(REPLICATION_STRATEGY)).of(
                Case($("SimpleStrategy"), strategy -> HashMap.<String, Object>of("class", strategy, "replication_factor", configuration.get(REPLICATION_FACTOR))),
                Case($("NetworkTopologyStrategy"),
                        strategy -> HashMap.<String, Object>of("class", strategy)
                                .merge(Array.of(configuration.get(REPLICATION_OPTIONS))
                                        .grouped(2)
                                        .toMap(array -> Tuple.of(array.get(0), Integer.parseInt(array.get(1)))))))
                .toJavaMap();

        session.execute(createKeyspace(this.keyspace)
                .ifNotExists()
                .withReplicationOptions(replication)
                .build());
    }

    CqlSession getSession() {
        return this.session;
    }

    ResultSet executeOnSession(Statement statement) {
        try {
            this.semaphore.acquire();
            return this.session.execute(statement);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JanusGraphException("Interrupted while acquiring resource to execute query on Session.");
        } finally {
            this.semaphore.release();
        }
    }

    private CompletionStage<AsyncResultSet> executeAsyncOnSession(Statement statement) {
        try {
            this.semaphore.acquire();
            CompletionStage<AsyncResultSet> async = this.session.executeAsync(statement);
            async.handle((result, exception) -> {
                this.semaphore.release();
                if (exception != null) {
                    return exception;
                } else {
                    return result;
                }
            });
            return async;
        } catch (InterruptedException e) {
            this.semaphore.release();
            Thread.currentThread().interrupt();
            throw new JanusGraphException("Interrupted while acquiring resource to execute query on Session.");
        }
    }

    String getKeyspaceName() {
        return this.keyspace;
    }

    @VisibleForTesting
    Map<String, String> getCompressionOptions(String name) throws BackendException {
        KeyspaceMetadata keyspaceMetadata1 = this.session.getMetadata().getKeyspace(this.keyspace)
                .orElseThrow(() -> new PermanentBackendException(String.format("Unknown keyspace '%s'", this.keyspace)));

        TableMetadata tableMetadata = keyspaceMetadata1.getTable(name)
                .orElseThrow(() -> new PermanentBackendException(String.format("Unknown table '%s'", name)));

        Object compressionOptions = tableMetadata.getOptions().get(CqlIdentifier.fromCql("compression"));

        return (Map<String, String>) compressionOptions;
    }

    @VisibleForTesting
    TableMetadata getTableMetadata(String name) throws BackendException {
        KeyspaceMetadata keyspaceMetadata = (this.session.getMetadata().getKeyspace(this.keyspace))
                .orElseThrow(() -> new PermanentBackendException(String.format("Unknown keyspace '%s'", this.keyspace)));
        return keyspaceMetadata.getTable(name)
                .orElseThrow(() -> new PermanentBackendException(String.format("Unknown table '%s'", name)));
    }

    @Override
    public void close() {
        this.session.close();
    }

    @Override
    public String getName() {
        return String.format("%s.%s", getClass().getSimpleName(), this.keyspace);
    }

    @Override
    public StoreFeatures getFeatures() {
        return this.storeFeatures;
    }

    @Override
    public KeyColumnValueStore openDatabase(String name, Container metaData) throws BackendException {
        return this.openStores.computeIfAbsent(name, n -> new CQLKeyColumnValueStore(this, n, getStorageConfig(), () -> this.openStores.remove(n)));
    }

    @Override
    public StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException {
        return new CQLTransaction(config);
    }

    @Override
    public void clearStorage() throws BackendException {
        if (this.storageConfig.get(DROP_ON_CLEAR)) {
            this.session.execute(dropKeyspace(this.keyspace).build());
        } else if (this.exists()) {
            Future<Seq<AsyncResultSet>> result = Future.sequence(
                    Iterator.ofAll(this.session.getMetadata().getKeyspace(this.keyspace).get().getTables().values())
                            .map(table -> Future.fromJavaFuture(this.session.executeAsync(truncate(this.keyspace, table.getName().toString()).build())
                                    .toCompletableFuture())));
            result.await();
        } else {
            LOGGER.info("Keyspace {} does not exist in the cluster", this.keyspace);
        }
    }

    @Override
    public boolean exists() {
        return session.getMetadata().getKeyspace(this.keyspace).isPresent();
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        if (this.atomicBatch) {
            mutateManyLogged(mutations, txh);
        } else {
            mutateManyUnlogged(mutations, txh);
        }
    }

    // We never use Logged Batches: this is because they are a performance hit as they need to ensure that all statements succeed on all nodes.
    // Logged batches might be potentially be useful in the future, specially when mutating different tables
    // Brief explanation:
    //"Logged batches are used to ensure that all the statements will eventually succeed. Cassandra achieves this by first writing all the statements to a batch log.
    // That batch log is replicated to two other nodes in case the coordinator fails. If the coordinator fails then another replica for the batch log will take over."
    // Basically only useful when need to guarantee success in a multi-node cluster (so more a KGMS kind of thing).
    private void mutateManyLogged(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        MaskedTimestamp commitTime = new MaskedTimestamp(txh);
        long deletionTime = commitTime.getDeletionTime(this.times);
        long additionTime = commitTime.getAdditionTime(this.times);

        BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.LOGGED);
        builder.setConsistencyLevel(getTransaction(txh).getWriteConsistencyLevel());
        builder.addStatements(Iterator.ofAll(mutations.entrySet()).flatMap(tableNameAndMutations -> {
            String tableName = tableNameAndMutations.getKey();
            Map<StaticBuffer, KCVMutation> tableMutations = tableNameAndMutations.getValue();

            CQLKeyColumnValueStore columnValueStore = this.openStores.get(tableName);
            return Iterator.ofAll(tableMutations.entrySet()).flatMap(keyAndMutations -> {
                StaticBuffer key = keyAndMutations.getKey();
                KCVMutation keyMutations = keyAndMutations.getValue();

                Iterator<BatchableStatement<BoundStatement>> deletions = Iterator.ofAll(keyMutations.getDeletions()).map(deletion -> columnValueStore.deleteColumn(key, deletion, deletionTime));
                Iterator<BatchableStatement<BoundStatement>> additions = Iterator.ofAll(keyMutations.getAdditions()).map(addition -> columnValueStore.insertColumn(key, addition, additionTime));

                return Iterator.concat(deletions, additions);
            });
        }));
        CompletableFuture<AsyncResultSet> result = executeAsyncOnSession(builder.build()).toCompletableFuture();

        try {
            result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw EXCEPTION_MAPPER.apply(e);
        } catch (ExecutionException e) {
            throw EXCEPTION_MAPPER.apply(e);
        }
        sleepAfterWrite(commitTime);
    }

    // Create an async un-logged batch per partition key
    private void mutateManyUnlogged(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        MaskedTimestamp commitTime = new MaskedTimestamp(txh);
        long deletionTime = commitTime.getDeletionTime(this.times);
        long additionTime = commitTime.getAdditionTime(this.times);
        List<CompletableFuture<AsyncResultSet>> executionFutures = new ArrayList<>();
        ConsistencyLevel consistencyLevel = getTransaction(txh).getWriteConsistencyLevel();

        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> tableNameAndMutations : mutations.entrySet()) {
            String tableName = tableNameAndMutations.getKey();
            Map<StaticBuffer, KCVMutation> tableMutations = tableNameAndMutations.getValue();
            CQLKeyColumnValueStore columnValueStore = this.openStores.get(tableName);

            // Prepare one BatchStatement containing all the statements concerning the same partitioning key.
            // For correctness we must batch statements based on partition key otherwise we might end-up having batch of statements
            // containing queries that have to be executed on different cluster nodes.
            for (Map.Entry<StaticBuffer, KCVMutation> keyAndMutations : tableMutations.entrySet()) {
                StaticBuffer key = keyAndMutations.getKey();
                KCVMutation keyMutations = keyAndMutations.getValue();

                //Concatenate additions and deletions and map them to async session executions (completable futures) of resulting batch statement
                List<BatchableStatement<BoundStatement>> modifications = Stream.concat(
                        keyMutations.getDeletions().stream().map(deletion -> columnValueStore.deleteColumn(key, deletion, deletionTime)),
                        keyMutations.getAdditions().stream().map(addition -> columnValueStore.insertColumn(key, addition, additionTime))
                ).collect(Collectors.toList());

                CompletableFuture<AsyncResultSet> future = executeAsyncOnSession(
                        BatchStatement.newInstance(DefaultBatchType.UNLOGGED)
                                .addAll(modifications)
                                .setConsistencyLevel(consistencyLevel)
                ).toCompletableFuture();
                executionFutures.add(future);
            }
        }

        try {
            CompletableFuture.allOf(executionFutures.toArray(new CompletableFuture[]{})).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw EXCEPTION_MAPPER.apply(e);
        } catch (ExecutionException e) {
            throw EXCEPTION_MAPPER.apply(e);
        }
        sleepAfterWrite(commitTime);
    }

    /**
     * COMMIT MESSAGE SAYS: "Now instead of sleeping for a hardcoded millisecond it sleeps for a time based on its TimestampProvider
     * (but falls back to 1 ms if no provider is set).
     * Cassandra could get away without this method in its mutate implementations back when it used nanotime,
     * but with millisecond resolution (Timestamps.MILLI or .MICRO providers),
     * some of the tests routinely fail because multiple operations are colliding inside a single millisecond
     * (MultiWrite especially)."
     */
    private void sleepAfterWrite(MaskedTimestamp mustPass) throws BackendException {
        try {
            times.sleepPast(mustPass.getAdditionTimeInstant(times));
        } catch (InterruptedException e) {
            throw new PermanentBackendException("Unexpected interrupt", e);
        }
    }

    /**
     * Helper class to create the deletion and addition timestamps for a particular transaction.
     * It needs to be ensured that the deletion time is prior to the addition time since
     * some storage backends use the time to resolve conflicts.
     */
    private class MaskedTimestamp {

        private final Instant t;

        MaskedTimestamp(Instant commitTime) {
            Preconditions.checkNotNull(commitTime);
            this.t = commitTime;
        }

        private MaskedTimestamp(StoreTransaction txh) {
            this(txh.getConfiguration().getCommitTime());
        }

        private long getDeletionTime(TimestampProvider times) {
            return times.getTime(t) & 0xFFFFFFFFFFFFFFFEL; // zero the LSB
        }

        private long getAdditionTime(TimestampProvider times) {
            return (times.getTime(t) & 0xFFFFFFFFFFFFFFFEL) | 1L; // force the LSB to 1
        }

        private Instant getAdditionTimeInstant(TimestampProvider times) {
            return times.getTime(getAdditionTime(times));
        }
    }

    private ModifiableConfiguration buildGraphConfiguration() {
        return new ModifiableConfiguration(ROOT_NS, new CommonsConfiguration());
    }
}
