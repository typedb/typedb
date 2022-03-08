/*
 * Copyright (C) 2021 Vaticle
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
 *
 */

package com.vaticle.typedb.core.database;

import com.vaticle.typedb.common.collection.ConcurrentSet;
import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.common.concurrent.NamedThreadFactory;
import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.graph.TypeGraph;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.common.KeyGenerator;
import com.vaticle.typedb.core.graph.common.StatisticsKey;
import com.vaticle.typedb.core.graph.common.Storage;
import com.vaticle.typedb.core.graph.iid.VertexIID;
import com.vaticle.typedb.core.graph.vertex.AttributeVertex;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.logic.LogicCache;
import com.vaticle.typedb.core.traversal.TraversalCache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Stream;

import static com.vaticle.typedb.common.collection.Collections.pair;
import static com.vaticle.typedb.core.common.collection.ByteArray.encodeLong;
import static com.vaticle.typedb.core.common.collection.ByteArray.encodeLongSet;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.DATABASE_CLOSED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.INCOMPATIBLE_ENCODING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.ROCKS_LOGGER_SHUTDOWN_FAILED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.DIRTY_INITIALISATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Session.SCHEMA_ACQUIRE_LOCK_TIMEOUT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_ISOLATION_DELETE_MODIFY_VIOLATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_ISOLATION_EXCLUSIVE_CREATE_VIOLATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_ISOLATION_MODIFY_DELETE_VIOLATION;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.parameters.Arguments.Session.Type.DATA;
import static com.vaticle.typedb.core.common.parameters.Arguments.Session.Type.SCHEMA;
import static com.vaticle.typedb.core.common.parameters.Arguments.Transaction.Type.READ;
import static com.vaticle.typedb.core.common.parameters.Arguments.Transaction.Type.WRITE;
import static com.vaticle.typedb.core.graph.common.Encoding.ENCODING_VERSION;
import static com.vaticle.typedb.core.graph.common.Encoding.System.ENCODING_VERSION_KEY;
import static java.util.Comparator.reverseOrder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class CoreDatabase implements TypeDB.Database {

    private static final Logger LOG = LoggerFactory.getLogger(CoreDatabase.class);
    private static final int ROCKS_LOG_PERIOD = 300;

    protected final RocksConfiguration rocksConfiguration;
    protected final KeyGenerator.Schema.Persisted schemaKeyGenerator;
    protected final KeyGenerator.Data.Persisted dataKeyGenerator;
    protected final ConcurrentMap<UUID, Pair<CoreSession, Long>> sessions;
    protected final String name;
    protected final AtomicBoolean isOpen;
    private final StampedLock schemaLock;
    private final CoreDatabaseManager databaseMgr;
    private final IsolationManager isolationMgr;
    private final StatisticsCompensator statisticsCompensator;
    private final Factory.Session sessionFactory;
    private final AtomicInteger schemaLockWriteRequests;
    private final AtomicLong transactionID;
    protected OptimisticTransactionDB rocksSchema;
    protected OptimisticTransactionDB rocksData;
    protected CorePartitionManager.Schema rocksSchemaPartitionMgr;
    protected CorePartitionManager.Data rocksDataPartitionMgr;
    protected CoreSession.Data statisticsBackgroundCounterSession;
    protected ScheduledExecutorService scheduledPropertiesLogger;
    private Cache cache;

    protected CoreDatabase(CoreDatabaseManager databaseMgr, String name, Factory.Session sessionFactory) {
        this.databaseMgr = databaseMgr;
        this.name = name;
        this.sessionFactory = sessionFactory;
        schemaKeyGenerator = new KeyGenerator.Schema.Persisted();
        dataKeyGenerator = new KeyGenerator.Data.Persisted();
        sessions = new ConcurrentHashMap<>();
        schemaLock = new StampedLock();
        schemaLockWriteRequests = new AtomicInteger(0);
        transactionID = new AtomicLong(0);
        isolationMgr = new IsolationManager();
        statisticsCompensator = new StatisticsCompensator();
        rocksConfiguration = new RocksConfiguration(options().storageDataCacheSize(),
                options().storageIndexCacheSize(), LOG.isDebugEnabled(), ROCKS_LOG_PERIOD);
        isOpen = new AtomicBoolean(false);
    }

    static CoreDatabase createAndOpen(CoreDatabaseManager databaseMgr, String name, Factory.Session sessionFactory) {
        try {
            Files.createDirectory(databaseMgr.directory().resolve(name));
        } catch (IOException e) {
            throw TypeDBException.of(e);
        }

        CoreDatabase database = new CoreDatabase(databaseMgr, name, sessionFactory);
        database.initialise();
        return database;
    }

    static CoreDatabase loadAndOpen(CoreDatabaseManager databaseMgr, String name, Factory.Session sessionFactory) {
        CoreDatabase database = new CoreDatabase(databaseMgr, name, sessionFactory);
        database.load();
        return database;
    }

    protected void initialise() {
        openSchema();
        initialiseEncodingVersion();
        openAndInitialiseData();
        isOpen.set(true);
        try (CoreSession.Schema session = createAndOpenSession(SCHEMA, new Options.Session()).asSchema()) {
            try (CoreTransaction.Schema txn = session.initialisationTransaction()) {
                if (txn.graph().isInitialised()) throw TypeDBException.of(DIRTY_INITIALISATION);
                txn.graph().initialise();
                txn.commit();
            }
        }
        statisticsCompensator.initialise();
    }

    private void openSchema() {
        try {
            List<ColumnFamilyDescriptor> schemaDescriptors = CorePartitionManager.Schema.descriptors(rocksConfiguration.schema());
            List<ColumnFamilyHandle> schemaHandles = new ArrayList<>();
            rocksSchema = OptimisticTransactionDB.open(
                    rocksConfiguration.schema().dbOptions(),
                    directory().resolve(Encoding.ROCKS_SCHEMA).toString(),
                    schemaDescriptors,
                    schemaHandles
            );
            rocksSchemaPartitionMgr = partitionManagerSchema(schemaDescriptors, schemaHandles);
        } catch (RocksDBException e) {
            throw TypeDBException.of(e);
        }
    }

    protected CorePartitionManager.Schema partitionManagerSchema(List<ColumnFamilyDescriptor> schemaDescriptors, List<ColumnFamilyHandle> schemaHandles) {
        return new CorePartitionManager.Schema(schemaDescriptors, schemaHandles);
    }

    private void openAndInitialiseData() {
        try {
            List<ColumnFamilyDescriptor> dataDescriptors = CorePartitionManager.Data.descriptors(rocksConfiguration.data());
            List<ColumnFamilyHandle> dataHandles = new ArrayList<>();
            rocksData = OptimisticTransactionDB.open(
                    rocksConfiguration.data().dbOptions(),
                    directory().resolve(Encoding.ROCKS_DATA).toString(),
                    dataDescriptors.subList(0, 1),
                    dataHandles
            );
            assert dataHandles.size() == 1;
            dataHandles.addAll(rocksData.createColumnFamilies(dataDescriptors.subList(1, dataDescriptors.size())));
            rocksDataPartitionMgr = partitionManagerData(dataDescriptors, dataHandles);
        } catch (RocksDBException e) {
            throw TypeDBException.of(e);
        }
        mayInitRocksDataLogger();
    }

    protected CorePartitionManager.Data partitionManagerData(List<ColumnFamilyDescriptor> dataDescriptors, List<ColumnFamilyHandle> dataHandles) {
        return new CorePartitionManager.Data(dataDescriptors, dataHandles);
    }

    protected void load() {
        openSchema();
        validateEncodingVersion();
        openData();
        isOpen.set(true);
        try (CoreSession.Schema session = createAndOpenSession(SCHEMA, new Options.Session()).asSchema()) {
            try (CoreTransaction.Schema txn = session.initialisationTransaction()) {
                schemaKeyGenerator.sync(txn.schemaStorage());
                dataKeyGenerator.sync(txn.schemaStorage(), txn.dataStorage());
            }
        }
        statisticsCompensator.initialiseAndCompensate();
    }

    private void openData() {
        try {
            List<ColumnFamilyDescriptor> dataDescriptors = CorePartitionManager.Data.descriptors(rocksConfiguration.data());
            List<ColumnFamilyHandle> dataHandles = new ArrayList<>();
            rocksData = OptimisticTransactionDB.open(
                    rocksConfiguration.data().dbOptions(),
                    directory().resolve(Encoding.ROCKS_DATA).toString(),
                    dataDescriptors,
                    dataHandles
            );
            assert dataDescriptors.size() == dataHandles.size();
            rocksDataPartitionMgr = partitionManagerData(dataDescriptors, dataHandles);
        } catch (RocksDBException e) {
            throw TypeDBException.of(e);
        }
        mayInitRocksDataLogger();
    }

    private void mayInitRocksDataLogger() {
        if (rocksConfiguration.isLoggingEnabled()) {
            scheduledPropertiesLogger = Executors.newScheduledThreadPool(1);
            scheduledPropertiesLogger.scheduleAtFixedRate(
                    new RocksProperties.Logger(rocksData, rocksDataPartitionMgr.handles, name),
                    0, ROCKS_LOG_PERIOD, SECONDS
            );
        } else {
            scheduledPropertiesLogger = null;
        }
    }

    private void initialiseEncodingVersion() {
        try {
            rocksSchema.put(
                    rocksSchemaPartitionMgr.get(Storage.Key.Partition.DEFAULT),
                    ENCODING_VERSION_KEY.bytes().getBytes(),
                    ByteArray.encodeInt(ENCODING_VERSION).getBytes()
            );
        } catch (RocksDBException e) {
            throw TypeDBException.of(e);
        }
    }

    private void validateEncodingVersion() {
        try {
            byte[] encodingBytes = rocksSchema.get(
                    rocksSchemaPartitionMgr.get(Storage.Key.Partition.DEFAULT),
                    ENCODING_VERSION_KEY.bytes().getBytes()
            );
            int encoding = encodingBytes == null || encodingBytes.length == 0 ? 0 : ByteArray.of(encodingBytes).decodeInt();
            if (encoding != ENCODING_VERSION) {
                throw TypeDBException.of(INCOMPATIBLE_ENCODING, name(), encoding, ENCODING_VERSION);
            }
        } catch (RocksDBException e) {
            throw TypeDBException.of(e);
        }
    }

    public CoreSession createAndOpenSession(Arguments.Session.Type type, Options.Session options) {
        if (!isOpen.get()) throw TypeDBException.of(DATABASE_CLOSED, name);

        long lock = 0;
        CoreSession session;

        if (type.isSchema()) {
            try {
                schemaLockWriteRequests.incrementAndGet();
                lock = schemaLock().tryWriteLock(options.schemaLockTimeoutMillis(), MILLISECONDS);
                if (lock == 0) throw TypeDBException.of(SCHEMA_ACQUIRE_LOCK_TIMEOUT);
            } catch (InterruptedException e) {
                throw TypeDBException.of(e);
            } finally {
                schemaLockWriteRequests.decrementAndGet();
            }
            session = sessionFactory.sessionSchema(this, options);
        } else if (type.isData()) {
            session = sessionFactory.sessionData(this, options);
        } else {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        sessions.put(session.uuid(), new Pair<>(session, lock));
        return session;
    }

    synchronized Cache cacheBorrow() {
        if (!isOpen.get()) throw TypeDBException.of(DATABASE_CLOSED, name);

        if (cache == null) cache = new Cache(this);
        cache.borrow();
        return cache;
    }

    synchronized void cacheUnborrow(Cache cache) {
        cache.unborrow();
    }

    public synchronized void cacheInvalidate() {
        if (!isOpen.get()) throw TypeDBException.of(DATABASE_CLOSED, name);

        if (cache != null) {
            cache.invalidate();
            cache = null;
        }
    }

    private synchronized void cacheClose() {
        if (cache != null) cache.close();
    }

    long nextTransactionID() {
        return transactionID.getAndIncrement();
    }

    protected Path directory() {
        return databaseMgr.directory().resolve(name);
    }

    public Options.Database options() {
        return databaseMgr.options();
    }

    KeyGenerator.Schema schemaKeyGenerator() {
        return schemaKeyGenerator;
    }

    KeyGenerator.Data dataKeyGenerator() {
        return dataKeyGenerator;
    }

    public IsolationManager isolationMgr() {
        return isolationMgr;
    }

    StatisticsCompensator statisticsCompensator() {
        return statisticsCompensator;
    }

    /**
     * Get the lock that guarantees that the schema is not modified at the same
     * time as data being written to the database. When a schema session is
     * opened (to modify the schema), all write transaction need to wait until
     * the schema session is completed. If there is a write transaction opened,
     * a schema session needs to wait until those transactions are completed.
     *
     * @return a {@code StampedLock} to protect data writes from concurrent schema modification
     */
    protected StampedLock schemaLock() {
        return schemaLock;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean contains(UUID sessionID) {
        return sessions.containsKey(sessionID);
    }

    @Override
    public TypeDB.Session session(UUID sessionID) {
        if (sessions.containsKey(sessionID)) return sessions.get(sessionID).first();
        else return null;
    }

    @Override
    public Stream<TypeDB.Session> sessions() {
        return sessions.values().stream().map(Pair::first);
    }

    @Override
    public String schema() {
        try (TypeDB.Session session = databaseMgr.session(name, DATA); TypeDB.Transaction tx = session.transaction(READ)) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("define\n\n");
            tx.concepts().exportTypes(stringBuilder);
            tx.logic().exportRules(stringBuilder);
            return stringBuilder.toString();
        }
    }

    void closed(CoreSession session) {
        if (session != statisticsBackgroundCounterSession) {
            long lock = sessions.remove(session.uuid()).second();
            if (session.type().isSchema()) schemaLock().unlockWrite(lock);
        }
    }

    protected void close() {
        if (isOpen.compareAndSet(true, false)) {
            if (scheduledPropertiesLogger != null) shutdownRocksPropertiesLogger();
            closeResources();
        }
    }

    private void shutdownRocksPropertiesLogger() {
        assert scheduledPropertiesLogger != null;
        try {
            scheduledPropertiesLogger.shutdown();
            boolean terminated = scheduledPropertiesLogger.awaitTermination(5, SECONDS);
            if (!terminated) throw TypeDBException.of(ROCKS_LOGGER_SHUTDOWN_FAILED);
        } catch (InterruptedException e) {
            throw TypeDBException.of(e);
        }
    }

    /**
     * Responsible for committing the initial schema of a database.
     * A different implementation of this class may override it.
     */
    protected void closeResources() {
        sessions.values().forEach(p -> p.first().close());
        statisticsCompensator.close();
        cacheClose();
        rocksDataPartitionMgr.close();
        rocksData.close();
        rocksSchemaPartitionMgr.close();
        rocksSchema.close();
    }

    @Override
    public void delete() {
        close();
        databaseMgr.remove(this);
        try {
            Files.walk(directory()).sorted(reverseOrder()).map(Path::toFile).forEach(File::delete);
        } catch (IOException e) {
            throw TypeDBException.of(e);
        }
    }

    public static class IsolationManager {

        private final ConcurrentMap<CoreTransaction.Data, CommitState> commitStates;
        private final ConcurrentNavigableMap<Long, Set<CoreTransaction.Data>> commitTimeline;
        private final AtomicBoolean cleanupRunning;

        enum CommitState {OPEN, COMMITTING, COMMITTED}

        IsolationManager() {
            this.cleanupRunning = new AtomicBoolean(false);
            this.commitStates = new ConcurrentHashMap<>();
            this.commitTimeline = new ConcurrentSkipListMap<>();
        }

        void opened(CoreTransaction.Data transaction) {
            commitStates.put(transaction, CommitState.OPEN);
        }

        long commitTimeNanos = 0;
        long mayViolateNanos = 0;
        long validationNanos = 0;
        long concurrent = 0;
        long commits = 0;

        public Set<CoreTransaction.Data> validateConcurrentAndStartCommit(CoreTransaction.Data txn) {
            Set<CoreTransaction.Data> transactions;
            synchronized (this) {
//                commits++;
//                Instant start = Instant.now();
                transactions = mayCommitConcurrently(txn);
//                Instant txns = Instant.now();
                transactions.forEach(other -> validateIsolation(txn, other));
//                Instant validation = Instant.now();
                commitStates.put(txn, CommitState.COMMITTING);
//                Instant end = Instant.now();
//                commitTimeNanos += Duration.between(start, end).toNanos();
//                mayViolateNanos += Duration.between(start, txns).toNanos();
//                validationNanos += Duration.between(txns, validation).toNanos();
//                concurrent += transactions.size();
//                if (commits % 1000 == 0) {
//                    LOG.warn("Mean synchronized nanos: " + (commitTimeNanos / (double) commits));
//                    LOG.warn("Mean get mayViolateIsolation nanos: " + (mayViolateNanos / (double) commits));
//                    LOG.warn("Mean validation nanos: " + (validationNanos / (double) commits));
//                    LOG.warn("Mean validated transactions: " + (concurrent / (double) commits));
//                }
            }
            return transactions;
        }

        private void validateIsolation(CoreTransaction.Data txn, CoreTransaction.Data mayConflict) {
            if (txn.dataStorage.modifyDeleteConflict(mayConflict.dataStorage)) {
                throw TypeDBException.of(TRANSACTION_ISOLATION_MODIFY_DELETE_VIOLATION);
            } else if (txn.dataStorage.deleteModifyConflict(mayConflict.dataStorage)) {
                throw TypeDBException.of(TRANSACTION_ISOLATION_DELETE_MODIFY_VIOLATION);
            } else if (txn.dataStorage.exclusiveCreateConflict(mayConflict.dataStorage)) {
                throw TypeDBException.of(TRANSACTION_ISOLATION_EXCLUSIVE_CREATE_VIOLATION);
            }
        }

        public void commitSucceeded(CoreTransaction.Data txn) {
            assert commitStates.get(txn) == CommitState.COMMITTING && txn.snapshotEnd().isPresent();
            commitStates.put(txn, CommitState.COMMITTED);
            commitTimeline.computeIfAbsent(txn.snapshotEnd().get(), (key) -> new HashSet<>()).add(txn);
        }

        public void closed(CoreTransaction.Data txn) {
            if (commitStates.get(txn) != CommitState.COMMITTED) commitStates.remove(txn);
            cleanupCommitted();
        }

        Set<CoreTransaction.Data> getUncommitted() {
            return iterate(commitStates.entrySet())
                    .filter(e -> e.getValue() == CommitState.OPEN || e.getValue() == CommitState.COMMITTING)
                    .map(Map.Entry::getKey).toSet();
        }

        private Set<CoreTransaction.Data> mayCommitConcurrently(CoreTransaction.Data txn) {
            return iterate(commitStates.entrySet())
                    .filter(e -> e.getValue() == CommitState.COMMITTING ||
                            (e.getValue() == CommitState.COMMITTED && e.getKey().snapshotEnd().get() > txn.snapshotStart()))
                    .map(Map.Entry::getKey)
                    .toSet();
        }

        // visible for testing
        public long committedEventCount() {
            return iterate(commitStates.values()).filter(s -> s == CommitState.COMMITTED).count();
        }

        private void cleanupCommitted() {
            if (cleanupRunning.compareAndSet(false, true)) {
                Optional<Long> oldestUncommittedSnapshot = oldestUncommittedSnapshot();
                ConcurrentNavigableMap<Long, Set<CoreTransaction.Data>> clearable;
                if (oldestUncommittedSnapshot.isEmpty()) clearable = commitTimeline;
                else clearable = commitTimeline.headMap(oldestUncommittedSnapshot.get());
                iterate(clearable.values()).flatMap(Iterators::iterate)
                        .forEachRemaining(txn -> {
                            txn.cleanUp();
                            commitStates.remove(txn);
                        });
                clearable.clear();
                cleanupRunning.set(false);
            }
        }

        private Optional<Long> oldestUncommittedSnapshot() {
            return iterate(commitStates.entrySet())
                    .filter(e -> e.getValue() == CommitState.OPEN || e.getValue() == CommitState.COMMITTING)
                    .map(e -> e.getKey().snapshotStart()).stream().min(Comparator.naturalOrder());
        }
    }

    static class Cache {

        private final TraversalCache traversalCache;
        private final LogicCache logicCache;
        private final TypeGraph typeGraph;
        private final RocksStorage schemaStorage;
        private long borrowerCount;
        private boolean invalidated;

        private Cache(CoreDatabase database) {
            schemaStorage = new RocksStorage.Cache(database.rocksSchema, database.rocksSchemaPartitionMgr);
            typeGraph = new TypeGraph(schemaStorage, true);
            traversalCache = new TraversalCache();
            logicCache = new LogicCache();
            borrowerCount = 0L;
            invalidated = false;
        }

        public TraversalCache traversal() {
            return traversalCache;
        }

        public LogicCache logic() {
            return logicCache;
        }

        public TypeGraph typeGraph() {
            return typeGraph;
        }

        private void borrow() {
            borrowerCount++;
        }

        private void unborrow() {
            borrowerCount--;
            mayClose();
        }

        private void invalidate() {
            invalidated = true;
            mayClose();
        }

        private void mayClose() {
            if (borrowerCount == 0 && invalidated) {
                schemaStorage.close();
            }
        }

        private void close() {
            schemaStorage.close();
        }
    }

    class StatisticsCompensator {

        private final ExecutorService executor;
        private final ConcurrentMap<CoreTransaction.Data, Set<CoreTransaction.Data>> dependencies;
        private final ConcurrentSet<Long> deletableTxnID;
        private CoreSession.Data session;

        StatisticsCompensator() {
            executor = Executors.newSingleThreadExecutor(NamedThreadFactory.create(name + "::statistics-compensator"));
            dependencies = new ConcurrentHashMap<>();
            this.deletableTxnID = new ConcurrentSet<>();
        }

        public void initialise() {
            session = createAndOpenSession(DATA, new Options.Session()).asData();
        }

        public void initialiseAndCompensate() {
            initialise();
            LOG.debug("Refreshing statistics.");
            compensate();
            deleteAllTxnIDs();
            LOG.debug("Statistics are up to date.");
            if (LOG.isDebugEnabled()) {
                logStatisticsSummary();
            }
        }

        private void logStatisticsSummary() {
            try (CoreTransaction.Data txn = session.transaction(READ)) {
                LOG.debug("Total 'thing' count: " +
                        txn.graphMgr.data().stats().thingVertexTransitiveCount(txn.graphMgr.schema().rootThingType())
                );
                long hasCount = 0;
                NavigableSet<TypeVertex> allTypes = txn.graphMgr.schema().getSubtypes(txn.graphMgr.schema().rootThingType());
                Set<TypeVertex> attributes = txn.graphMgr.schema().getSubtypes(txn.graphMgr.schema().rootAttributeType());
                for (TypeVertex attr : attributes) {
                    hasCount += txn.graphMgr.data().stats().hasEdgeSum(allTypes, attr);
                }
                LOG.debug("Total 'role' count: " +
                        txn.graphMgr.data().stats().thingVertexTransitiveCount(txn.graphMgr.schema().rootRoleType())
                );
                LOG.debug("Total 'has' count: " + hasCount);
            }
        }

        private void deleteAllTxnIDs() {
            try (CoreTransaction.Data txn = session.transaction(WRITE)) {
                txn.dataStorage.iterate(StatisticsKey.txnCommittedPrefix()).forEachRemaining(kv -> {
                    txn.dataStorage.deleteUntracked(kv.key());
                });
                txn.commit();
            }
        }

        public CompletableFuture<Void> mayCompensate(CoreTransaction.Data transaction) {
            if (dependencies.containsKey(transaction)) {
                return CompletableFuture.runAsync(() -> compensate(), executor);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }

        public CompletableFuture<Void> forceCompensate() {
            return CompletableFuture.runAsync(() -> compensate(), executor);
        }

        public void cleanUp(CoreTransaction.Data data) {
            dependencies.forEach((txn, deps) -> {
                if (deps.contains(data)) dependencies.compute(txn, (t, ds) -> {
                    ds.remove(data);
                    if (ds.isEmpty()) return null;
                    else return ds;
                });
            });
            deletableTxnID.add(data.id);
        }

        private void compensate() {
            Set<Long> uncommittedIDs = iterate(isolationMgr.getUncommitted()).map(txn -> txn.id).toSet();
            try (CoreTransaction.Data txn = session.transaction(WRITE)) {
                boolean[] updated = new boolean[]{false};
                txn.dataStorage.iterate(StatisticsKey.MisCount.prefix()).forEachRemaining(kv -> {
                    Set<Long> conditions = kv.value().decodeLongSet();
                    if (anyCommitted(conditions, txn.dataStorage)) {
                        txn.dataStorage.deleteUntracked(kv.key());
                        if (kv.key().isAttrConditionalOvercount()) {
                            VertexIID.Attribute<?> attribute = kv.key().attributeMiscounted();
                            VertexIID.Type type = attribute.type();
                            txn.dataStorage.mergeUntracked(StatisticsKey.vertexCount(type), encodeLong(-1));
                        } else if (kv.key().isAttrConditionalUndercount()) {
                            VertexIID.Attribute<?> attribute = kv.key().attributeMiscounted();
                            VertexIID.Type type = attribute.type();
                            txn.dataStorage.mergeUntracked(StatisticsKey.vertexCount(type), encodeLong(1));
                        } else if (kv.key().isHasConditionalOvercount()) {
                            Pair<VertexIID.Thing, VertexIID.Attribute<?>> has = kv.key().hasMiscounted();
                            txn.dataStorage.mergeUntracked(
                                    StatisticsKey.hasEdgeCount(has.first().type(), has.second().type()),
                                    encodeLong(-1)
                            );
                        } else if (kv.key().isHasConditionalUndercount()) {
                            Pair<VertexIID.Thing, VertexIID.Attribute<?>> has = kv.key().hasMiscounted();
                            txn.dataStorage.mergeUntracked(
                                    StatisticsKey.hasEdgeCount(has.first().type(), has.second().type()),
                                    encodeLong(1)
                            );
                        }
                        updated[0] = true;
                    } else if (iterate(conditions).noneMatch(uncommittedIDs::contains)) {
                        txn.dataStorage.deleteUntracked(kv.key());
                        updated[0] = true;
                    }
                });
                if (updated[0]) {
                    txn.dataStorage.mergeUntracked(StatisticsKey.snapshot(), encodeLong(1));
                    for (Long txnID : deletableTxnID) {
                        txn.dataStorage.deleteUntracked(StatisticsKey.txnCommitted(txnID));
                        deletableTxnID.remove(txnID);
                    }
                    txn.commit();
                }
            }
        }

        private boolean anyCommitted(Set<Long> txnIDs, RocksStorage.Data dataStorage) {
            for (Long txnID : txnIDs) {
                if (dataStorage.get(StatisticsKey.txnCommitted(txnID)) != null) return true;
            }
            return false;
        }

        void close() {
            session.close();
            executor.shutdownNow();
            try {
                // TODO
                executor.awaitTermination(1000, MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void writeMetadata(CoreTransaction.Data txn, Set<CoreTransaction.Data> concurrentTxn) {
            recordMiscountDependencies(txn, concurrentTxn);
            txn.dataStorage.putUntracked(StatisticsKey.txnCommitted(txn.id));
        }

        private void recordMiscountDependencies(CoreTransaction.Data txn, Set<CoreTransaction.Data> concurrentTxn) {
            Set<CoreTransaction.Data> txnDependencies = new HashSet<>();
            Map<AttributeVertex.Write<?>, Set<Long>> attrOvercountDependencies = new HashMap<>();
            Map<AttributeVertex.Write<?>, Set<Long>> attrUndercountDependencies = new HashMap<>();
            Map<Pair<ThingVertex.Write, AttributeVertex.Write<?>>, Set<Long>> hasOvercountDependencies = new HashMap<>();
            Map<Pair<ThingVertex.Write, AttributeVertex.Write<?>>, Set<Long>> hasUndercountDependencies = new HashMap<>();
            for (CoreTransaction.Data concurrent : concurrentTxn) {
                if (!txn.graphMgr.data().attributesCreated().isEmpty() && !concurrent.graphMgr.data().attributesCreated().isEmpty()) {
                    iterate(txn.graphMgr.data().attributesCreated()).filter(concurrent.graphMgr.data().attributesCreated()::contains)
                            .forEachRemaining(attribute -> {
                                        attrOvercountDependencies.computeIfAbsent(attribute, (key) -> new HashSet<>()).add(concurrent.id);
                                        txnDependencies.add(concurrent);
                                    }
                            );
                }
                if (!txn.graphMgr.data().attributesDeleted().isEmpty() && !concurrent.graphMgr.data().attributesDeleted().isEmpty()) {
                    iterate(txn.graphMgr.data().attributesDeleted()).filter(concurrent.graphMgr.data().attributesDeleted()::contains)
                            .forEachRemaining(attribute -> {
                                attrUndercountDependencies.computeIfAbsent(attribute, (key) -> new HashSet<>()).add(concurrent.id);
                                txnDependencies.add(concurrent);
                            });
                }
                if (!txn.graphMgr.data().hasEdgeCreated().isEmpty() && !concurrent.graphMgr.data().hasEdgeCreated().isEmpty()) {
                    iterate(txn.graphMgr.data().hasEdgeCreated()).filter(concurrent.graphMgr.data().hasEdgeCreated()::contains)
                            .forEachRemaining(edge -> {
                                hasOvercountDependencies.computeIfAbsent(
                                        pair(edge.from().asWrite(), edge.to().asAttribute().asWrite()),
                                        (key) -> new HashSet<>()
                                ).add(concurrent.id);
                                txnDependencies.add(concurrent);
                            });
                }
                if (!txn.graphMgr.data().hasEdgeDeleted().isEmpty() && !concurrent.graphMgr.data().hasEdgeDeleted().isEmpty()) {
                    iterate(txn.graphMgr.data().hasEdgeDeleted()).filter(concurrent.graphMgr.data().hasEdgeDeleted()::contains)
                            .forEachRemaining(edge -> {
                                hasUndercountDependencies.computeIfAbsent(
                                        pair(edge.from().asWrite(), edge.to().asAttribute().asWrite()),
                                        (key) -> new HashSet<>()
                                ).add(concurrent.id);
                                txnDependencies.add(concurrent);
                            });
                }
            }

            attrOvercountDependencies.forEach((attr, txs) ->
                    txn.dataStorage.putUntracked(StatisticsKey.MisCount.attrConditionalOvercount(txn.id, attr.iid()), encodeLongSet(txs))
            );
            attrUndercountDependencies.forEach((attr, txs) ->
                    txn.dataStorage.putUntracked(StatisticsKey.MisCount.attrConditionalUndercount(txn.id, attr.iid()), encodeLongSet(txs))
            );
            hasOvercountDependencies.forEach((has, txs) ->
                    txn.dataStorage.putUntracked(StatisticsKey.MisCount.hasConditionalOvercount(txn.id, has.first().iid(), has.second().iid()), encodeLongSet(txs))
            );
            hasUndercountDependencies.forEach((has, txs) ->
                    txn.dataStorage.putUntracked(StatisticsKey.MisCount.hasConditionalUndercount(txn.id, has.first().iid(), has.second().iid()), encodeLongSet(txs))
            );

            txnDependencies.forEach(dependency -> {
                dependencies.compute(txn, (t, deps) -> {
                    if (deps == null) deps = new HashSet<>();
                    deps.add(dependency);
                    return deps;
                });
            });
        }
    }

    private static class SchemaExporter {

    }
}
