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
import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concurrent.executor.Executors;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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
import static com.vaticle.typedb.core.concurrent.executor.Executors.singleThreaded;
import static com.vaticle.typedb.core.graph.common.Encoding.ENCODING_VERSION;
import static com.vaticle.typedb.core.graph.common.Encoding.System.ENCODING_VERSION_KEY;
import static java.util.Comparator.reverseOrder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class CoreDatabase implements TypeDB.Database {

    private static final Logger LOG = LoggerFactory.getLogger(CoreDatabase.class);
    private static final int ROCKS_LOG_PERIOD = 300;

    private final CoreDatabaseManager databaseMgr;
    private final Factory.Session sessionFactory;
    protected final String name;
    protected final AtomicBoolean isOpen;
    private final AtomicLong nextTransactionID;
    private final AtomicInteger schemaLockWriteRequests;
    private final StampedLock schemaLock;
    protected final ConcurrentMap<UUID, Pair<CoreSession, Long>> sessions;
    protected final RocksConfiguration rocksConfiguration;
    protected final KeyGenerator.Schema.Persisted schemaKeyGenerator;
    protected final KeyGenerator.Data.Persisted dataKeyGenerator;
    private final IsolationManager isolationMgr;
    private final StatisticsCorrector statisticsCorrector;
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
        isolationMgr = new IsolationManager();
        statisticsCorrector = new StatisticsCorrector();
        sessions = new ConcurrentHashMap<>();
        rocksConfiguration = new RocksConfiguration(options().storageDataCacheSize(),
                options().storageIndexCacheSize(), LOG.isDebugEnabled(), ROCKS_LOG_PERIOD);
        schemaLock = new StampedLock();
        schemaLockWriteRequests = new AtomicInteger(0);
        nextTransactionID = new AtomicLong(0);
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
        statisticsCorrector.initialise();
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
        statisticsCorrector.initialiseAndSynchronise();
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
            scheduledPropertiesLogger = java.util.concurrent.Executors.newScheduledThreadPool(1);
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
        return nextTransactionID.getAndIncrement();
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

    StatisticsCorrector statisticsCorrector() {
        return statisticsCorrector;
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
        statisticsCorrector.close();
        Optional<CompletableFuture<Void>> correction = statisticsCorrector.runningCorrection();
        if (correction.isPresent()) {
            try {
                correction.get().get(Executors.SHUTDOWN_TIMEOUT_MS, MILLISECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                LOG.warn("Statistics corrector was not fully shutdown");
            }
        }
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

        public Set<CoreTransaction.Data> validateConcurrentAndStartCommit(CoreTransaction.Data txn) {
            Set<CoreTransaction.Data> transactions;
            synchronized (this) {
                transactions = mayCommitConcurrently(txn);
                transactions.forEach(other -> validateIsolation(txn, other));
                commitStates.put(txn, CommitState.COMMITTING);
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

        public void committed(CoreTransaction.Data txn) {
            assert commitStates.get(txn) == CommitState.COMMITTING && txn.snapshotEnd().isPresent();
            commitStates.put(txn, CommitState.COMMITTED);
            commitTimeline.compute(txn.snapshotEnd().get(), (snapshot, committed) -> {
                if (committed == null) committed = new HashSet<>();
                committed.add(txn);
                return committed;
            });
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

        private void cleanupCommitted() {
            if (cleanupRunning.compareAndSet(false, true)) {
                Optional<Long> oldestUncommittedSnapshot = oldestUncommittedSnapshot();
                ConcurrentNavigableMap<Long, Set<CoreTransaction.Data>> deletable;
                if (oldestUncommittedSnapshot.isEmpty()) deletable = commitTimeline;
                else deletable = commitTimeline.headMap(oldestUncommittedSnapshot.get());
                iterate(deletable.values()).flatMap(Iterators::iterate)
                        .forEachRemaining(txn -> {
                            txn.delete();
                            commitStates.remove(txn);
                        });
                deletable.clear();
                cleanupRunning.set(false);
            }
        }

        private Optional<Long> oldestUncommittedSnapshot() {
            return iterate(commitStates.entrySet())
                    .filter(e -> e.getValue() == CommitState.OPEN || e.getValue() == CommitState.COMMITTING)
                    .map(e -> e.getKey().snapshotStart()).stream().min(Comparator.naturalOrder());
        }

        public long committedEventCount() {
            return iterate(commitStates.values()).filter(s -> s == CommitState.COMMITTED).count();
        }
    }

    class StatisticsCorrector {

        private final ConcurrentSet<Long> deletableTransactionIDs;
        private final AtomicBoolean isCorrectionQueued;
        private AtomicReference<CompletableFuture<Void>> correctionFuture;
        private CoreSession.Data session;

        StatisticsCorrector() {
            deletableTransactionIDs = new ConcurrentSet<>();
            isCorrectionQueued = new AtomicBoolean(false);
            correctionFuture = new AtomicReference<>();
        }

        public void initialise() {
            session = createAndOpenSession(DATA, new Options.Session()).asData();
        }

        public void initialiseAndSynchronise() {
            initialise();
            LOG.debug("Synchronising statistics.");
            correctMiscounts();
            deleteCorrectionMetadata();
            LOG.debug("Statistics are up to date.");
            if (LOG.isDebugEnabled()) logStatisticsSummary();
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

        void close() {
            session.close();
        }

        private void deleteCorrectionMetadata() {
            try (CoreTransaction.Data txn = session.transaction(WRITE)) {
                txn.dataStorage.iterate(StatisticsKey.txnCommittedPrefix()).forEachRemaining(kv ->
                        txn.dataStorage.deleteUntracked(kv.key())
                );
                txn.commit();
            }
        }

        private boolean mayMiscount(CoreTransaction.Data transaction) {
            return !transaction.graphMgr.data().attributesCreated().isEmpty() ||
                    !transaction.graphMgr.data().attributesDeleted().isEmpty() ||
                    !transaction.graphMgr.data().hasEdgeCreated().isEmpty() ||
                    !transaction.graphMgr.data().hasEdgeDeleted().isEmpty();
        }

        void committed(CoreTransaction.Data transaction) {
            if (mayMiscount(transaction) && isCorrectionQueued.compareAndSet(false, true)) {
                submitCorrection();
            }
        }

        void deleted(CoreTransaction.Data transaction) {
            deletableTransactionIDs.add(transaction.id);
        }

        public CompletableFuture<Void> submitCorrection() {
            correctionFuture.set(CompletableFuture.runAsync(this::correctMiscounts, singleThreaded()));
            return correctionFuture.get();
        }

        public Optional<CompletableFuture<Void>> runningCorrection() {
            return Optional.ofNullable(correctionFuture.get());
        }

        /**
         * Scan through all attributes that may need to be corrected (eg. have been over/under counted),
         * and correct them if we have enough information to do so.
         */
        private void correctMiscounts() {
            isCorrectionQueued.set(false);
            try (CoreTransaction.Data txn = session.transaction(WRITE)) {
                boolean[] modified = new boolean[]{false};
                boolean[] miscountCorrected = new boolean[]{false};
                Set<Long> openTxnIDs = iterate(isolationMgr.getUncommitted()).map(t -> t.id).toSet();
                txn.dataStorage.iterate(StatisticsKey.Miscountable.prefix()).forEachRemaining(kv -> {
                    StatisticsKey.Miscountable item = kv.key();
                    Set<Long> txnIDsCausingMiscount = kv.value().decodeLongSet();

                    if (anyCommitted(txnIDsCausingMiscount, txn.dataStorage)) {
                        correctMiscount(item, txn);
                        miscountCorrected[0] = true;
                        txn.dataStorage.deleteUntracked(item);
                        modified[0] = true;
                    } else if (noneOpen(txnIDsCausingMiscount, openTxnIDs)) {
                        txn.dataStorage.deleteUntracked(item);
                        modified[0] = true;
                    }
                });
                if (modified[0]) {
                    if (miscountCorrected[0]) txn.dataStorage.mergeUntracked(StatisticsKey.snapshot(), encodeLong(1));
                    for (Long txnID : deletableTransactionIDs) {
                        txn.dataStorage.deleteUntracked(StatisticsKey.txnCommitted(txnID));
                    }
                    deletableTransactionIDs.clear();
                    txn.commit();
                }
            }
        }

        private void correctMiscount(StatisticsKey.Miscountable miscount, CoreTransaction.Data txn) {
            if (miscount.isOvercountableAttr()) {
                VertexIID.Type type = miscount.getMiscountableAttribute().type();
                txn.dataStorage.mergeUntracked(StatisticsKey.vertexCount(type), encodeLong(-1));
            } else if (miscount.isUndercountableAttr()) {
                VertexIID.Type type = miscount.getMiscountableAttribute().type();
                txn.dataStorage.mergeUntracked(StatisticsKey.vertexCount(type), encodeLong(1));
            } else if (miscount.isOvercountableHas()) {
                Pair<VertexIID.Thing, VertexIID.Attribute<?>> has = miscount.getMiscountableHas();
                txn.dataStorage.mergeUntracked(
                        StatisticsKey.hasEdgeCount(has.first().type(), has.second().type()),
                        encodeLong(-1)
                );
            } else if (miscount.isUndercountableHas()) {
                Pair<VertexIID.Thing, VertexIID.Attribute<?>> has = miscount.getMiscountableHas();
                txn.dataStorage.mergeUntracked(
                        StatisticsKey.hasEdgeCount(has.first().type(), has.second().type()),
                        encodeLong(1)
                );
            }
        }

        private boolean anyCommitted(Set<Long> txnIDsToCheck, RocksStorage.Data storage) {
            for (Long txnID : txnIDsToCheck) {
                if (storage.get(StatisticsKey.txnCommitted(txnID)) != null) return true;
            }
            return false;
        }

        private boolean noneOpen(Set<Long> txnIDs, Set<Long> openTxnIDs) {
            return iterate(txnIDs).noneMatch(openTxnIDs::contains);
        }

        public void recordCorrectionMetadata(CoreTransaction.Data txn, Set<CoreTransaction.Data> concurrentTxn) {
            recordMiscounts(txn, concurrentTxn);
            txn.dataStorage.putUntracked(StatisticsKey.txnCommitted(txn.id));
        }

        private void recordMiscounts(CoreTransaction.Data txn, Set<CoreTransaction.Data> concurrentTxn) {
            Map<AttributeVertex.Write<?>, Set<Long>> attrOvercountDependencies = new HashMap<>();
            Map<AttributeVertex.Write<?>, Set<Long>> attrUndercountDependencies = new HashMap<>();
            Map<Pair<ThingVertex.Write, AttributeVertex.Write<?>>, Set<Long>> hasOvercountDependencies = new HashMap<>();
            Map<Pair<ThingVertex.Write, AttributeVertex.Write<?>>, Set<Long>> hasUndercountDependencies = new HashMap<>();
            for (CoreTransaction.Data concurrent : concurrentTxn) {
                // note: fail-fast if checks are much faster than using empty iterators (probably due to concurrent data structures)
                if (!txn.graphMgr.data().attributesCreated().isEmpty() && !concurrent.graphMgr.data().attributesCreated().isEmpty()) {
                    iterate(txn.graphMgr.data().attributesCreated()).filter(concurrent.graphMgr.data().attributesCreated()::contains)
                            .forEachRemaining(attribute ->
                                    attrOvercountDependencies.computeIfAbsent(attribute, (key) -> new HashSet<>()).add(concurrent.id)

                            );
                }
                if (!txn.graphMgr.data().attributesDeleted().isEmpty() && !concurrent.graphMgr.data().attributesDeleted().isEmpty()) {
                    iterate(txn.graphMgr.data().attributesDeleted()).filter(concurrent.graphMgr.data().attributesDeleted()::contains)
                            .forEachRemaining(attribute ->
                                    attrUndercountDependencies.computeIfAbsent(attribute, (key) -> new HashSet<>()).add(concurrent.id)
                            );
                }
                if (!txn.graphMgr.data().hasEdgeCreated().isEmpty() && !concurrent.graphMgr.data().hasEdgeCreated().isEmpty()) {
                    iterate(txn.graphMgr.data().hasEdgeCreated()).filter(concurrent.graphMgr.data().hasEdgeCreated()::contains)
                            .forEachRemaining(edge ->
                                    hasOvercountDependencies.computeIfAbsent(
                                            pair(edge.from().asWrite(), edge.to().asAttribute().asWrite()),
                                            (key) -> new HashSet<>()
                                    ).add(concurrent.id)
                            );
                }
                if (!txn.graphMgr.data().hasEdgeDeleted().isEmpty() && !concurrent.graphMgr.data().hasEdgeDeleted().isEmpty()) {
                    iterate(txn.graphMgr.data().hasEdgeDeleted()).filter(concurrent.graphMgr.data().hasEdgeDeleted()::contains)
                            .forEachRemaining(edge ->
                                    hasUndercountDependencies.computeIfAbsent(
                                            pair(edge.from().asWrite(), edge.to().asAttribute().asWrite()),
                                            (key) -> new HashSet<>()
                                    ).add(concurrent.id)
                            );
                }
            }

            attrOvercountDependencies.forEach((attr, txs) ->
                    txn.dataStorage.putUntracked(StatisticsKey.Miscountable.attrOvercountable(txn.id, attr.iid()), encodeLongSet(txs))
            );
            attrUndercountDependencies.forEach((attr, txs) ->
                    txn.dataStorage.putUntracked(StatisticsKey.Miscountable.attrUndercountable(txn.id, attr.iid()), encodeLongSet(txs))
            );
            hasOvercountDependencies.forEach((has, txs) ->
                    txn.dataStorage.putUntracked(StatisticsKey.Miscountable.hasOvercountable(txn.id, has.first().iid(), has.second().iid()), encodeLongSet(txs))
            );
            hasUndercountDependencies.forEach((has, txs) ->
                    txn.dataStorage.putUntracked(StatisticsKey.Miscountable.hasUndercountable(txn.id, has.first().iid(), has.second().iid()), encodeLongSet(txs))
            );
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
                schemaStorage.closeResources();
            }
        }

        private void close() {
            schemaStorage.closeResources();
        }
    }

    private static class SchemaExporter {

    }
}
