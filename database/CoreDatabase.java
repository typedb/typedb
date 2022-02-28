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

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.common.concurrent.NamedThreadFactory;
import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.graph.TypeGraph;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.common.KeyGenerator;
import com.vaticle.typedb.core.graph.common.Storage;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Stream;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.DATABASE_CLOSED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.INCOMPATIBLE_ENCODING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.ROCKS_LOGGER_SHUTDOWN_FAILED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.DIRTY_INITIALISATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNEXPECTED_INTERRUPTION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Session.SCHEMA_ACQUIRE_LOCK_TIMEOUT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_CONSISTENCY_DELETE_MODIFY_VIOLATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_CONSISTENCY_EXCLUSIVE_CREATE_VIOLATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_CONSISTENCY_MODIFY_DELETE_VIOLATION;
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
    private final ConsistencyManager consistencyMgr;
    private final AtomicInteger schemaLockWriteRequests;
    private final Factory.Session sessionFactory;
    protected OptimisticTransactionDB rocksSchema;
    protected OptimisticTransactionDB rocksData;
    protected CorePartitionManager.Schema rocksSchemaPartitionMgr;
    protected CorePartitionManager.Data rocksDataPartitionMgr;
    protected CoreSession.Data statisticsBackgroundCounterSession;
    protected StatisticsBackgroundCounter statisticsBackgroundCounter;
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
        consistencyMgr = new ConsistencyManager();
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
        database.statisticsBgCounterStart();
        return database;
    }

    static CoreDatabase loadAndOpen(CoreDatabaseManager databaseMgr, String name, Factory.Session sessionFactory) {
        CoreDatabase database = new CoreDatabase(databaseMgr, name, sessionFactory);
        database.load();
        database.statisticsBgCounterStart();
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

    protected void statisticsBgCounterStart() {
        assert statisticsBackgroundCounterSession == null;
        assert statisticsBackgroundCounter == null;

        statisticsBackgroundCounterSession = sessionFactory.sessionData(this, new Options.Session());
        statisticsBackgroundCounter = new StatisticsBackgroundCounter(statisticsBackgroundCounterSession);
    }

    protected void statisticsBgCounterStop() {
        assert statisticsBackgroundCounterSession != null;
        assert statisticsBackgroundCounter != null;

        statisticsBackgroundCounter.stop();
        statisticsBackgroundCounter = null;
        statisticsBackgroundCounterSession.close();
        statisticsBackgroundCounterSession = null;
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

    public ConsistencyManager consistencyMgr() {
        return consistencyMgr;
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
        statisticsBgCounterStop();
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

    /*
    Because we use multiple data structures, tracking and recording without synchronized is non-atomic. To avoid this,
    we move records first, then delete them -- this means intermediate readers may see a storage in two states. We
    avoid this causing issues by deduplicating into sets where otherwise we may have used iterators.
    This is preferable to intermediate readers not seeing the transaction at all, possibly causing consistency violations
     */
    public static class ConsistencyManager {

        private final ConcurrentNavigableMap<Long, ConcurrentMap<CoreTransaction.Data, Event>> events;
        private final ConcurrentMap<CoreTransaction.Data, CommitState> commitState;
        private final AtomicBoolean cleanupRunning;

        enum Event {OPENED, COMMITTED}

        enum CommitState {UNCOMMITTED, COMMITTING}

        ConsistencyManager() {
            this.events = new ConcurrentSkipListMap<>();
            this.commitState = new ConcurrentHashMap<>();
            this.cleanupRunning = new AtomicBoolean(false);
        }

        void opened(CoreTransaction.Data transaction) {
            events.computeIfAbsent(transaction.snapshotStart(), (key) -> new ConcurrentHashMap<>()).put(transaction, Event.OPENED);
            commitState.put(transaction, CommitState.UNCOMMITTED);
        }

        public void commitStarted(CoreTransaction.Data transaction) {
            commitState.put(transaction, CommitState.COMMITTING);
        }

        public void commitSuccessful(CoreTransaction.Data transaction) {
            assert commitState.get(transaction) == CommitState.COMMITTING && transaction.snapshotEnd().isPresent();
            events.compute(transaction.snapshotEnd().get(), (snapshot, events) -> {
                if (events == null) events = new ConcurrentHashMap<>();
                events.put(transaction, Event.COMMITTED);
                return events;
            });
            commitState.remove(transaction);
        }

        public void closed(CoreTransaction.Data transaction) {
            if (commitState.containsKey(transaction)) {
                commitState.remove(transaction);
                deleteOpenedEvent(transaction);
            } else {
                Map<CoreTransaction.Data, Event> events = this.events.get(transaction.snapshotEnd().get());
                if (events != null) {
                    Event event = events.get(transaction);
                    assert event == null || event == Event.COMMITTED;
                    if (events.get(transaction) != null && isDeletable(transaction)) {
                        deleteCommittedEvent(transaction);
                        deleteOpenedEvent(transaction);
                    }
                }
                cleanupCommitted();
            }
        }

        public Set<CoreTransaction.Data> commitMayConflict(CoreTransaction.Data transaction) {
            Set<CoreTransaction.Data> concurrent = iterate(commitState.keySet())
                    .filter(txn -> commitState.get(txn) == CommitState.COMMITTING).toSet();
            iterate(events.tailMap(transaction.snapshotStart() + 1).values())
                    .flatMap(events1 -> iterate(events1.entrySet()))
                    .filter(storageEvent -> storageEvent.getValue() == Event.COMMITTED)
                    .map(Map.Entry::getKey).toSet(concurrent);
            return concurrent;
        }

        // visible for testing
        public int committedEventCount() {
            Set<CoreTransaction.Data> recorded = new HashSet<>();
            this.events.forEach((snapshot, events) ->
                    events.forEach((storage, type) -> {
                        if (type == Event.COMMITTED) recorded.add(storage);
                    })
            );
            return recorded.size();
        }


        private boolean isDeletable(CoreTransaction.Data transaction) {
            assert transaction.snapshotEnd().isPresent() || commitState.containsKey(transaction);
            if (commitState.containsKey(transaction)) return false;
            // check for: open transactions that were opened before this one was committed
            Map<Long, ConcurrentMap<CoreTransaction.Data, Event>> beforeCommitted = events.headMap(transaction.snapshotEnd().get());
            for (CoreTransaction.Data uncommitted : commitState.keySet()) {
                ConcurrentMap<CoreTransaction.Data, Event> events = beforeCommitted.get(uncommitted.snapshotStart());
                if (events != null && events.containsKey(uncommitted) && events.get(uncommitted) == Event.OPENED) {
                    return false;
                }
            }
            return true;
        }

        private void deleteCommittedEvent(CoreTransaction.Data transaction) {
            events.compute(transaction.snapshotEnd().get(), (snapshot, events) -> {
                if (events != null) {
                    events.remove(transaction);
                    if (!events.isEmpty()) return events;
                }
                return null;
            });
        }

        private void deleteOpenedEvent(CoreTransaction.Data transaction) {
            events.compute(transaction.snapshotStart(), (snapshot, events) -> {
                if (events != null) {
                    events.remove(transaction);
                    if (!events.isEmpty()) return events;
                }
                return null;
            });
        }

        private void cleanupCommitted() {
            if (cleanupRunning.compareAndSet(false, true)) {
                for (Map.Entry<Long, ConcurrentMap<CoreTransaction.Data, Event>> entry : this.events.entrySet()) {
                    Long snapshot = entry.getKey();
                    ConcurrentMap<CoreTransaction.Data, Event> events = entry.getValue();
                    CoreTransaction.Data other;
                    for (Iterator<CoreTransaction.Data> iter = events.keySet().iterator(); iter.hasNext(); ) {
                        other = iter.next();
                        if (other.snapshotEnd().isPresent() && isDeletable(other)) iter.remove();
                    }
                    if (events.isEmpty()) this.events.remove(snapshot);
                }
                cleanupRunning.set(false);
            }
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

    public static class StatisticsBackgroundCounter {

        private final CoreSession.Data session;
        private final Thread thread;
        private final Semaphore countJobNotifications;
        private boolean isStopped;

        StatisticsBackgroundCounter(CoreSession.Data session) {
            this.session = session;
            countJobNotifications = new Semaphore(0);
            thread = NamedThreadFactory.create(session.database().name + "::statistics-background-counter")
                    .newThread(this::countFn);
            // TODO re-enable new statistics
//            thread.start();
        }

        public void needsBackgroundCounting() {
            countJobNotifications.release();
        }

        private void countFn() {
            do {
                try (CoreTransaction.Data tx = session.transaction(WRITE)) {
                    // TODO
//                    boolean shouldRestart = tx.graphMgr.data().stats().processCountJobs();
//                    if (shouldRestart) countJobNotifications.release();
                    tx.commit();
                } catch (TypeDBException e) {
                    if (e.code().isPresent() && e.code().get().equals(DATABASE_CLOSED.code())) {
                        break;
                    } else if (e.code().isPresent() && (
                            e.code().get().equals(TRANSACTION_CONSISTENCY_MODIFY_DELETE_VIOLATION.code()) ||
                                    e.code().get().equals(TRANSACTION_CONSISTENCY_DELETE_MODIFY_VIOLATION.code()) ||
                                    e.code().get().equals(TRANSACTION_CONSISTENCY_EXCLUSIVE_CREATE_VIOLATION.code())
                    )) {
                        countJobNotifications.release();
                    } else {
                        LOG.error("Background statistics counting received exception.", e);
                        countJobNotifications.release();
                    }
                }
                waitForCountJob();
                mayHoldBackForSchemaSession();
            } while (!isStopped);
        }

        private void waitForCountJob() {
            try {
                countJobNotifications.acquire();
            } catch (InterruptedException e) {
                throw TypeDBException.of(UNEXPECTED_INTERRUPTION);
            }
            countJobNotifications.drainPermits();
        }

        private void mayHoldBackForSchemaSession() {
            if (session.database().schemaLockWriteRequests.get() > 0) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw TypeDBException.of(UNEXPECTED_INTERRUPTION);
                }
            }
        }

        public void stop() {
            try {
                isStopped = true;
                countJobNotifications.release();
                thread.join();
            } catch (InterruptedException e) {
                throw TypeDBException.of(UNEXPECTED_INTERRUPTION);
            }
        }
    }

    private static class SchemaExporter {

    }
}
