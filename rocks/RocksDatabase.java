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

package com.vaticle.typedb.core.rocks;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.common.concurrent.NamedThreadFactory;
import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.exception.ErrorMessage;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.graph.TypeGraph;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.common.KeyGenerator;
import com.vaticle.typedb.core.logic.LogicCache;
import com.vaticle.typedb.core.traversal.TraversalCache;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Stream;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Database.DATABASE_CLOSED;
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
import static java.util.Comparator.reverseOrder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RocksDatabase implements TypeDB.Database {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDatabase.class);

    protected final OptimisticTransactionDB rocksSchema;
    protected final OptimisticTransactionDB rocksData;
    protected final ConcurrentMap<UUID, Pair<RocksSession, Long>> sessions;
    protected final String name;
    protected StatisticsBackgroundCounter statisticsBackgroundCounter;
    protected RocksSession.Data statisticsBackgroundCounterSession;
    protected final KeyGenerator.Schema.Persisted schemaKeyGenerator;
    protected final KeyGenerator.Data.Persisted dataKeyGenerator;
    private final StampedLock schemaLock;
    private final RocksTypeDB typedb;
    private final ConsistencyManager consistencyMgr;
    private final AtomicInteger schemaLockWriteRequests;
    private Cache cache;

    private final Factory.Session sessionFactory;
    protected final AtomicBoolean isOpen;

    protected RocksDatabase(RocksTypeDB typedb, String name, Factory.Session sessionFactory) {
        this.typedb = typedb;
        this.name = name;
        this.sessionFactory = sessionFactory;
        schemaKeyGenerator = new KeyGenerator.Schema.Persisted();
        dataKeyGenerator = new KeyGenerator.Data.Persisted();
        sessions = new ConcurrentHashMap<>();
        schemaLock = new StampedLock();
        schemaLockWriteRequests = new AtomicInteger(0);
        consistencyMgr = new ConsistencyManager();

        try {
            String schemaDirPath = directory().resolve(Encoding.ROCKS_SCHEMA).toString();
            String dataDirPath = directory().resolve(Encoding.ROCKS_DATA).toString();
            rocksSchema = OptimisticTransactionDB.open(this.typedb.rocksDBOptions(), schemaDirPath);
            rocksData = OptimisticTransactionDB.open(this.typedb.rocksDBOptions(), dataDirPath);
        } catch (RocksDBException e) {
            throw TypeDBException.of(e);
        }
        isOpen = new AtomicBoolean(true);
    }

    static RocksDatabase createAndOpen(RocksTypeDB typedb, String name, Factory.Session sessionFactory) {
        try {
            Files.createDirectory(typedb.directory().resolve(name));
        } catch (IOException e) {
            throw TypeDBException.of(e);
        }

        RocksDatabase database = new RocksDatabase(typedb, name, sessionFactory);
        database.initialise();
        database.statisticsBgCounterStart();
        return database;
    }

    static RocksDatabase loadAndOpen(RocksTypeDB typedb, String name, Factory.Session sessionFactory) {
        RocksDatabase database = new RocksDatabase(typedb, name, sessionFactory);
        database.load();
        database.statisticsBgCounterStart();
        return database;
    }

    protected void initialise() {
        try (RocksSession.Schema session = createAndOpenSession(SCHEMA, new Options.Session()).asSchema()) {
            try (RocksTransaction.Schema txn = session.initialisationTransaction()) {
                if (txn.graph().isInitialised()) throw TypeDBException.of(DIRTY_INITIALISATION);
                txn.graph().initialise();
                txn.commit();
            }
        }
    }

    protected void load() {
        try (RocksSession.Schema session = createAndOpenSession(SCHEMA, new Options.Session()).asSchema()) {
            try (RocksTransaction.Schema txn = session.initialisationTransaction()) {
                schemaKeyGenerator.sync(txn.schemaStorage());
                dataKeyGenerator.sync(txn.schemaStorage(), txn.dataStorage());
            }
        }
    }

    RocksSession createAndOpenSession(Arguments.Session.Type type, Options.Session options) {
        if (!isOpen.get()) throw TypeDBException.of(DATABASE_CLOSED, name);

        long lock = 0;
        RocksSession session;

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
        return typedb.directory().resolve(name);
    }

    public Options.Database options() {
        return typedb.options();
    }

    OptimisticTransactionDB rocksData() {
        return rocksData;
    }

    OptimisticTransactionDB rocksSchema() {
        return rocksSchema;
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
    StampedLock schemaLock() {
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
        try (TypeDB.Session session = typedb.session(name, DATA); TypeDB.Transaction tx = session.transaction(READ)) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("define\n\n");
            tx.concepts().exportTypes(stringBuilder);
            tx.logic().exportRules(stringBuilder);
            return stringBuilder.toString();
        }
    }

    void remove(RocksSession session) {
        if (session != statisticsBackgroundCounterSession) {
            long lock = sessions.remove(session.uuid()).second();
            if (session.type().isSchema()) schemaLock().unlockWrite(lock);
        }
    }

    void close() {
        if (isOpen.compareAndSet(true, false)) {
            closeResources();
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
        rocksData.close();
        rocksSchema.close();
    }

    @Override
    public void delete() {
        close();
        typedb.databases().remove(this);
        try {
            Files.walk(directory()).sorted(reverseOrder()).map(Path::toFile).forEach(File::delete);
        } catch (IOException e) {
            throw TypeDBException.of(e);
        }
    }

    /*
    In the consistency manager, we aim to only completely synchronize on commit. However,
    because we use multiple data structures, tracking and recording without synchronized is non-atomic. To avoid this,
    we move records first, then delete them -- this means intermediate readers may see a storage in two states. We
    avoid this causing issues by deduplicating into sets where otherwise we may have used iterators.
    This is preferable to intermediate readers not seeing the storage at all, possibly causing consistency violations
     */
    public static class ConsistencyManager {

        private final Timeline timeline;

        ConsistencyManager() {
            this.timeline = new Timeline();
        }

        void register(RocksStorage.Data storage) {
            timeline.opened(storage);
        }

        public void committed(RocksStorage.Data storage) {
            timeline.committed(storage);
        }

        public void closed(RocksStorage.Data storage) {
            timeline.closed(storage);
        }

        void tryOptimisticCommit(RocksStorage.Data storage) {
            assert timeline.isUncommitted(storage);
            Set<RocksStorage.Data> concurrent = timeline.concurrentCommitted(storage);
            synchronized (this) {
                for (RocksStorage.Data committed : concurrent) {
                    validateModifiedKeys(storage, committed);
                    requireEmptyIntersection(storage.deletedKeys(), committed.modifiedKeys(), TRANSACTION_CONSISTENCY_DELETE_MODIFY_VIOLATION);
                    requireEmptyIntersection(storage.exclusiveInsertKeys(), committed.exclusiveInsertKeys(), TRANSACTION_CONSISTENCY_EXCLUSIVE_CREATE_VIOLATION);
                }
                timeline.optimisticCommit(storage);
            }
        }

        private void validateModifiedKeys(RocksStorage.Data storage, RocksStorage.Data committed) {
            NavigableSet<ByteBuffer> active = storage.modifiedKeys();
            NavigableSet<ByteBuffer> other = committed.deletedKeys();
            if (active.isEmpty()) return;
            ByteBuffer currentKey = active.first();
            while (currentKey != null) {
                ByteBuffer otherKey = other.ceiling(currentKey);
                if (otherKey != null && otherKey.equals(currentKey)) {
                    if (storage.isModifiedValidatedKey(currentKey)) {
                        throw TypeDBException.of(TRANSACTION_CONSISTENCY_MODIFY_DELETE_VIOLATION);
                    } else {
                        otherKey = other.higher(currentKey);
                    }
                }
                currentKey = otherKey;
                NavigableSet<ByteBuffer> tmp = other;
                other = active;
                active = tmp;
            }
        }

        private void requireEmptyIntersection(NavigableSet<ByteBuffer> set1, NavigableSet<ByteBuffer> set2, ErrorMessage error) {
            NavigableSet<ByteBuffer> active = set1;
            NavigableSet<ByteBuffer> other = set2;
            if (active.isEmpty()) return;
            ByteBuffer currentKey = active.first();
            while (currentKey != null) {
                ByteBuffer otherKey = other.ceiling(currentKey);
                if (otherKey != null && otherKey.equals(currentKey)) throw TypeDBException.of(error);
                currentKey = otherKey;
                NavigableSet<ByteBuffer> tmp = other;
                other = active;
                active = tmp;
            }
        }

        ;

        // visible for testing
        public int recordedCommittedEvents() {
            return timeline.recordedCommittedEvents();
        }

        private static class Timeline {

            private final ConcurrentNavigableMap<Long, ConcurrentMap<RocksStorage.Data, EventType>> events;
            private final ConcurrentMap<RocksStorage.Data, UncommittedState> uncommitted;
            private final AtomicBoolean cleanupRunning;

            enum EventType {OPENED, COMMITTED}
            enum UncommittedState {OPEN, OPTIMISTICALLY_COMMITTED};

            public Timeline() {
                this.events = new ConcurrentSkipListMap<>();
                this.uncommitted = new ConcurrentHashMap<>();
                this.cleanupRunning = new AtomicBoolean(false);
            }

            void opened(RocksStorage.Data storage) {
                events.computeIfAbsent(storage.snapshotStart(), (key) -> new ConcurrentHashMap<>()).put(storage, EventType.OPENED);
                uncommitted.put(storage, UncommittedState.OPEN);
            }

            boolean isUncommitted(RocksStorage.Data storage) {
                return uncommitted.containsKey(storage);
            }

            void optimisticCommit(RocksStorage.Data storage) {
                uncommitted.put(storage, UncommittedState.OPTIMISTICALLY_COMMITTED);
            }

            void committed(RocksStorage.Data storage) {
                assert uncommitted.get(storage) == UncommittedState.OPTIMISTICALLY_COMMITTED && storage.snapshotEnd().isPresent();
                events.compute(storage.snapshotEnd().get(), (snapshot, events) -> {
                    if (events == null) events = new ConcurrentHashMap<>();
                    events.put(storage, EventType.COMMITTED);
                    return events;
                });
                uncommitted.remove(storage);
            }

            void closed(RocksStorage.Data storage) {
                if (uncommitted.containsKey(storage)) {
                    uncommitted.remove(storage);
                    deleteOpenEvent(storage);
                } else {
                    Map<RocksStorage.Data, EventType> events = this.events.get(storage.snapshotEnd().get());
                    assert events == null || events.get(storage) == null || events.get(storage) == EventType.COMMITTED;
                    if (events != null && events.get(storage) != null && isDeletable(storage)) {
                        deleteCommitted(storage);
                    }
                    cleanupCommitted();
                }
            }

            Set<RocksStorage.Data> concurrentCommitted(RocksStorage.Data storage) {
                if (!storage.snapshotEnd().isPresent()) return concurrentCommittedWithOpen(storage);
                else return concurrentCommittedWithCommitted(storage);
            }

            private Set<RocksStorage.Data> concurrentCommittedWithCommitted(RocksStorage.Data storage) {
                assert storage.snapshotEnd().isPresent();
                Set<RocksStorage.Data> concurrentCommitted = new HashSet<>();
                // include any storage opened or closed between storage's open-closed range
                events.subMap(storage.snapshotStart(), storage.snapshotEnd().get()).forEach((snapshot, events) -> {
                    events.forEach((evtStorage, eventType) -> {
                        if ((eventType == EventType.OPENED || snapshot != evtStorage.snapshotStart()) &&
                                evtStorage.snapshotEnd().isPresent()) {
                            concurrentCommitted.add(evtStorage);
                        }
                    });
                });
                // include any storage committed after storage is committed and opened before it is opened
                events.tailMap(storage.snapshotEnd().get()).forEach((snapshot, events) -> {
                    events.forEach((evtStorage, eventType) -> {
                        if (eventType == EventType.COMMITTED && evtStorage.snapshotStart() <= storage.snapshotStart()) {
                            concurrentCommitted.add(evtStorage);
                        }
                    });
                });
                return concurrentCommitted;
            }

            private Set<RocksStorage.Data> concurrentCommittedWithOpen(RocksStorage.Data storage) {
                assert !storage.snapshotEnd().isPresent();
                Set<RocksStorage.Data> concurrentCommitted = iterate(uncommitted.keySet())
                        .filter(s -> uncommitted.get(s) == UncommittedState.OPTIMISTICALLY_COMMITTED).toSet();
                events.tailMap(storage.snapshotStart() + 1).forEach((snapshot, events) -> {
                    events.forEach((s, type) -> {
                        if (type == EventType.COMMITTED) concurrentCommitted.add(s);
                    });
                });
                return concurrentCommitted;
            }

            private boolean isDeletable(RocksStorage.Data storage) {
                assert storage.snapshotEnd().isPresent() || uncommitted.containsKey(storage);
                if (uncommitted.containsKey(storage)) return false;
                // check for: open transactions that were opened before this one was committed
                Map<Long, ConcurrentMap<RocksStorage.Data, EventType>> beforeCommitted = events.headMap(storage.snapshotEnd().get());
                for (RocksStorage.Data uncommitted : uncommitted.keySet()) {
                    ConcurrentMap<RocksStorage.Data, EventType> events = beforeCommitted.get(uncommitted.snapshotStart());
                    if (events != null && events.containsKey(uncommitted) && events.get(uncommitted) == EventType.OPENED) {
                        return false;
                    }
                }
                return true;
            }

            private void deleteCommitted(RocksStorage.Data storage) {
                events.compute(storage.snapshotEnd().get(), (snapshot, events) -> {
                    if (events != null) {
                        events.remove(storage);
                        if (!events.isEmpty()) return events;
                    }
                    return null;
                });
                deleteOpenEvent(storage);
            }

            private void deleteOpenEvent(RocksStorage.Data storage) {
                events.compute(storage.snapshotStart(), (snapshot, events) -> {
                    if (events != null) {
                        events.remove(storage);
                        if (!events.isEmpty()) return events;
                    }
                    return null;
                });
            }

            private void cleanupCommitted() {
                if (cleanupRunning.compareAndSet(false, true)) {
                    for (Map.Entry<Long, ConcurrentMap<RocksStorage.Data, EventType>> entry : this.events.entrySet()) {
                        Long snapshot = entry.getKey();
                        ConcurrentMap<RocksStorage.Data, EventType> events = entry.getValue();
                        events.keySet().forEach(storage -> {
                            if (storage.snapshotEnd().isPresent() && isDeletable(storage)) {
                                events.remove(storage);
                            }
                        });
                        if (events.isEmpty()) this.events.remove(snapshot);
                    }
                    cleanupRunning.set(false);
                }
            }

            int recordedCommittedEvents() {
                Set<RocksStorage.Data> recorded = new HashSet<>();
                this.events.forEach((snapshot, events) -> {
                    events.forEach((storage, type) -> {
                        if (type == EventType.COMMITTED) recorded.add(storage);
                    });
                });
                return recorded.size();
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

        private Cache(RocksDatabase database) {
            schemaStorage = new RocksStorage.Cache(database.rocksSchema());
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

        private final RocksSession.Data session;
        private final Thread thread;
        private final Semaphore countJobNotifications;
        private boolean isStopped;

        StatisticsBackgroundCounter(RocksSession.Data session) {
            this.session = session;
            countJobNotifications = new Semaphore(0);
            thread = NamedThreadFactory.create(session.database().name + "::statistics-background-counter")
                    .newThread(this::countFn);
            thread.start();
        }

        public void needsBackgroundCounting() {
            countJobNotifications.release();
        }

        private void countFn() {
            do {
                try (RocksTransaction.Data tx = session.transaction(WRITE)) {
                    boolean shouldRestart = tx.graphMgr.data().stats().processCountJobs();
                    if (shouldRestart) countJobNotifications.release();
                    tx.commit();
                } catch (TypeDBException e) {
                    if (e.code().isPresent() && e.code().get().equals(DATABASE_CLOSED.code())) {
                        break;
                    } else {
                        // TODO: Add specific code indicating rocksdb conflict to TypeDBException status code
                        boolean txConflicted = e.getCause() instanceof RocksDBException &&
                                ((RocksDBException) e.getCause()).getStatus().getCode() == Status.Code.Busy;
                        if (txConflicted) {
                            countJobNotifications.release();
                        } else {
                            throw e;
                        }
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
