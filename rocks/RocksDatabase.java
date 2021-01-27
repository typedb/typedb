/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.rocks;

import grakn.common.collection.Pair;
import grakn.common.concurrent.NamedThreadFactory;
import grakn.core.Grakn;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Options;
import grakn.core.graph.SchemaGraph;
import grakn.core.graph.common.Encoding;
import grakn.core.graph.common.KeyGenerator;
import grakn.core.logic.LogicCache;
import grakn.core.traversal.TraversalCache;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Stream;

import static grakn.core.common.exception.ErrorMessage.Database.DATABASE_CLOSED;
import static grakn.core.common.exception.ErrorMessage.Internal.DIRTY_INITIALISATION;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Internal.UNEXPECTED_INTERRUPTION;
import static grakn.core.common.parameters.Arguments.Session.Type.SCHEMA;
import static grakn.core.common.parameters.Arguments.Transaction.Type.READ;
import static grakn.core.common.parameters.Arguments.Transaction.Type.WRITE;
import static java.util.Comparator.reverseOrder;

public class RocksDatabase implements Grakn.Database {

    protected final OptimisticTransactionDB rocksSchema;
    protected final OptimisticTransactionDB rocksData;
    protected final ConcurrentMap<UUID, Pair<RocksSession, Long>> sessions;
    protected final String name;
    protected StatisticsBackgroundCounter statisticsBackgroundCounter;
    protected RocksSession.Data statisticsBackgroundCounterSession;
    private final KeyGenerator.Schema.Persisted schemaKeyGenerator;
    private final KeyGenerator.Data.Persisted dataKeyGenerator;
    private final StampedLock dataWriteSchemaLock;
    private final RocksGrakn grakn;
    private Cache cache;

    private final Factory.Session sessionFactory;
    protected final AtomicBoolean isOpen;

    protected RocksDatabase(RocksGrakn grakn, String name, Factory.Session sessionFactory) {
        this.grakn = grakn;
        this.name = name;
        this.sessionFactory = sessionFactory;
        schemaKeyGenerator = new KeyGenerator.Schema.Persisted();
        dataKeyGenerator = new KeyGenerator.Data.Persisted();
        sessions = new ConcurrentHashMap<>();
        dataWriteSchemaLock = new StampedLock();

        try {
            rocksSchema = OptimisticTransactionDB.open(this.grakn.rocksOptions(), directory().resolve(Encoding.ROCKS_SCHEMA).toString());
            rocksData = OptimisticTransactionDB.open(this.grakn.rocksOptions(), directory().resolve(Encoding.ROCKS_DATA).toString());
        } catch (RocksDBException e) {
            throw GraknException.of(e);
        }
        isOpen = new AtomicBoolean(true);
    }

    static RocksDatabase createAndOpen(RocksGrakn grakn, String name, Factory.Session sessionFactory) {
        try {
            Files.createDirectory(grakn.directory().resolve(name));
        } catch (IOException e) {
            throw GraknException.of(e);
        }

        RocksDatabase database = new RocksDatabase(grakn, name, sessionFactory);
        database.initialise();
        database.statisticsBgCounterStart();
        return database;
    }

    static RocksDatabase loadAndOpen(RocksGrakn grakn, String name, Factory.Session sessionFactory) {
        RocksDatabase database = new RocksDatabase(grakn, name, sessionFactory);
        database.load();
        database.statisticsBgCounterStart();
        return database;
    }

    protected void initialise() {
        try (RocksSession session = createAndOpenSession(SCHEMA, new Options.Session())) {
            try (RocksTransaction.Schema txn = session.transaction(WRITE).asSchema()) {
                if (txn.graph().isInitialised()) throw GraknException.of(DIRTY_INITIALISATION);
                txn.graph().initialise();
                initialiseCommit(txn);
            }
        }
    }

    /**
     * Responsible for committing the initial schema of a database.
     * A different implementation of this class may override it.
     *
     * @param transaction
     */
    protected void initialiseCommit(RocksTransaction.Schema transaction) {
        transaction.commit();
    }

    protected void load() {
        try (RocksSession session = createAndOpenSession(SCHEMA, new Options.Session())) {
            try (RocksTransaction txn = session.transaction(READ)) {
                schemaKeyGenerator.sync(txn.asSchema().schemaStorage());
                dataKeyGenerator.sync(txn.asSchema().dataStorage());
            }
        }
    }

    RocksSession createAndOpenSession(Arguments.Session.Type type, Options.Session options) {
        if (!isOpen.get()) throw GraknException.of(DATABASE_CLOSED, name);

        long lock = 0;
        RocksSession session;

        if (type.isSchema()) {
            lock = dataWriteSchemaLock().writeLock();
            session = sessionFactory.sessionSchema(this, options);
        } else if (type.isData()) {
            session = sessionFactory.sessionData(this, options);
        } else {
            throw GraknException.of(ILLEGAL_STATE);
        }

        sessions.put(session.uuid(), new Pair<>(session, lock));
        return session;
    }

    synchronized Cache cacheBorrow() {
        if (!isOpen.get()) throw GraknException.of(DATABASE_CLOSED, name);

        if (cache == null) cache = new Cache(this);
        cache.borrow();
        return cache;
    }

    synchronized void cacheUnborrow(Cache cache) {
        cache.unborrow();
    }

    public synchronized void cacheInvalidate() {
        if (!isOpen.get()) throw GraknException.of(DATABASE_CLOSED, name);

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
        return grakn.directory().resolve(name);
    }

    public Options.Database options() {
        return grakn.options();
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

    /**
     * Get the lock that guarantees that the schema is not modified at the same
     * time as data being written to the database. When a schema session is
     * opened (to modify the schema), all write transaction need to wait until
     * the schema session is completed. If there is a write transaction opened,
     * a schema session needs to wait until those transactions are completed.
     *
     * @return a {@code StampedLock} to protect data writes from concurrent schema modification
     */
    StampedLock dataWriteSchemaLock() {
        return dataWriteSchemaLock;
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
    public Grakn.Session get(UUID sessionID) {
        if (sessions.containsKey(sessionID)) return sessions.get(sessionID).first();
        else return null;
    }

    @Override
    public Stream<Grakn.Session> sessions() {
        return sessions.values().stream().map(Pair::first);
    }

    void remove(RocksSession session) {
        if (statisticsBackgroundCounterSession != session) {
            long lock = sessions.remove(session.uuid()).second();
            if (session.type().isSchema()) dataWriteSchemaLock().unlockWrite(lock);
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
        grakn.databases().remove(this);
        try {
            Files.walk(directory()).sorted(reverseOrder()).map(Path::toFile).forEach(File::delete);
        } catch (IOException e) {
            throw GraknException.of(e);
        }
    }

    static class Cache {

        private final TraversalCache traversalCache;
        private final LogicCache logicCache;
        private final SchemaGraph schemaGraph;
        private final RocksStorage schemaStorage;
        private long borrowerCount;
        private boolean invalidated;

        private Cache(RocksDatabase database) {
            this.schemaStorage = new RocksStorage.Cache(database.rocksSchema());
            schemaGraph = new SchemaGraph(schemaStorage, true);
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

        public SchemaGraph schemaGraph() {
            return schemaGraph;
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
                } catch (GraknException e) {
                    if (e.code().isPresent() && e.code().get().equals(DATABASE_CLOSED.code())) {
                        break;
                    } else {
                        // TODO: Add specific code indicating rocksdb conflict to GraknException status code
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
            } while (!isStopped);
        }

        private void waitForCountJob() {
            try {
                countJobNotifications.acquire();
            } catch (InterruptedException e) {
                throw GraknException.of(UNEXPECTED_INTERRUPTION);
            }
            countJobNotifications.drainPermits();
        }

        public void stop() {
            try {
                isStopped = true;
                countJobNotifications.release();
                thread.join();
            } catch (InterruptedException e) {
                throw GraknException.of(UNEXPECTED_INTERRUPTION);
            }
        }
    }
}
