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
 *
 */

package grakn.core.rocks;

import grakn.common.collection.Pair;
import grakn.core.Grakn;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Options;
import grakn.core.graph.SchemaGraph;
import grakn.core.graph.util.KeyGenerator;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Stream;

import static grakn.core.common.exception.ErrorMessage.Internal.DIRTY_INITIALISATION;
import static grakn.core.common.parameters.Arguments.Session.Type.DATA;
import static grakn.core.common.parameters.Arguments.Session.Type.SCHEMA;
import static grakn.core.common.parameters.Arguments.Transaction.Type.READ;
import static grakn.core.common.parameters.Arguments.Transaction.Type.WRITE;
import static java.util.Comparator.reverseOrder;

public class RocksDatabase implements Grakn.Database {

    private final String name;
    private final RocksGrakn rocksGrakn;
    private final OptimisticTransactionDB rocksDB;
    private final KeyGenerator.Data.Persisted dataKeyGenerator;
    private final KeyGenerator.Schema.Persisted schemaKeyGenerator;
    private final ConcurrentMap<UUID, Pair<RocksSession, Long>> sessions;
    private final StampedLock schemaLock;
    private final AtomicBoolean isOpen;
    private final AtomicReference<SchemaGraph> schemaGraph;

    private RocksDatabase(RocksGrakn rocksGrakn, String name, boolean isNew) {
        this.name = name;
        this.rocksGrakn = rocksGrakn;
        schemaKeyGenerator = new KeyGenerator.Schema.Persisted();
        dataKeyGenerator = new KeyGenerator.Data.Persisted();
        sessions = new ConcurrentHashMap<>();
        schemaLock = new StampedLock();
        isOpen = new AtomicBoolean(false);
        schemaGraph = new AtomicReference<>();

        try {
            rocksDB = OptimisticTransactionDB.open(this.rocksGrakn.rocksOptions(), directory().toString());
        } catch (RocksDBException e) {
            throw new GraknException(e);
        }

        if (isNew) initialise();
        else load();
        isOpen.set(true);
    }

    static RocksDatabase createNewAndOpen(RocksGrakn rocksGrakn, String name) {
        return new RocksDatabase(rocksGrakn, name, true);
    }

    static RocksDatabase loadExistingAndOpen(RocksGrakn rocksGrakn, String name) {
        return new RocksDatabase(rocksGrakn, name, false);
    }

    private void initialise() {
        try (RocksSession session = createAndOpenSession(SCHEMA, new Options.Session())) {
            try (RocksTransaction txn = session.transaction(WRITE)) {
                if (txn.graphs().schema().isInitialised()) throw new GraknException(DIRTY_INITIALISATION);
                txn.graphs().schema().initialise();
                txn.commit();
            }
        }
    }

    private void load() {
        try (RocksSession session = createAndOpenSession(DATA, new Options.Session())) {
            try (RocksTransaction txn = session.transaction(READ)) {
                schemaKeyGenerator.sync(txn.storage());
                dataKeyGenerator.sync(txn.storage());
            }
        }
    }

    RocksSession createAndOpenSession(Arguments.Session.Type type, Options.Session options) {
        long schemaWriteLockStamp = 0;
        if (type.isSchema()) schemaWriteLockStamp = schemaLock.writeLock();
        RocksSession session = new RocksSession(this, type, options);
        sessions.put(session.uuid(), new Pair<>(session, schemaWriteLockStamp));
        return session;
    }

    private Path directory() {
        return rocksGrakn.directory().resolve(name);
    }

    public Options.Database options() {
        return rocksGrakn.options();
    }

    OptimisticTransactionDB rocks() {
        return rocksDB;
    }

    KeyGenerator.Schema schemaKeyGenerator() {
        return schemaKeyGenerator;
    }

    KeyGenerator.Data dataKeyGenerator() {
        return dataKeyGenerator;
    }

    long acquireSchemaReadLock() {
        return schemaLock.readLock();
    }

    void releaseSchemaReadLock(long stamp) {
        schemaLock.unlockRead(stamp);
    }

    void remove(RocksSession session) {
        long schemaWriteLockStamp = sessions.remove(session.uuid()).second();
        if (session.type().isSchema()) schemaLock.unlockWrite(schemaWriteLockStamp);
    }

    void close() {
        if (isOpen.compareAndSet(true, false)) {
            sessions.values().forEach(p -> p.first().close());
            rocksDB.close();
        }
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

    @Override
    public void delete() {
        close();
        rocksGrakn.databases().remove(this);
        try {
            Files.walk(directory()).sorted(reverseOrder()).map(Path::toFile).forEach(File::delete);
        } catch (IOException e) {
            throw new GraknException(e);
        }
    }
}
