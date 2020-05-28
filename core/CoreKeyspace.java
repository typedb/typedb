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

package hypergraph.core;

import hypergraph.Hypergraph;
import hypergraph.common.exception.HypergraphException;
import hypergraph.graph.util.AttributeSync;
import hypergraph.graph.util.KeyGenerator;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

public class CoreKeyspace implements Hypergraph.Keyspace {

    private final String name;
    private final CoreHypergraph core;
    private final OptimisticTransactionDB rocksDB;
    private final KeyGenerator.Persisted keyGenerator;
    private final ConcurrentSkipListSet<Long> writeSnapshots;
    private final CoreAttributeSync attributeSync;
    private final Set<CoreSession> sessions;
    private final AtomicBoolean isOpen;

    private CoreKeyspace(CoreHypergraph core, String name) {
        this.name = name;
        this.core = core;
        keyGenerator = new KeyGenerator.Persisted();
        sessions = ConcurrentHashMap.newKeySet();
        isOpen = new AtomicBoolean(false);
        writeSnapshots = new ConcurrentSkipListSet<>();
        attributeSync = new CoreAttributeSync(
                this,
                core.properties().ATTRIBUTE_SYNC_EXPIRE_DURATION,
                core.properties().ATTRIBUTE_SYNC_EVICTION_DURATION,
                core.properties().MEMORY_MINIMUM_RESERVE
        );

        try {
            rocksDB = OptimisticTransactionDB.open(this.core.rocksOptions(), directory().toString());
        } catch (RocksDBException e) {
            throw new HypergraphException(e);
        }
    }

    static CoreKeyspace createNewAndOpen(CoreHypergraph core, String name) {
        return new CoreKeyspace(core, name).initialiseAndOpen();
    }

    static CoreKeyspace loadExistingAndOpen(CoreHypergraph core, String name) {
        return new CoreKeyspace(core, name).loadAndOpen();
    }

    private CoreKeyspace initialiseAndOpen() {
        try (CoreSession session = createSessionAndOpen()) {
            try (CoreTransaction txn = session.transaction(Hypergraph.Transaction.Type.WRITE)) {
                if (txn.graph().isInitialised()) {
                    throw new HypergraphException("Invalid Keyspace Initialisation");
                }
                txn.graph().initialise();
                txn.commit();
            }
        }
        isOpen.set(true);
        return this;
    }

    private CoreKeyspace loadAndOpen() {
        try (CoreSession session = createSessionAndOpen()) {
            try (CoreTransaction txn = session.transaction(Hypergraph.Transaction.Type.READ)) {
                keyGenerator.sync(txn.storage());
            }
        }
        isOpen.set(true);
        return this;
    }

    CoreSession createSessionAndOpen() {
        CoreSession session = new CoreSession(this);
        sessions.add(session);
        return session;
    }

    private Path directory() {
        return core.directory().resolve(name);
    }

    OptimisticTransactionDB rocks() {
        return rocksDB;
    }

    KeyGenerator keyGenerator() {
        return keyGenerator;
    }

    void trackWriteSnapshot(long snapshot) {
        writeSnapshots.add(snapshot);
    }

    void untrackWriteSnapshot(long snapshot) {
        writeSnapshots.remove(snapshot);
    }

    long oldestWriteSnapshot() {
        return writeSnapshots.first();
    }

    AttributeSync attributeSync() {
        return attributeSync;
    }

    public void remove(CoreSession session) {
        sessions.remove(session);
    }

    void close() {
        if (isOpen.compareAndSet(true, false)) {
            sessions.parallelStream().forEach(CoreSession::close);
            attributeSync.close();
            rocksDB.close();
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void delete() {
        close();
        core.keyspaces().remove(this);
        try {
            Files.walk(directory()).sorted(Comparator.reverseOrder())
                    .map(Path::toFile).forEach(File::delete);
        } catch (IOException e) {
            throw new HypergraphException(e);
        }
    }
}
