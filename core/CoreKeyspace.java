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
import hypergraph.common.HypergraphException;
import hypergraph.graph.KeyGenerator;
import hypergraph.graph.Schema;
import org.rocksdb.Options;

import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

class CoreKeyspace implements Hypergraph.Keyspace {

    private final String name;
    private final CoreHypergraph core;
    private final KeyGenerator keyGenerator;
    private final AtomicBoolean isOpen;
    private final Set<CoreSession> sessions;

    private CoreKeyspace(CoreHypergraph core, String name) {
        this.name = name;
        this.core = core;
        keyGenerator = new KeyGenerator(Schema.Key.PERSISTED);
        sessions = ConcurrentHashMap.newKeySet();
        isOpen = new AtomicBoolean(false);
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
        // TODO load keyGenerator
        isOpen.set(true);
        return this;
    }

    Options options() {
        return core.options();
    }

    Path directory() {
        return core.directory().resolve(name);
    }

    CoreSession createSessionAndOpen() {
        CoreSession session = new CoreSession(this);
        sessions.add(session);
        return session;
    }

    KeyGenerator keyGenerator() {
        return keyGenerator;
    }

    void close() {
        if (isOpen.compareAndSet(true, false)) {
            sessions.parallelStream().forEach(CoreSession::close);
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void delete() {
        // TODO
    }
}
