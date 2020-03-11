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
import hypergraph.storage.KeyGenerator;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

class CoreKeyspace implements Hypergraph.Keyspace {

    private final String name;
    private final CoreHypergraph core;
    private final KeyGenerator keyGenerator;
    private final AtomicBoolean isOpen;
    private final List<CoreSession> sessions;

    CoreKeyspace(CoreHypergraph core, String name) {
        this.name = name;
        this.core = core;
        keyGenerator = new KeyGenerator();
        sessions = new ArrayList<>();

        initialise();

        isOpen = new AtomicBoolean();
        isOpen.set(true);
    }

    private void initialise() {
        try (CoreSession session = sessionCreateAndOpen()) {
            try (CoreTransaction txn = session.transaction(Hypergraph.Transaction.Type.WRITE)) {
                txn.graph().creatRootTypes();
                txn.commit();
            }
        }
    }

    CoreSession sessionCreateAndOpen() {
        try {
            OptimisticTransactionDB rocksSession = OptimisticTransactionDB.open(
                    core.options(), core.directory().resolve(name).toString()
            );
            CoreSession session = new CoreSession(this, rocksSession);
            sessions.add(session);
            return session;
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new HypergraphException(e);
        }
    }

    KeyGenerator keyGenerator() {
        return keyGenerator;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean isOpen() {
        return isOpen.get();
    }

    @Override
    public void load() {
        // TODO
    }

    @Override
    public void close() {
        if (isOpen.compareAndSet(true, false)) {
            for (CoreSession session : sessions) {
                session.close();
            }
        }
    }

    @Override
    public void delete() {
        // TODO
    }
}
