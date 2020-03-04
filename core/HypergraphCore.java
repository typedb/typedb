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
import hypergraph.concept.ConceptMgr;
import hypergraph.graph.Graph;
import hypergraph.index.Index;
import hypergraph.traversal.Traversal;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Hypergraph implementation with RocksDB
 */
public class HypergraphCore implements Hypergraph {

    static {
        RocksDB.loadLibrary();
    }

    private final Path directory;
    private final Options optionsGraph = new Options();
    private List<Session> sessions = new ArrayList<>();
    private AtomicBoolean isOpen = new AtomicBoolean();
    private Map<String, Index> indexes = new ConcurrentHashMap<>();

    public HypergraphCore(String directory) {
        this(directory, new Properties());
    }

    public HypergraphCore(String directory, Properties properties) {
        this.directory = Paths.get(directory);
        this.optionsGraph.setCreateIfMissing(true);
        this.isOpen.set(true);
        setOPtionsFromProperties(properties);
    }

    private void setOPtionsFromProperties(Properties properties) {
        // TODO: configure optimisation paramaters
    }

    @Override
    public Session session(String keyspace) {
        Session session = new Session(keyspace);
        sessions.add(session);
        return session;
    }

    @Override
    public boolean isOpen() {
        return this.isOpen.get();
    }

    @Override
    public void close() {
        if (isOpen.compareAndSet(true, false)) {
            for (Session session : sessions) {
                session.close();
            }
            optionsGraph.close();
        }
    }

    private class Session implements Hypergraph.Session {

        private final String keyspace;
        private OptimisticTransactionDB sessionRocks;
        private List<Transaction> transactions = new ArrayList<>();
        private AtomicBoolean isOpen = new AtomicBoolean();

        Session(String keyspace) {
            try {
                sessionRocks = OptimisticTransactionDB.open(optionsGraph, directory.resolve(keyspace).toString());
            } catch (RocksDBException e) {
                e.printStackTrace();
                throw new HypergraphException(e);
            }
            this.keyspace = keyspace;
            this.isOpen.set(true);
        }

        @Override
        public String keyspace() {
            return this.keyspace;
        }

        @Override
        public Transaction transaction(Hypergraph.Transaction.Type type) {
            Transaction transaction = new Transaction(type);
            transactions.add(transaction);
            return transaction;
        }

        @Override
        public boolean isOpen() {
            return this.isOpen.get();
        }

        @Override
        public void close() {
            if (isOpen.compareAndSet(true, false)) {
                for (Transaction transaction : transactions) {
                    transaction.close();
                }
                sessionRocks.close();
            }
        }

        private class Transaction implements Hypergraph.Transaction {

            private final ReadOptions optionsRead = new ReadOptions();
            private final WriteOptions optionsWrite = new WriteOptions();
            private final org.rocksdb.Transaction transactionRocks;
            private final Hypergraph.Transaction.Type type;
            private final Graph transactionGraph = new Graph(new Operation());
            private final ConceptMgr conceptMgr = new ConceptMgr(indexes.get(keyspace), transactionGraph);
            private final Traversal traversal = new Traversal(conceptMgr);
            private AtomicBoolean isOpen = new AtomicBoolean();

            Transaction(Hypergraph.Transaction.Type type) {
                this.transactionRocks = sessionRocks.beginTransaction(optionsWrite);
                this.type = type;
                this.isOpen.set(true);
            }

            @Override
            public boolean isOpen() {
                return this.isOpen.get();
            }

            @Override
            public Traversal read() {
                return traversal;
            }

            @Override
            public ConceptMgr write() {
                if (this.type.equals(Type.READ)) {
                    throw new HypergraphException("Illegal Write Exception");
                }
                return conceptMgr;
            }

            @Override
            public void commit() {
                try {
                    this.transactionRocks.commit();
                } catch (RocksDBException e) {
                    e.printStackTrace();
                    throw new HypergraphException(e);
                }
            }

            @Override
            public void rollback() {
                try {
                    this.transactionRocks.rollback();
                } catch (RocksDBException e) {
                    e.printStackTrace();
                    throw new HypergraphException(e);
                }
            }

            @Override
            public Hypergraph.Transaction.Type type() {
                return this.type;
            }

            @Override
            public void close() {
                if (isOpen.compareAndSet(true, false)) {
                    transactionRocks.close();
                    optionsWrite.close();
                }
            }

            private class Operation implements hypergraph.operation.Operation {

                @Override
                public byte[] get(byte[] key) {
                    try {
                        return transactionRocks.get(optionsRead, key);
                    } catch (RocksDBException e) {
                        e.printStackTrace();
                        throw new HypergraphException(e);
                    }
                }

                @Override
                public void put(byte[] key, byte[] value) {
                    try {
                        transactionRocks.put(key, value);
                    } catch (RocksDBException e) {
                        e.printStackTrace();
                        throw new HypergraphException(e);
                    }
                }
            }
        }
    }
}
