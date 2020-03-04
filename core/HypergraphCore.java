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
import hypergraph.concept.Concepts;
import hypergraph.graph.Graph;
import hypergraph.storage.Index;
import hypergraph.storage.Storage;
import hypergraph.traversal.Traversal;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.OptimisticTransactionOptions;
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

    public static HypergraphCore open(String directory) {
        return new HypergraphCore(directory,  new Properties());
    }

    public static HypergraphCore open(String directory, Properties properties) {
        return new HypergraphCore(directory, properties);
    }

    private HypergraphCore(String directory, Properties properties) {
        this.directory = Paths.get(directory);
        this.optionsGraph.setCreateIfMissing(true);
        this.isOpen.set(true);
        setOPtionsFromProperties(properties);
    }

    private void setOPtionsFromProperties(Properties properties) {
        // TODO: configure optimisation paramaters
    }

    private void loadIndexes() {
        // TODO: load indexes for every pre-existing keyspace
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
            if (!indexes.containsKey(this.keyspace)) {
                indexes.put(keyspace, new Index());
            }
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

            private final ReadOptions readOptions = new ReadOptions();
            private final WriteOptions writeOptions = new WriteOptions();
            private final OptimisticTransactionOptions transactionOptions =
                    new OptimisticTransactionOptions().setSetSnapshot(true);
            private final org.rocksdb.Transaction rocksTransaction;
            private final Hypergraph.Transaction.Type type;
            private final Storage storage = new Storage(new Operation(), indexes.get(keyspace));
            private final Graph graph = new Graph(storage);
            private final Concepts concepts = new Concepts(graph);
            private final Traversal traversal = new Traversal(concepts);
            private AtomicBoolean isOpen = new AtomicBoolean();

            Transaction(Hypergraph.Transaction.Type type) {
                this.type = type;
                rocksTransaction = sessionRocks.beginTransaction(writeOptions, transactionOptions);
                readOptions.setSnapshot(rocksTransaction.getSnapshot());
                isOpen.set(true);
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
            public Concepts write() {
                return concepts;
            }

            @Override
            public void commit() {
                if (type.equals(Type.READ)) {
                    throw new HypergraphException("Illegal Write Exception");
                }
                try {
                    rocksTransaction.commit();
                } catch (RocksDBException e) {
                    e.printStackTrace();
                    throw new HypergraphException(e);
                }
            }

            @Override
            public void rollback() {
                try {
                    rocksTransaction.rollback();
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
                    storage.persist();
                    rocksTransaction.close();
                    writeOptions.close();
                }
            }

            private class Operation implements hypergraph.storage.Operation {

                @Override
                public byte[] get(byte[] key) {
                    try {
                        return rocksTransaction.get(readOptions, key);
                    } catch (RocksDBException e) {
                        e.printStackTrace();
                        throw new HypergraphException(e);
                    }
                }

                @Override
                public void put(byte[] key, byte[] value) {
                    try {
                        rocksTransaction.put(key, value);
                    } catch (RocksDBException e) {
                        e.printStackTrace();
                        throw new HypergraphException(e);
                    }
                }
            }
        }
    }
}
