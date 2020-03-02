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

package grakn.hypergraph;

import grakn.hypergraph.exception.GraknHypergraphException;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class Graph implements AutoCloseable {

    private final Path directory;
    private final Options optionsGraph = new Options();
    private List<Session> sessions = new ArrayList<>();
    private AtomicBoolean isOpen = new AtomicBoolean();

    static {
        RocksDB.loadLibrary();
    }

    public Graph(String directory) {
        this(directory, new Properties());
    }

    public Graph(String directory, Properties properties) {
        this.directory = Paths.get(directory);
        this.optionsGraph.setCreateIfMissing(true);
        this.isOpen.set(true);
        setOPtionsFromProperties(properties);
    }

    private void setOPtionsFromProperties(Properties properties) {
        // TODO: configure optimisation paramaters
    }

    public Session session(String keyspace) {
        Session session = new Session(keyspace);
        sessions.add(session);
        return session;
    }

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

    public class Session implements AutoCloseable {

        private final String keyspace;
        private OptimisticTransactionDB sessionRocks;
        private List<Transaction> transactions = new ArrayList<>();
        private AtomicBoolean isOpen = new AtomicBoolean();

        Session(String keyspace) {
            try {
                sessionRocks = OptimisticTransactionDB.open(Graph.this.optionsGraph, directory.resolve(keyspace).toString());
            } catch (RocksDBException e) {
                e.printStackTrace();
                throw new GraknHypergraphException(e);
            }
            this.keyspace = keyspace;
            this.isOpen.set(true);
        }

        public String keyspace() {
            return this.keyspace;
        }

        public Transaction transaction(Transaction.Type type) {
            Transaction transaction = new Transaction(sessionRocks, type);
            transactions.add(transaction);
            return transaction;
        }

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
    }

    public static class Transaction implements AutoCloseable {

        private AtomicBoolean isOpen = new AtomicBoolean();
        private final WriteOptions optionsWrite = new WriteOptions();
        private final org.rocksdb.Transaction transactionRocks;
        private final Type type;

        public enum Type {
            READ(0),
            WRITE(1);

            private final int type;

            Type(int type) {
                this.type = type;
            }

            public int id() {
                return type;
            }

            @Override
            public String toString() {
                return this.name();
            }

            public static Type of(String value) {
                for (Type t : Type.values()) {
                    if (t.name().equalsIgnoreCase(value)) return t;
                }
                return null;
            }
        }

        Transaction(OptimisticTransactionDB sessionRocks, Type type) {
            this.transactionRocks = sessionRocks.beginTransaction(optionsWrite);
            this.type = type;
            this.isOpen.set(true);
        }

        public boolean isOpen() {
            return this.isOpen.get();
        }

        public Type type() {
            return this.type;
        }

        @Override
        public void close() {
            if (isOpen.compareAndSet(true, false)) {
                transactionRocks.close();
                optionsWrite.close();
            }
        }
    }
}
