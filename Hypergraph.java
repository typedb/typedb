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

package hypergraph;

import hypergraph.concept.Concepts;
import hypergraph.traversal.Traversal;

import java.util.Set;

/**
 * A Hypergraph Database API
 *
 * This interface allows you to open a 'Session' to connect to the Hypergraph
 * database, and open a 'Transaction' from that session.
 */
public interface Hypergraph extends AutoCloseable {

    Session session(String keyspace);

    KeyspaceManager keyspaces();

    boolean isOpen();

    void close();

    interface KeyspaceManager {

        Keyspace create(String keyspace);

        Keyspace get(String keyspace);

        Set<? extends Keyspace> getAll();
    }
    /**
     * A Hypergraph Keyspace
     *
     * A keyspace is an isolated scope of data in the storage engine.
     */
    interface Keyspace {

        String name();

        void delete();
    }

    /**
     * A Hypergraph Database Session
     *
     * This interface allows you to create transactions to perform READs or
     * WRITEs onto the database.
     */
    interface Session extends AutoCloseable {

        Transaction transaction(Transaction.Type type);

        Keyspace keyspace();

        boolean isOpen();

        void close();
    }

    /**
     * A Hypergraph Database Transaction
     */
    interface Transaction extends AutoCloseable {

        Transaction.Type type();

        boolean isOpen();

        Traversal traversal();

        Concepts concepts();

        void commit();

        void rollback();

        void close();

        enum Type {
            READ(0),
            WRITE(1);

            private final int type;
            private final boolean isWrite;

            Type(int type) {
                this.type = type;
                this.isWrite = type == 1;
            }

            public static Type of(int value) {
                for (Type t : Type.values()) {
                    if (t.type == value) return t;
                }
                return null;
            }

            public static Type of(String value) {
                for (Type t : Type.values()) {
                    if (t.name().equalsIgnoreCase(value)) return t;
                }
                return null;
            }

            public int id() {
                return type;
            }

            public boolean isRead() {
                return !isWrite;
            }

            public boolean isWrite() {
                return isWrite;
            }

            @Override
            public String toString() {
                return this.name();
            }
        }
    }

}
