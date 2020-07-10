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

package grakn;

import grakn.concept.Concepts;
import grakn.traversal.Traversal;

import java.util.Set;

/**
 * A Grakn Database API
 *
 * This interface allows you to open a 'Session' to connect to the Grakn
 * database, and open a 'Transaction' from that session.
 */
public interface Grakn extends AutoCloseable {

    default Session session(String keyspace) { return session(keyspace, Session.Type.DATA); }

    Session session(String keyspace, Session.Type type);

    KeyspaceManager keyspaces();

    boolean isOpen();

    void close();

    /**
     * Grakn Keyspace Manager
     */
    interface KeyspaceManager {

        boolean contains(String keyspace);

        Keyspace create(String keyspace);

        Keyspace get(String keyspace);

        Set<? extends Keyspace> getAll();
    }

    /**
     * A Grakn Keyspace
     *
     * A keyspace is an isolated scope of data in the storage engine.
     */
    interface Keyspace {

        String name();

        void delete();
    }

    /**
     * A Grakn Database Session
     *
     * This interface allows you to create transactions to perform READs or
     * WRITEs onto the database.
     */
    interface Session extends AutoCloseable {

        Transaction transaction(Transaction.Type type);

        Session.Type type();

        Keyspace keyspace();

        boolean isOpen();

        void close();

        enum Type {
            DATA(0),
            SCHEMA(1);

            private final int id;
            private final boolean isSchema;

            Type(int id) {
                this.id = id;
                this.isSchema = id == 1;
            }

            public static Session.Type of(int value) {
                for (Session.Type t : Session.Type.values()) {
                    if (t.id == value) return t;
                }
                return null;
            }

            public boolean isData() { return !isSchema; }

            public boolean isSchema() { return isSchema; }
        }
    }

    /**
     * A Grakn Database Transaction
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

            private final int id;
            private final boolean isWrite;

            Type(int id) {
                this.id = id;
                this.isWrite = id == 1;
            }

            public static Type of(int value) {
                for (Type t : Type.values()) {
                    if (t.id == value) return t;
                }
                return null;
            }

            public boolean isRead() { return !isWrite; }

            public boolean isWrite() { return isWrite; }
        }
    }

}
