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

/**
 * A Hypergraph Database API
 * <p>
 * This interface allows you to open a 'Session' to connect to the Hypergraph
 * database, and open a 'Transaction' from that session.
 */
public interface Hypergraph extends AutoCloseable {

    Session session(String keyspace);

    boolean isOpen();

    void close();

    /**
     * A Hypergraph Database Session
     * <p>
     * This interface allows you to create transactions to perform READs or
     * WRITEs onto the database.
     */
    interface Session extends AutoCloseable {

        Transaction transaction(Transaction.Type type);

        String keyspace();

        boolean isOpen();

        void close();
    }

    /**
     * A Hypergraph Database Transaction
     */
    interface Transaction extends AutoCloseable {

        Transaction.Type type();

        boolean isOpen();

        void commit();

        void rollback();

        void close();

        enum Type {
            READ(0),
            WRITE(1);

            private final int type;

            Type(int type) {
                this.type = type;
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

            @Override
            public String toString() {
                return this.name();
            }
        }
    }

}
