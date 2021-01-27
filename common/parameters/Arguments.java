/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.common.parameters;

public class Arguments {

    public static class Session {

        public enum Type {
            DATA(0),
            SCHEMA(1);

            private final int id;
            private final boolean isSchema;

            Type(int id) {
                this.id = id;
                this.isSchema = id == 1;
            }

            public static Arguments.Session.Type of(int value) {
                for (Arguments.Session.Type t : values()) {
                    if (t.id == value) return t;
                }
                return null;
            }

            public boolean isData() { return !isSchema; }

            public boolean isSchema() { return isSchema; }
        }
    }

    public static class Transaction {

        public enum Type {
            READ(0),
            WRITE(1);

            private final int id;
            private final boolean isWrite;

            Type(int id) {
                this.id = id;
                this.isWrite = id == 1;
            }

            public static Arguments.Transaction.Type of(int value) {
                for (Arguments.Transaction.Type t : values()) {
                    if (t.id == value) return t;
                }
                return null;
            }

            public boolean isRead() { return !isWrite; }

            public boolean isWrite() { return isWrite; }
        }
    }

    public static class Query {

        public enum Producer {
            INCREMENTAL(0),
            EXHAUSTIVE(1);

            private final int id;
            private final boolean isExhaustive;

            Producer(int id) {
                this.id = id;
                this.isExhaustive = id == 1;
            }

            public static Producer of(int value) {
                for (Producer t : values()) {
                    if (t.id == value) return t;
                }
                return null;
            }

            public boolean isIncremental() { return !isExhaustive; }

            public boolean isExhaustive() { return isExhaustive; }
        }
    }
}
