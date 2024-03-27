/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.parameters;

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
