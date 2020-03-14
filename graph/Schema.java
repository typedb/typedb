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

package hypergraph.graph;

public class Schema {

    public enum Key {
        PERSISTED(0, true),
        BUFFERED(-1, false);

        private final int initialValue;
        private final boolean isIncrement;


        Key(int initialValue, boolean isIncrement) {
            this.initialValue = initialValue;
            this.isIncrement = isIncrement;
        }

        public int initialValue() {
            return initialValue;
        }

        public boolean isIncrement() {
            return isIncrement;
        }
    }

    /**
     * The values in this class will be used as 'prefixes' within an IID in the
     * of every object database, and must not overlap with each other.
     *
     * The size of a prefix is 1 byte; i.e. min-value = 0 and max-value = 255.
     */
    public enum Prefix {
        INDEX_TYPE(0),
        INDEX_VALUE(5),
        VERTEX_TYPE(10),
        VERTEX_ENTITY_TYPE(20),
        VERTEX_RELATION_TYPE(30),
        VERTEX_ROLE_TYPE(40),
        VERTEX_ATTRIBUTE_TYPE(50),
        VERTEX_ENTITY(60),
        VERTEX_RELATION(70),
        VERTEX_ROLE(80),
        VERTEX_ATTRIBUTE(90),
        VERTEX_VALUE(100),
        VERTEX_RULE(110);

        private final byte key;

        Prefix(int key) {
            this.key = (byte) key;
        }

        public byte key() {
            return key;
        }
    }

    /**
     * The values in this class will be used as 'infixes' between two IIDs of
     * two objects in the database, and must not overlap with each other.
     *
     * The size of a prefix is 1 byte; i.e. min-value = 0 and max-value = 255.
     */
    public enum Infix {
        PROPERTY_ABSTRACT(0),
        PROPERTY_DATATYPE(1),
        PROPERTY_REGEX(2),
        PROPERTY_VALUE(3),
        PROPERTY_VALUE_REF(4),
        PROPERTY_WHEN(5),
        PROPERTY_THEN(6),
        EDGE_SUB_OUT(20),
        EDGE_SUB_IN(25),
        EDGE_KEY_OUT(30),
        EDGE_KEY_IN(35),
        EDGE_HAS_OUT(40),
        EDGE_HAS_IN(45),
        EDGE_PLAYS_OUT(50),
        EDGE_PLAYS_IN(55),
        EDGE_RELATES_OUT(60),
        EDGE_RELATES_IN(65),
        EDGE_OPT_ROLE_OUT(100),
        EDGE_OPT_ROLE_IN(105),
        EDGE_OPT_RELATION_OUT(110);

        private final byte key;

        Infix(int key) {
            this.key = (byte) key;
        }

        public byte key() {
            return key;
        }
    }

    public enum Index {
        TYPE(Prefix.INDEX_TYPE),
        VALUE(Prefix.INDEX_VALUE);

        private final Prefix prefix;

        Index(Prefix prefix) {
            this.prefix = prefix;
        }

        public Prefix prefix(){
            return prefix;
        }
    }

    public enum Status {
        BUFFERED(0),
        PERSISTED(1);

        private int status;
        Status(int status) {
            this.status = status;
        }

        public int status() {
            return status;
        }
    }

    public enum Property {
        ABSTRACT(Infix.PROPERTY_ABSTRACT),
        DATATYPE(Infix.PROPERTY_DATATYPE),
        REGEX(Infix.PROPERTY_REGEX),
        VALUE(Infix.PROPERTY_VALUE),
        VALUE_REF(Infix.PROPERTY_VALUE_REF),
        WHEN(Infix.PROPERTY_WHEN),
        THEN(Infix.PROPERTY_THEN);

        private final Infix infix;

        Property(Infix infix) {
            this.infix = infix;
        }

        public Infix infix() {
            return infix;
        }
    }
    public enum DataType {
        LONG(0),
        DOUBLE(2),
        STRING(4),
        BOOLEAN(6),
        DATE(8);

        private final byte value;

        DataType(int value) {
            this.value = (byte) value;
        }

        public byte value() {
            return value;
        }
    }

    public interface Vertex {

        Prefix prefix();

        public enum Other implements Vertex {
            VALUE(Prefix.VERTEX_VALUE),
            RULE(Prefix.VERTEX_RULE);

            private final Prefix prefix;

            Other(Prefix prefix) {
                this.prefix = prefix;
            }

            @Override
            public Prefix prefix() {
                return prefix;
            }
        }

        public enum Type implements Vertex {
            TYPE(Prefix.VERTEX_TYPE, Root.THING),
            ENTITY_TYPE(Prefix.VERTEX_ENTITY_TYPE, Root.ENTITY),
            RELATION_TYPE(Prefix.VERTEX_RELATION_TYPE, Root.RELATION),
            ROLE_TYPE(Prefix.VERTEX_ROLE_TYPE, Root.ROLE),
            ATTRIBUTE_TYPE(Prefix.VERTEX_ATTRIBUTE_TYPE, Root.ATTRIBUTE);

            private final Prefix prefix;
            private final Root root;

            Type(Prefix prefix, Root root) {
                this.prefix = prefix;
                this.root = root;
            }

            @Override
            public Prefix prefix() {
                return prefix;
            }

            public Root root() {
                return root;
            }

            public enum Root {
                THING("thing"),
                ENTITY("entity"),
                RELATION("relation"),
                ROLE("role"),
                ATTRIBUTE("attribute");

                private final String label;

                Root(String label) {
                    this.label = label;
                }

                public String label() {
                    return label;
                }
            }
        }

        public enum Thing implements Vertex {
            ENTITY(Prefix.VERTEX_ENTITY),
            RELATION(Prefix.VERTEX_RELATION),
            ROLE(Prefix.VERTEX_ROLE),
            ATTRIBUTE(Prefix.VERTEX_ATTRIBUTE),
            RULE(Prefix.VERTEX_RULE);

            private final Prefix prefix;

            Thing(Prefix prefix) {
                this.prefix = prefix;
            }

            @Override
            public Prefix prefix() {
                return prefix;
            }
        }

    }

    public enum Edge {
        SUB(Infix.EDGE_SUB_OUT, Infix.EDGE_SUB_IN),
        KEY(Infix.EDGE_KEY_OUT, Infix.EDGE_KEY_IN),
        HAS(Infix.EDGE_HAS_OUT, Infix.EDGE_HAS_IN),
        PLAYS(Infix.EDGE_PLAYS_OUT, Infix.EDGE_PLAYS_IN),
        RELATES(Infix.EDGE_RELATES_OUT, Infix.EDGE_RELATES_IN),
        OPT_ROLE(Infix.EDGE_OPT_ROLE_OUT, Infix.EDGE_OPT_ROLE_IN),
        OPT_RELATION(Infix.EDGE_OPT_RELATION_OUT, null);

        private final Infix out;
        private final Infix in;

        Edge(Infix out, Infix in) {
            this.out = out;
            this.in = in;
        }

        public Infix out() {
            return out;
        }

        public Infix in() {
            return in;
        }
    }
}
