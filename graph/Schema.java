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

import javax.annotation.Nullable;
import java.time.LocalDateTime;

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

    public enum IID {
        TYPE(3),
        THING(12);

        private final int length;

        IID(int length) {
            this.length = length;
        }

        public int length() {
            return length;
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
        VERTEX_THING_TYPE(10),
        VERTEX_ENTITY_TYPE(20),
        VERTEX_ATTRIBUTE_TYPE(30),
        VERTEX_RELATION_TYPE(40),
        VERTEX_ROLE_TYPE(50),
        VERTEX_ENTITY(60),
        VERTEX_ATTRIBUTE(70),
        VERTEX_RELATION(80),
        VERTEX_ROLE(90),
        VERTEX_VALUE(100),
        VERTEX_RULE(110);

        private final byte key;

        Prefix(int key) {
            this.key = (byte) key;
        }

        public byte[] key() {
            return new byte[]{key};
        }
    }

    /**
     * The values in this class will be used as 'infixes' between two IIDs of
     * two objects in the database, and must not overlap with each other.
     *
     * The size of a prefix is 1 byte; i.e. min-value = 0 and max-value = 255.
     */
    public enum Infix {
        PROPERTY_LABEL(0),
        PROPERTY_SCOPE(1),
        PROPERTY_ABSTRACT(2),
        PROPERTY_REGEX(3),
        PROPERTY_VALUE_CLASS(4),
        PROPERTY_VALUE_REF(5),
        PROPERTY_VALUE(6),
        PROPERTY_WHEN(7),
        PROPERTY_THEN(8),
        EDGE_SUB_OUT(20),
        EDGE_SUB_IN(-20),
        EDGE_KEY_OUT(30),
        EDGE_KEY_IN(-30),
        EDGE_HAS_OUT(40),
        EDGE_HAS_IN(-40),
        EDGE_PLAYS_OUT(50),
        EDGE_PLAYS_IN(-50),
        EDGE_RELATES_OUT(60),
        EDGE_RELATES_IN(-60),
        EDGE_OPT_ROLE_OUT(100),
        EDGE_OPT_ROLE_IN(-100),
        EDGE_OPT_RELATION_OUT(110);

        private final byte key;

        Infix(int key) {
            this.key = (byte) key;
        }

        public byte[] key() {
            return new byte[]{key};
        }
    }

    public enum Index {
        TYPE(Prefix.INDEX_TYPE),
        VALUE(Prefix.INDEX_VALUE);

        private final Prefix prefix;

        Index(Prefix prefix) {
            this.prefix = prefix;
        }

        public Prefix prefix() {
            return prefix;
        }
    }

    public enum Status {
        BUFFERED(0),
        COMMITTED(1),
        PERSISTED(2);

        private int status;

        Status(int status) {
            this.status = status;
        }

        public int status() {
            return status;
        }
    }

    public enum Property {
        LABEL(Infix.PROPERTY_LABEL),
        SCOPE(Infix.PROPERTY_SCOPE),
        ABSTRACT(Infix.PROPERTY_ABSTRACT),
        REGEX(Infix.PROPERTY_REGEX),
        VALUE_CLASS(Infix.PROPERTY_VALUE_CLASS),
        VALUE_REF(Infix.PROPERTY_VALUE_REF),
        VALUE(Infix.PROPERTY_VALUE),
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

    public enum ValueClass {
        OBJECT(0, Object.class, null),
        BOOLEAN(10, Boolean.class, "boolean"),
        LONG(20, Long.class, "long"),
        DOUBLE(30, Double.class, "double"),
        STRING(40, String.class, "string"),
        DATETIME(50, LocalDateTime.class, "datetime");

        private final byte key;
        private final Class<?> valueClass;
        private final String keyword;

        ValueClass(int key, Class<?> valueClass, @Nullable String keyword) {
            this.key = (byte) key;
            this.valueClass = valueClass;
            this.keyword = keyword;
        }

        public byte[] key() {
            return new byte[]{key};
        }

        public static ValueClass of(byte value) {
            for (ValueClass t : ValueClass.values()) {
                if (t.key == value) {
                    return t;
                }
            }
            return null;
        }

        public static ValueClass of(Class<?> valueClass) {
            for (ValueClass t : ValueClass.values()) {
                if (t.valueClass == valueClass) {
                    return t;
                }
            }
            return null;
        }

        @Nullable
        public String keyword() {
            return keyword;
        }

        public Class<?> valueClass() {
            return valueClass;
        }
    }

    public interface Vertex {

        Prefix prefix();

        enum Value implements Vertex {
            VALUE(Prefix.VERTEX_VALUE);

            private final Prefix prefix;

            Value(Prefix prefix) {
                this.prefix = prefix;
            }

            @Override
            public Prefix prefix() {
                return prefix;
            }
        }

        enum Rule implements Vertex {
            RULE(Prefix.VERTEX_RULE);

            private final Prefix prefix;

            Rule(Prefix prefix) {
                this.prefix = prefix;
            }

            @Override
            public Prefix prefix() {
                return prefix;
            }
        }

        enum Type implements Vertex {
            THING_TYPE(Prefix.VERTEX_THING_TYPE, Root.THING),
            ENTITY_TYPE(Prefix.VERTEX_ENTITY_TYPE, Root.ENTITY),
            ATTRIBUTE_TYPE(Prefix.VERTEX_ATTRIBUTE_TYPE, Root.ATTRIBUTE),
            RELATION_TYPE(Prefix.VERTEX_RELATION_TYPE, Root.RELATION),
            ROLE_TYPE(Prefix.VERTEX_ROLE_TYPE, Root.ROLE);

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

            public static Type of(byte prefix) {
                for (Type t : Type.values()) {
                    if (t.prefix.key == prefix) return t;
                }
                return null;
            }

            public static Type of(Thing thing) {
                for (Type t : Type.values()) {
                    if (t.root.name().equals(thing.name())) return t;
                }
                return null;
            }

            public enum Root {
                THING("thing"),
                ENTITY("entity"),
                ATTRIBUTE("attribute"),
                RELATION("relation"),
                ROLE("role", "relation");

                private final String label;
                private final String scope;

                Root(String label) {
                    this(label, null);
                }

                Root(String label, @Nullable String scope) {
                    this.label = label;
                    this.scope = scope;
                }

                public String label() {
                    return label;
                }

                public String scope() {
                    return scope;
                }
            }
        }

        enum Thing implements Vertex {
            ENTITY(Prefix.VERTEX_ENTITY),
            ATTRIBUTE(Prefix.VERTEX_ATTRIBUTE),
            RELATION(Prefix.VERTEX_RELATION),
            ROLE(Prefix.VERTEX_ROLE);

            private final Prefix prefix;

            Thing(Prefix prefix) {
                this.prefix = prefix;
            }

            @Override
            public Prefix prefix() {
                return prefix;
            }

            public static Thing of(byte prefix) {
                for (Thing t : Thing.values()) {
                    if (t.prefix.key == prefix) return t;
                }
                return null;
            }

            public static Thing of(Type type) {
                for (Thing t : Thing.values()) {
                    if (t.name().equals(type.root().label())) return t;
                }
                return null;
            }
        }

    }

    public interface Edge {

        Infix out();

        Infix in();

        static boolean isOut(byte infix) {
            return infix > 0;
        }

        enum Type implements Edge {
            SUB(Infix.EDGE_SUB_OUT, Infix.EDGE_SUB_IN),
            KEY(Infix.EDGE_KEY_OUT, Infix.EDGE_KEY_IN),
            HAS(Infix.EDGE_HAS_OUT, Infix.EDGE_HAS_IN),
            PLAYS(Infix.EDGE_PLAYS_OUT, Infix.EDGE_PLAYS_IN),
            RELATES(Infix.EDGE_RELATES_OUT, Infix.EDGE_RELATES_IN);

            private final Infix out;
            private final Infix in;

            Type(Infix out, Infix in) {
                this.out = out;
                this.in = in;
            }

            public static Type of(byte infix) {
                for (Type t : Type.values()) {
                    if (t.out.key == infix || t.in.key == infix) {
                        return t;
                    }
                }
                return null;
            }

            @Override
            public Infix out() {
                return out;
            }

            public Infix in() {
                return in;
            }
        }

        enum Thing implements Edge {
            HAS(Infix.EDGE_HAS_OUT, Infix.EDGE_HAS_IN),
            PLAYS(Infix.EDGE_PLAYS_OUT, Infix.EDGE_PLAYS_IN),
            RELATES(Infix.EDGE_RELATES_OUT, Infix.EDGE_RELATES_IN),
            OPT_ROLE(Infix.EDGE_OPT_ROLE_OUT, Infix.EDGE_OPT_ROLE_IN),
            OPT_RELATION(Infix.EDGE_OPT_RELATION_OUT, null);

            private final Infix out;
            private final Infix in;

            Thing(Infix out, Infix in) {
                this.out = out;
                this.in = in;
            }

            @Override
            public Infix out() {
                return out;
            }

            @Override
            public Infix in() {
                return in;
            }
        }
    }
}
