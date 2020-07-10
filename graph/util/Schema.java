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

package grakn.core.graph.util;

import grakn.core.common.exception.Error;
import grakn.core.common.exception.GraknException;

import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Schema {

    public static final int STRING_MAX_LENGTH = 255;
    public static final Charset STRING_ENCODING = UTF_8;
    public static final ZoneId TIME_ZONE_ID = ZoneId.of("Z");

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
        INDEX_ATTRIBUTE(5),
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

        public static Prefix of(byte key) {
            for (Prefix i : Prefix.values()) {
                if (i.key == key) return i;
            }
            throw new GraknException(Error.Internal.UNRECOGNISED_VALUE);
        }

        public byte key() {
            return key;
        }

        public byte[] bytes() {
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
        PROPERTY_VALUE_TYPE(4),
        PROPERTY_VALUE_REF(5),
        PROPERTY_VALUE(6),
        PROPERTY_WHEN(7),
        PROPERTY_THEN(8),
        EDGE_ISA_IN(-20), // EDGE_ISA_OUT does not exist by design
        EDGE_SUB_OUT(30),
        EDGE_SUB_IN(-30),
        EDGE_KEY_OUT(40),
        EDGE_KEY_IN(-40),
        EDGE_HAS_OUT(50),
        EDGE_HAS_IN(-50),
        EDGE_PLAYS_OUT(60),
        EDGE_PLAYS_IN(-60),
        EDGE_RELATES_OUT(70),
        EDGE_RELATES_IN(-70),
        EDGE_ROLEPLAYER_OUT(100, true),
        EDGE_ROLEPLAYER_IN(-100, true);

        private final byte key;
        private final boolean isOptimisation;

        Infix(int key) {
            this(key, false);
        }

        Infix(int key, boolean isOptimisation) {
            this.key = (byte) key;
            this.isOptimisation = isOptimisation;
        }

        public static Infix of(byte key) {
            for (Infix i : Infix.values()) {
                if (i.key == key) return i;
            }
            throw new GraknException(Error.Internal.UNRECOGNISED_VALUE);
        }

        public byte key() {
            return key;
        }

        public byte[] bytes() {
            return new byte[]{key};
        }

        public boolean isOptimisation() {
            return isOptimisation;
        }
    }

    public enum Index {
        TYPE(Prefix.INDEX_TYPE),
        ATTRIBUTE(Prefix.INDEX_ATTRIBUTE);

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
        PERSISTED(2),
        IMMUTABLE(3);

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
        VALUE_TYPE(Infix.PROPERTY_VALUE_TYPE),
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

    public enum ValueType {
        OBJECT(0, Object.class, false, false),
        BOOLEAN(10, Boolean.class, true, false),
        LONG(20, Long.class, true, true),
        DOUBLE(30, Double.class, true, false),
        STRING(40, String.class, true, true),
        DATETIME(50, LocalDateTime.class, true, true);

        private final byte key;
        private final Class<?> valueClass;
        private final boolean isKeyable;
        private final boolean isIndexable;

        ValueType(int key, Class<?> valueClass, boolean isIndexable, boolean isKeyable) {
            this.key = (byte) key;
            this.valueClass = valueClass;
            this.isKeyable = isKeyable;
            this.isIndexable = isIndexable;
        }

        public static ValueType of(byte value) {
            for (ValueType t : ValueType.values()) {
                if (t.key == value) {
                    return t;
                }
            }
            throw new GraknException(Error.Internal.UNRECOGNISED_VALUE);
        }

        public static ValueType of(Class<?> valueClass) {
            for (ValueType t : ValueType.values()) {
                if (t.valueClass == valueClass) {
                    return t;
                }
            }
            throw new GraknException(Error.Internal.UNRECOGNISED_VALUE);
        }

        public byte[] bytes() {
            return new byte[]{key};
        }

        public Class<?> valueClass() {
            return valueClass;
        }

        public boolean isIndexable() {
            return isIndexable;
        }

        public boolean isKeyable() {
            return isKeyable;
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

            public static Type of(byte prefix) {
                for (Type t : Type.values()) {
                    if (t.prefix.key == prefix) return t;
                }
                throw new GraknException(Error.Internal.UNRECOGNISED_VALUE);
            }

            /**
             * Returns the fully scoped label for a given {@code TypeVertex}
             *
             * @param label the unscoped label of the {@code TypeVertex}
             * @param scope the scope label of the {@code TypeVertex}
             * @return the fully scoped label for a given {@code TypeVertex} as a string
             */
            public static String scopedLabel(String label, @Nullable String scope) {
                if (scope == null) return label;
                else return scope + ":" + label;
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
            ENTITY(Prefix.VERTEX_ENTITY, Type.ENTITY_TYPE),
            ATTRIBUTE(Prefix.VERTEX_ATTRIBUTE, Type.ATTRIBUTE_TYPE),
            RELATION(Prefix.VERTEX_RELATION, Type.RELATION_TYPE),
            ROLE(Prefix.VERTEX_ROLE, Type.ROLE_TYPE);

            private final Prefix prefix;
            private final Schema.Vertex.Type type;

            Thing(Prefix prefix, Schema.Vertex.Type type) {
                this.prefix = prefix;
                this.type = type;
            }

            public static Thing of(byte prefix) {
                for (Thing t : Thing.values()) {
                    if (t.prefix.key == prefix) return t;
                }
                throw new GraknException(Error.Internal.UNRECOGNISED_VALUE);
            }

            public static Thing of(Type type) {
                for (Thing t : Thing.values()) {
                    if (t.type().equals(type)) return t;
                }
                throw new GraknException(Error.Internal.UNRECOGNISED_VALUE);
            }

            @Override
            public Prefix prefix() {
                return prefix;
            }

            public Type type() {
                return type;
            }
        }

    }

    public interface Edge {

        Edge ISA = new Edge() {

            @Override
            public Infix out() { return null; }

            @Override
            public Infix in() { return Infix.EDGE_ISA_IN; }

            @Override
            public boolean isOptimisation() { return false; }
        };

        static boolean isOut(byte infix) {
            return infix > 0;
        }

        Infix out();

        Infix in();

        boolean isOptimisation();

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
                throw new GraknException(Error.Internal.UNRECOGNISED_VALUE);
            }

            @Override
            public Infix out() {
                return out;
            }

            @Override
            public Infix in() {
                return in;
            }

            @Override
            public boolean isOptimisation() {
                return false;
            }
        }

        enum Thing implements Edge {
            HAS(Infix.EDGE_HAS_OUT, Infix.EDGE_HAS_IN),
            PLAYS(Infix.EDGE_PLAYS_OUT, Infix.EDGE_PLAYS_IN),
            RELATES(Infix.EDGE_RELATES_OUT, Infix.EDGE_RELATES_IN),
            ROLEPLAYER(Infix.EDGE_ROLEPLAYER_OUT, Infix.EDGE_ROLEPLAYER_IN, true, 1);

            private final Infix out;
            private final Infix in;
            private final boolean isOptimisation;
            private final int tailSize;

            Thing(Infix out, Infix in) {
                this(out, in, false, 0);
            }

            Thing(Infix out, Infix in, boolean isOptimisation, int tailSize) {
                this.out = out;
                this.in = in;
                this.isOptimisation = isOptimisation;
                this.tailSize = tailSize;
                assert out == null || out.isOptimisation() == isOptimisation;
                assert in == null || in.isOptimisation() == isOptimisation;
            }

            public static Thing of(Infix infix) {
                return of(infix.key);
            }

            public static Thing of(byte infix) {
                for (Thing t : Thing.values()) {
                    if ((t.out != null && t.out.key == infix) || (t.in != null && t.in.key == infix)) {
                        return t;
                    }
                }
                throw new GraknException(Error.Internal.UNRECOGNISED_VALUE);
            }

            @Override
            public Infix out() {
                return out;
            }

            @Override
            public Infix in() {
                return in;
            }

            @Override
            public boolean isOptimisation() {
                return isOptimisation;
            }

            public int tailSize() {
                return tailSize;
            }

            public int lookAhead() {
                return tailSize + 2;
            }
        }
    }
}
