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

import grakn.core.common.collection.Bytes;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Label;
import graql.lang.common.GraqlArg;

import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static grakn.common.collection.Collections.map;
import static grakn.common.collection.Collections.pair;
import static grakn.common.collection.Collections.set;
import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static grakn.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static java.nio.charset.StandardCharsets.UTF_8;

public class Encoding {

    public static final String ROCKS_DATA = "data";
    public static final String ROCKS_SCHEMA = "schema";

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

    public interface Direction {

        enum Adjacency implements Direction {
            OUT(true),
            IN(false);

            private final boolean isOut;

            Adjacency(boolean isOut) {
                this.isOut = isOut;
            }

            public boolean isOut() {
                return isOut;
            }

            public boolean isIn() {
                return !isOut;
            }
        }

        enum Edge implements Direction {
            FORWARD(true),
            BACKWARD(false);

            private final boolean isForward;

            Edge(boolean isForward) {
                this.isForward = isForward;
            }

            public boolean isForward() {
                return isForward;
            }

            public boolean isBackward() {
                return !isForward;
            }
        }
    }

    public enum PrefixType {
        INDEX(0),
        TYPE(1),
        THING(2),
        RULE(3),
        STATISTICS(4);

        private final int key;

        PrefixType(int key) {
            this.key = key;
        }
    }

    /**
     * The values in this class will be used as 'prefixes' within an IID in the
     * of every object database, and must not overlap with each other.
     *
     * The size of a prefix is 1 byte; i.e. min-value = 0 and max-value = 255.
     */
    public enum Prefix {
        // leave large open range for future indices
        INDEX_TYPE(0, PrefixType.INDEX),
        INDEX_RULE(10, PrefixType.INDEX),
        INDEX_ATTRIBUTE(20, PrefixType.INDEX),
        STATISTICS_THINGS(50, PrefixType.STATISTICS),
        STATISTICS_COUNT_JOB(51, PrefixType.STATISTICS),
        STATISTICS_COUNTED(52, PrefixType.STATISTICS),
        VERTEX_THING_TYPE(100, PrefixType.TYPE),
        VERTEX_ENTITY_TYPE(110, PrefixType.TYPE),
        VERTEX_ATTRIBUTE_TYPE(120, PrefixType.TYPE),
        VERTEX_RELATION_TYPE(130, PrefixType.TYPE),
        VERTEX_ROLE_TYPE(140, PrefixType.TYPE),
        VERTEX_ENTITY(150, PrefixType.THING),
        VERTEX_ATTRIBUTE(160, PrefixType.THING),
        VERTEX_RELATION(170, PrefixType.THING),
        VERTEX_ROLE(180, PrefixType.THING),
        STRUCTURE_RULE(190, PrefixType.RULE);

        private final byte key;
        private final PrefixType type;

        Prefix(int key, PrefixType type) {
            this.key = (byte) key;
            this.type = type;
        }

        public static Prefix of(byte key) {
            for (Prefix i : Prefix.values()) {
                if (i.key == key) return i;
            }
            throw GraknException.of(UNRECOGNISED_VALUE);
        }

        public byte key() {
            return key;
        }

        public byte[] bytes() {
            return new byte[]{key};
        }

        public PrefixType type() {
            return type;
        }

        public boolean isIndex() {
            return type.equals(PrefixType.INDEX);
        }

        public boolean isType() {
            return type.equals(PrefixType.TYPE);
        }

        public boolean isThing() {
            return type.equals(PrefixType.THING);
        }

        public boolean isRule() {
            return type.equals(PrefixType.RULE);
        }
    }

    public enum StatisticsCountJobType {
        ATTRIBUTE_VERTEX(0),
        HAS_EDGE(1);

        private final byte key;

        StatisticsCountJobType(int key) {
            this.key = (byte) key;
        }

        public static StatisticsCountJobType of(byte[] key) {
            if (key.length == 1) {
                for (StatisticsCountJobType i : StatisticsCountJobType.values()) {
                    if (i.key == key[0]) return i;
                }
            }
            throw GraknException.of(UNRECOGNISED_VALUE);
        }

        public byte key() {
            return key;
        }

        public byte[] bytes() {
            return new byte[]{key};
        }
    }

    public enum StatisticsCountJobValue {
        CREATED(0),
        DELETED(1);

        private final byte key;

        StatisticsCountJobValue(int key) {
            this.key = (byte) key;
        }

        public static StatisticsCountJobValue of(byte[] key) {
            if (key.length == 1) {
                for (StatisticsCountJobValue i : StatisticsCountJobValue.values()) {
                    if (i.key == key[0]) return i;
                }
            }
            throw GraknException.of(UNRECOGNISED_VALUE);
        }

        public byte key() {
            return key;
        }

        public byte[] bytes() {
            return new byte[]{key};
        }
    }

    public enum StatisticsInfix {
        VERTEX_COUNT(0),
        VERTEX_TRANSITIVE_COUNT(1),
        HAS_EDGE_COUNT(2),
        HAS_EDGE_TOTAL_COUNT(3);

        private final byte key;

        StatisticsInfix(int key) {
            this.key = (byte) key;
        }

        public byte key() {
            return key;
        }

        public byte[] bytes() {
            return new byte[]{key};
        }
    }

    public enum InfixType {
        PROPERTY(0),
        EDGE(1);

        private final int key;

        InfixType(int key) {
            this.key = key;
        }
    }

    /**
     * The values in this class will be used as 'infixes' between two IIDs of
     * two objects in the database, and must not overlap with each other.
     *
     * The size of a prefix is 1 byte; i.e. min-value = 0 and max-value = 255.
     */
    // TODO I think we should compress these more
    // TODO for example, group properties 1-50, type edges 40-60, instance edges 60-80, reserved for future use 80-127
    public enum Infix {
        PROPERTY_LABEL(0, InfixType.PROPERTY),
        PROPERTY_SCOPE(1, InfixType.PROPERTY),
        PROPERTY_ABSTRACT(2, InfixType.PROPERTY),
        PROPERTY_VALUE_TYPE(3, InfixType.PROPERTY),
        PROPERTY_REGEX(4, InfixType.PROPERTY),
        PROPERTY_WHEN(5, InfixType.PROPERTY),
        PROPERTY_THEN(6, InfixType.PROPERTY),
        PROPERTY_VALUE(7, InfixType.PROPERTY),
        PROPERTY_VALUE_REF(8, InfixType.PROPERTY),
        EDGE_ISA_IN(-20, InfixType.EDGE), // EDGE_ISA_OUT does not exist by design
        EDGE_SUB_OUT(30, InfixType.EDGE),
        EDGE_SUB_IN(-30, InfixType.EDGE),
        EDGE_OWNS_OUT(40, InfixType.EDGE),
        EDGE_OWNS_IN(-40, InfixType.EDGE),
        EDGE_OWNS_KEY_OUT(50, InfixType.EDGE),
        EDGE_OWNS_KEY_IN(-50, InfixType.EDGE),
        EDGE_HAS_OUT(60, InfixType.EDGE),
        EDGE_HAS_IN(-60, InfixType.EDGE),
        EDGE_PLAYS_OUT(70, InfixType.EDGE),
        EDGE_PLAYS_IN(-70, InfixType.EDGE),
        EDGE_PLAYING_OUT(80, InfixType.EDGE),
        EDGE_PLAYING_IN(-80, InfixType.EDGE),
        EDGE_RELATES_OUT(90, InfixType.EDGE),
        EDGE_RELATES_IN(-90, InfixType.EDGE),
        EDGE_RELATING_OUT(100, InfixType.EDGE),
        EDGE_RELATING_IN(-100, InfixType.EDGE),
        EDGE_ROLEPLAYER_OUT(110, InfixType.EDGE, true),
        EDGE_ROLEPLAYER_IN(-110, InfixType.EDGE, true);

        private final byte key;
        private final boolean isOptimisation;
        private final InfixType type;

        Infix(int key, InfixType type) {
            this(key, type, false);
        }

        Infix(int key, InfixType type, boolean isOptimisation) {
            this.key = (byte) key;
            this.type = type;
            this.isOptimisation = isOptimisation;
        }

        public static Infix of(byte key) {
            for (Infix i : Infix.values()) {
                if (i.key == key) return i;
            }
            throw GraknException.of(UNRECOGNISED_VALUE);
        }

        public byte key() {
            return key;
        }

        public byte[] bytes() {
            return new byte[]{key};
        }

        public InfixType type() {
            return type;
        }

        public boolean isProperty() {
            return type.equals(InfixType.PROPERTY);
        }

        public boolean isEdge() {
            return type.equals(InfixType.EDGE);
        }

        public boolean isOptimisation() {
            return isOptimisation;
        }
    }

    public enum Index {
        TYPE(Prefix.INDEX_TYPE),
        RULE(Prefix.INDEX_RULE),
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
        OBJECT(0, Object.class, false, false, null),
        BOOLEAN(10, Boolean.class, true, false, GraqlArg.ValueType.BOOLEAN),
        LONG(20, Long.class, true, true, GraqlArg.ValueType.LONG),
        DOUBLE(30, Double.class, true, false, GraqlArg.ValueType.DOUBLE),
        STRING(40, String.class, true, true, GraqlArg.ValueType.STRING),
        DATETIME(50, LocalDateTime.class, true, true, GraqlArg.ValueType.DATETIME);

        public static final ZoneId TIME_ZONE_ID = ZoneId.of("Z");
        public static final Charset STRING_ENCODING = UTF_8;
        public static final int STRING_SIZE_ENCODING = Bytes.SHORT_SIZE;
        public static final int STRING_MAX_SIZE = Bytes.SHORT_UNSIGNED_MAX_VALUE;
        public static final double DOUBLE_PRECISION = 0.0000000000000001;

        private static final Map<ValueType, Set<ValueType>> COMPARABLES = map(
                pair(OBJECT, set(OBJECT)),
                pair(BOOLEAN, set(BOOLEAN)),
                pair(LONG, set(LONG, DOUBLE)),
                pair(DOUBLE, set(LONG, DOUBLE)),
                pair(STRING, set(STRING)),
                pair(DATETIME, set(DATETIME))
        );

        private final byte key;
        private final Class<?> valueClass;
        private final boolean isKeyable;
        private final boolean isWritable;
        private final GraqlArg.ValueType graqlValueType;

        ValueType(int key, Class<?> valueClass, boolean isWritable, boolean isKeyable,
                  @Nullable GraqlArg.ValueType graqlValueType) {
            this.key = (byte) key;
            this.valueClass = valueClass;
            this.isWritable = isWritable;
            this.isKeyable = isKeyable;
            this.graqlValueType = graqlValueType;
        }

        public static ValueType of(byte value) {
            for (ValueType vt : ValueType.values()) {
                if (vt.key == value) return vt;
            }
            throw GraknException.of(UNRECOGNISED_VALUE);
        }

        public static ValueType of(Class<?> valueClass) {
            for (ValueType vt : ValueType.values()) {
                if (vt.valueClass == valueClass) return vt;
            }
            throw GraknException.of(UNRECOGNISED_VALUE);
        }

        public static ValueType of(GraqlArg.ValueType graqlValueType) {
            for (ValueType vt : ValueType.values()) {
                if (vt.graqlValueType == graqlValueType) return vt;
            }
            throw GraknException.of(UNRECOGNISED_VALUE);
        }

        public byte[] bytes() {
            return new byte[]{key};
        }

        public Class<?> valueClass() {
            return valueClass;
        }

        public boolean isWritable() {
            return isWritable;
        }

        public boolean isKeyable() {
            return isKeyable;
        }

        public Set<ValueType> comparables() {
            return COMPARABLES.get(this);
        }

        public boolean comparableTo(ValueType valueType) {
            return COMPARABLES.get(this).contains(valueType);
        }

        public GraqlArg.ValueType graqlValueType() {
            return graqlValueType;
        }
    }

    public interface Structure {
        Prefix prefix();

        interface Rule extends Structure {

            String label();
        }

        Structure.Rule RULE = new Structure.Rule() {
            @Override
            public Prefix prefix() {
                return Prefix.STRUCTURE_RULE;
            }

            @Override
            public String label() {
                return "rule";
            }
        };
    }

    public interface Vertex {

        Prefix prefix();

        enum Type implements Vertex {
            THING_TYPE(Prefix.VERTEX_THING_TYPE, Root.THING, null),
            ENTITY_TYPE(Prefix.VERTEX_ENTITY_TYPE, Root.ENTITY, Thing.ENTITY),
            ATTRIBUTE_TYPE(Prefix.VERTEX_ATTRIBUTE_TYPE, Root.ATTRIBUTE, Thing.ATTRIBUTE),
            RELATION_TYPE(Prefix.VERTEX_RELATION_TYPE, Root.RELATION, Thing.RELATION),
            ROLE_TYPE(Prefix.VERTEX_ROLE_TYPE, Root.ROLE, Thing.ROLE);

            private final Prefix prefix;
            private final Root root;
            private final Thing instance;

            Type(Prefix prefix, Root root, Thing instance) {
                this.prefix = prefix;
                this.root = root;
                this.instance = instance;
            }

            public static Type of(byte prefix) {
                for (Type t : Type.values()) {
                    if (t.prefix.key == prefix) return t;
                }
                throw GraknException.of(UNRECOGNISED_VALUE);
            }

            public static Type of(Thing thing) {
                for (Type t : Type.values()) {
                    if (Objects.equals(t.instance, thing)) return t;
                }
                throw GraknException.of(UNRECOGNISED_VALUE);
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

            public Thing instance() {
                return instance;
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

                public Label properLabel() {
                    return Label.of(label, scope);
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

            public static Thing of(byte prefix) {
                for (Thing t : Thing.values()) {
                    if (t.prefix.key == prefix) return t;
                }
                throw GraknException.of(UNRECOGNISED_VALUE);
            }

            @Override
            public Prefix prefix() {
                return prefix;
            }
        }

    }

    public interface Edge {

        static boolean isOut(byte infix) {
            return infix > 0;
        }

        Infix out();

        Infix in();

        String name();

        default boolean isOptimisation() {
            return false;
        }

        default boolean isType() {
            return false;
        }

        default boolean isThing() {
            return false;
        }

        default Type asType() {
            throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(Type.class));
        }

        default Thing asThing() {
            throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(Thing.class));
        }

        Edge ISA = new Edge() {

            @Override
            public Infix out() {
                return null;
            }

            @Override
            public Infix in() {
                return Infix.EDGE_ISA_IN;
            }

            @Override
            public String name() {
                return "ISA";
            }

            @Override
            public String toString() {
                return name();
            }
        };

        enum Type implements Edge {
            SUB(Infix.EDGE_SUB_OUT, Infix.EDGE_SUB_IN),
            OWNS(Infix.EDGE_OWNS_OUT, Infix.EDGE_OWNS_IN),
            OWNS_KEY(Infix.EDGE_OWNS_KEY_OUT, Infix.EDGE_OWNS_KEY_IN),
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
                throw GraknException.of(UNRECOGNISED_VALUE);
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
            public boolean isType() {
                return true;
            }

            @Override
            public Type asType() {
                return this;
            }
        }

        enum Thing implements Edge {
            HAS(Infix.EDGE_HAS_OUT, Infix.EDGE_HAS_IN),
            PLAYING(Infix.EDGE_PLAYING_OUT, Infix.EDGE_PLAYING_IN),
            RELATING(Infix.EDGE_RELATING_OUT, Infix.EDGE_RELATING_IN),
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
                throw GraknException.of(UNRECOGNISED_VALUE);
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

            @Override
            public boolean isThing() {
                return true;
            }

            @Override
            public Thing asThing() {
                return this;
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
