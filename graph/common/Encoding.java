/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.graph.common;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typeql.lang.common.TypeQLArg;

import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.map;
import static com.vaticle.typedb.common.collection.Collections.pair;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.collection.Bytes.SHORT_SIZE;
import static com.vaticle.typedb.core.common.collection.Bytes.SHORT_UNSIGNED_MAX_VALUE;
import static com.vaticle.typedb.core.common.collection.Bytes.signedByte;
import static com.vaticle.typedb.core.common.collection.Bytes.unsignedByte;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
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
        SYSTEM,
        INDEX,
        STATISTICS,
        TYPE,
        THING,
        RULE;
    }

    /**
     * The values in this class will be used as 'prefixes' within an IID in the
     * of every object database, and must not overlap with each other.
     *
     * The size of a prefix is 1 unsigned byte; i.e. min-value = 0 and max-value = 255.
     */
    public enum Prefix {
        // leave large open range for future indices
        INDEX_TYPE(0, PrefixType.INDEX),
        INDEX_RULE(10, PrefixType.INDEX),
        INDEX_ATTRIBUTE(20, PrefixType.INDEX),
        STATISTICS_THINGS(50, PrefixType.STATISTICS),
        STATISTICS_COUNT_JOB(51, PrefixType.STATISTICS),
        STATISTICS_COUNTED(52, PrefixType.STATISTICS),
        STATISTICS_SNAPSHOT(53, PrefixType.STATISTICS),
        SYSTEM(70, PrefixType.SYSTEM), // TODO reorganise SYSTEM to come first when releasing an incompatible storage
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

        private static final ByteMap<Prefix> prefixByKey = ByteMap.create(
                pair(SYSTEM.key, SYSTEM),
                pair(INDEX_TYPE.key, INDEX_TYPE),
                pair(INDEX_RULE.key, INDEX_RULE),
                pair(INDEX_ATTRIBUTE.key, INDEX_ATTRIBUTE),
                pair(STATISTICS_THINGS.key, STATISTICS_THINGS),
                pair(STATISTICS_COUNT_JOB.key, STATISTICS_COUNT_JOB),
                pair(STATISTICS_COUNTED.key, STATISTICS_COUNTED),
                pair(STATISTICS_SNAPSHOT.key, STATISTICS_SNAPSHOT),
                pair(VERTEX_THING_TYPE.key, VERTEX_THING_TYPE),
                pair(VERTEX_ENTITY_TYPE.key, VERTEX_ENTITY_TYPE),
                pair(VERTEX_ATTRIBUTE_TYPE.key, VERTEX_ATTRIBUTE_TYPE),
                pair(VERTEX_RELATION_TYPE.key, VERTEX_RELATION_TYPE),
                pair(VERTEX_ROLE_TYPE.key, VERTEX_ROLE_TYPE),
                pair(VERTEX_ENTITY.key, VERTEX_ENTITY),
                pair(VERTEX_ATTRIBUTE.key, VERTEX_ATTRIBUTE),
                pair(VERTEX_RELATION.key, VERTEX_RELATION),
                pair(VERTEX_ROLE.key, VERTEX_ROLE),
                pair(STRUCTURE_RULE.key, STRUCTURE_RULE)
        );

        private final byte key;
        private final PrefixType type;
        private final ByteArray bytes;

        Prefix(int key, PrefixType type) {
            assert key < 200 : "The encoding range >= 200 is reserved for TypeDB Cluster.";
            this.key = unsignedByte(key);
            this.type = type;
            this.bytes = ByteArray.of(new byte[]{this.key});
        }

        public static Prefix of(byte key) {
            Prefix prefix = prefixByKey.get(key);
            if (prefix == null) throw TypeDBException.of(UNRECOGNISED_VALUE);
            else return prefix;
        }

        public byte key() {
            return key;
        }

        public ByteArray bytes() {
            return bytes;
        }

        public PrefixType type() {
            return type;
        }

        public boolean isIndex() {
            return type.equals(PrefixType.INDEX);
        }

        public boolean isStatistics() {
            return type.equals(PrefixType.STATISTICS);
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

        public boolean isSystem() {
            return type.equals(PrefixType.SYSTEM);
        }

    }

    /**
     * The values in this class will be used as 'infixes' between two IIDs of
     * two objects in the database, and must not overlap with each other.
     *
     * The size of a prefix is 1 signed byte; i.e. min-value = -128 and max-value = 127.
     */
    public enum Infix {
        PROPERTY_LABEL(0),
        PROPERTY_SCOPE(1),
        PROPERTY_ABSTRACT(2),
        PROPERTY_VALUE_TYPE(3),
        PROPERTY_REGEX(4),
        PROPERTY_WHEN(5),
        PROPERTY_THEN(6),
        PROPERTY_VALUE(7),
        PROPERTY_VALUE_REF(8),
        EDGE_ISA_IN(-40), // EDGE_ISA_OUT does not exist by design
        EDGE_SUB_OUT(50),
        EDGE_SUB_IN(-50),
        EDGE_OWNS_OUT(51),
        EDGE_OWNS_IN(-51),
        EDGE_OWNS_KEY_OUT(52),
        EDGE_OWNS_KEY_IN(-52),
        EDGE_PLAYS_OUT(53),
        EDGE_PLAYS_IN(-53),
        EDGE_RELATES_OUT(54),
        EDGE_RELATES_IN(-54),
        EDGE_HAS_OUT(70),
        EDGE_HAS_IN(-70),
        EDGE_PLAYING_OUT(71),
        EDGE_PLAYING_IN(-71),
        EDGE_RELATING_OUT(72),
        EDGE_RELATING_IN(-72),
        EDGE_ROLEPLAYER_OUT(73, true),
        EDGE_ROLEPLAYER_IN(-73, true);

        private static final ByteMap<Infix> infixByKey = ByteMap.create(
                pair(PROPERTY_LABEL.key, PROPERTY_LABEL),
                pair(PROPERTY_SCOPE.key, PROPERTY_SCOPE),
                pair(PROPERTY_ABSTRACT.key, PROPERTY_ABSTRACT),
                pair(PROPERTY_VALUE_TYPE.key, PROPERTY_VALUE_TYPE),
                pair(PROPERTY_REGEX.key, PROPERTY_REGEX),
                pair(PROPERTY_WHEN.key, PROPERTY_WHEN),
                pair(PROPERTY_THEN.key, PROPERTY_THEN),
                pair(PROPERTY_VALUE.key, PROPERTY_VALUE),
                pair(PROPERTY_VALUE_REF.key, PROPERTY_VALUE_REF),
                pair(EDGE_ISA_IN.key, EDGE_ISA_IN),
                pair(EDGE_SUB_OUT.key, EDGE_SUB_OUT),
                pair(EDGE_SUB_IN.key, EDGE_SUB_IN),
                pair(EDGE_OWNS_OUT.key, EDGE_OWNS_OUT),
                pair(EDGE_OWNS_IN.key, EDGE_OWNS_IN),
                pair(EDGE_OWNS_KEY_OUT.key, EDGE_OWNS_KEY_OUT),
                pair(EDGE_OWNS_KEY_IN.key, EDGE_OWNS_KEY_IN),
                pair(EDGE_PLAYS_OUT.key, EDGE_PLAYS_OUT),
                pair(EDGE_PLAYS_IN.key, EDGE_PLAYS_IN),
                pair(EDGE_RELATES_OUT.key, EDGE_RELATES_OUT),
                pair(EDGE_RELATES_IN.key, EDGE_RELATES_IN),
                pair(EDGE_HAS_OUT.key, EDGE_HAS_OUT),
                pair(EDGE_HAS_IN.key, EDGE_HAS_IN),
                pair(EDGE_PLAYING_OUT.key, EDGE_PLAYING_OUT),
                pair(EDGE_PLAYING_IN.key, EDGE_PLAYING_IN),
                pair(EDGE_RELATING_OUT.key, EDGE_RELATING_OUT),
                pair(EDGE_RELATING_IN.key, EDGE_RELATING_IN),
                pair(EDGE_ROLEPLAYER_OUT.key, EDGE_ROLEPLAYER_OUT),
                pair(EDGE_ROLEPLAYER_IN.key, EDGE_ROLEPLAYER_IN)
        );

        private final byte key;
        private final boolean isOptimisation;
        private final ByteArray bytes;

        Infix(int key) {
            this(key, false);
        }

        Infix(int key, boolean isOptimisation) {
            this.key = signedByte(key);
            this.isOptimisation = isOptimisation;
            this.bytes = ByteArray.of(new byte[]{this.key});
        }

        public static Infix of(byte key) {
            Infix infix = infixByKey.get(key); // already unsigned??
            if (infix == null) throw TypeDBException.of(UNRECOGNISED_VALUE);
            else return infix;
        }

        public byte key() {
            return key;
        }

        public ByteArray bytes() {
            return bytes;
        }

        public boolean isOptimisation() {
            return isOptimisation;
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

    /**
     * The size of a prefix is 1 unsigned byte; i.e. min-value = 0 and max-value = 255.
     */
    public enum ValueType {
        OBJECT(0, Object.class, false, false, null),
        BOOLEAN(10, Boolean.class, true, false, TypeQLArg.ValueType.BOOLEAN),
        LONG(20, Long.class, true, true, TypeQLArg.ValueType.LONG),
        DOUBLE(30, Double.class, true, false, TypeQLArg.ValueType.DOUBLE),
        STRING(40, String.class, true, true, TypeQLArg.ValueType.STRING),
        DATETIME(50, LocalDateTime.class, true, true, TypeQLArg.ValueType.DATETIME);
        public static final ZoneId TIME_ZONE_ID = ZoneId.of("Z");
        public static final Charset STRING_ENCODING = UTF_8;
        public static final int STRING_SIZE_ENCODING = SHORT_SIZE;
        public static final int STRING_MAX_SIZE = SHORT_UNSIGNED_MAX_VALUE;
        public static final double DOUBLE_PRECISION = 0.0000000000000001;

        private static final ByteMap<ValueType> valueTypeByKey = ByteMap.create(
                pair(OBJECT.key, OBJECT),
                pair(BOOLEAN.key, BOOLEAN),
                pair(LONG.key, LONG),
                pair(DOUBLE.key, DOUBLE),
                pair(STRING.key, STRING),
                pair(DATETIME.key, DATETIME)
        );

        private static final Map<ValueType, Set<ValueType>> ASSIGNABLES = map(
                pair(OBJECT, set(OBJECT)),
                pair(BOOLEAN, set(BOOLEAN)),
                pair(LONG, set(LONG, DOUBLE)),
                pair(DOUBLE, set(DOUBLE)),
                pair(STRING, set(STRING)),
                pair(DATETIME, set(DATETIME))
        );

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

        private final TypeQLArg.ValueType typeQLValueType;
        private final ByteArray bytes;

        ValueType(int key, Class<?> valueClass, boolean isWritable, boolean isKeyable,
                  @Nullable TypeQLArg.ValueType typeQLValueType) {
            this.key = unsignedByte(key);
            this.bytes = ByteArray.of(new byte[]{this.key});
            this.valueClass = valueClass;
            this.isWritable = isWritable;
            this.isKeyable = isKeyable;
            this.typeQLValueType = typeQLValueType;
        }

        public static ValueType of(byte value) {
            ValueType valueType = valueTypeByKey.get(value);
            if (valueType == null) throw TypeDBException.of(UNRECOGNISED_VALUE);
            else return valueType;
        }

        public static ValueType of(Class<?> valueClass) {
            if (valueClass == OBJECT.valueClass) return OBJECT;
            else if (valueClass == BOOLEAN.valueClass) return BOOLEAN;
            else if (valueClass == LONG.valueClass) return LONG;
            else if (valueClass == DOUBLE.valueClass) return DOUBLE;
            else if (valueClass == STRING.valueClass) return STRING;
            else if (valueClass == DATETIME.valueClass) return DATETIME;
            else throw TypeDBException.of(UNRECOGNISED_VALUE);
        }

        public static ValueType of(TypeQLArg.ValueType typeQLValueType) {
            if (typeQLValueType == OBJECT.typeQLValueType) return OBJECT;
            else if (typeQLValueType == BOOLEAN.typeQLValueType) return BOOLEAN;
            else if (typeQLValueType == LONG.typeQLValueType) return LONG;
            else if (typeQLValueType == DOUBLE.typeQLValueType) return DOUBLE;
            else if (typeQLValueType == STRING.typeQLValueType) return STRING;
            else if (typeQLValueType == DATETIME.typeQLValueType) return DATETIME;
            else throw TypeDBException.of(UNRECOGNISED_VALUE);
        }

        public ByteArray bytes() {
            return bytes;
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

        public Set<ValueType> assignables() {
            return ASSIGNABLES.get(this);
        }

        public Set<ValueType> comparables() {
            return COMPARABLES.get(this);
        }

        public boolean comparableTo(ValueType valueType) {
            return COMPARABLES.get(this).contains(valueType);
        }

        public TypeQLArg.ValueType typeQLValueType() {
            return typeQLValueType;
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

            private static final ByteMap<Type> typeVertexByKey = ByteMap.create(
                    pair(THING_TYPE.prefix.key, THING_TYPE),
                    pair(ENTITY_TYPE.prefix.key, ENTITY_TYPE),
                    pair(ATTRIBUTE_TYPE.prefix.key, ATTRIBUTE_TYPE),
                    pair(RELATION_TYPE.prefix.key, RELATION_TYPE),
                    pair(ROLE_TYPE.prefix.key, ROLE_TYPE)
            );

            private final Prefix prefix;
            private final Root root;
            private final Thing instance;

            Type(Prefix prefix, Root root, Thing instance) {
                this.prefix = prefix;
                this.root = root;
                this.instance = instance;
            }

            public static Type of(byte prefix) {
                Type type = typeVertexByKey.get(prefix);
                if (type == null) throw TypeDBException.of(UNRECOGNISED_VALUE);
                else return type;
            }

            public static Type of(Thing thing) {
                if (Objects.equals(thing, THING_TYPE.instance)) return THING_TYPE;
                else if (Objects.equals(thing, ENTITY_TYPE.instance)) return ENTITY_TYPE;
                else if (Objects.equals(thing, ATTRIBUTE_TYPE.instance)) return ATTRIBUTE_TYPE;
                else if (Objects.equals(thing, RELATION_TYPE.instance)) return RELATION_TYPE;
                else if (Objects.equals(thing, ROLE_TYPE.instance)) return ROLE_TYPE;
                else throw TypeDBException.of(UNRECOGNISED_VALUE);
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

            private static final ByteMap<Thing> thingVertexByKey = ByteMap.create(
                    pair(ENTITY.prefix.key, ENTITY),
                    pair(ATTRIBUTE.prefix.key, ATTRIBUTE),
                    pair(RELATION.prefix.key, RELATION),
                    pair(ROLE.prefix.key, ROLE)
            );

            private final Prefix prefix;

            Thing(Prefix prefix) {
                this.prefix = prefix;
            }

            public static Thing of(byte prefix) {
                Thing thing = thingVertexByKey.get(prefix);
                if (thing == null) throw TypeDBException.of(UNRECOGNISED_VALUE);
                else return thing;
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
            throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Type.class));
        }

        default Thing asThing() {
            throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Thing.class));
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
                if (infix == SUB.in.key || infix == SUB.out.key) return SUB;
                else if (infix == OWNS.in.key || infix == OWNS.out.key) return OWNS;
                else if (infix == OWNS_KEY.in.key || infix == OWNS_KEY.out.key) return OWNS_KEY;
                else if (infix == PLAYS.in.key || infix == PLAYS.out.key) return PLAYS;
                else if (infix == RELATES.in.key || infix == RELATES.out.key) return RELATES;
                else throw TypeDBException.of(UNRECOGNISED_VALUE);
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
                if ((HAS.out != null && HAS.out.key == infix) || (HAS.in != null && HAS.in.key == infix)) {
                    return HAS;
                } else if ((PLAYING.out != null && PLAYING.out.key == infix) || (PLAYING.in != null && PLAYING.in.key == infix)) {
                    return PLAYING;
                } else if ((RELATING.out != null && RELATING.out.key == infix) || (RELATING.in != null && RELATING.in.key == infix)) {
                    return RELATING;
                } else if ((ROLEPLAYER.out != null && ROLEPLAYER.out.key == infix) || (ROLEPLAYER.in != null && ROLEPLAYER.in.key == infix)) {
                    return ROLEPLAYER;
                } else {
                    throw TypeDBException.of(UNRECOGNISED_VALUE);
                }
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

    public interface Index {

        enum Prefix {
            TYPE(Encoding.Prefix.INDEX_TYPE),
            RULE(Encoding.Prefix.INDEX_RULE),
            ATTRIBUTE(Encoding.Prefix.INDEX_ATTRIBUTE);

            private final Encoding.Prefix prefix;

            Prefix(Encoding.Prefix prefix) {
                this.prefix = prefix;
            }

            public Encoding.Prefix prefix() {
                return prefix;
            }

            public ByteArray bytes() {
                return prefix.bytes();
            }
        }

        /**
         * The size of a prefix is 1 unsigned byte; i.e. min-value = 0 and max-value = 255.
         */
        enum Infix {
            CONTAINED_TYPE(0),
            CONCLUDED_VERTEX(10),
            CONCLUDED_EDGE_TO(11);

            public static final int LENGTH = 1;
            private final byte key;
            private final ByteArray bytes;

            Infix(int key) {
                this.key = unsignedByte(key);
                bytes = ByteArray.of(new byte[]{this.key});
            }

            public static Infix of(ByteArray key) {
                if (key.get(0) == CONTAINED_TYPE.key) return CONTAINED_TYPE;
                else if (key.get(0) == CONCLUDED_VERTEX.key) return CONCLUDED_VERTEX;
                else if (key.get(0) == CONCLUDED_EDGE_TO.key) return CONCLUDED_EDGE_TO;
                else throw TypeDBException.of(UNRECOGNISED_VALUE);
            }

            public ByteArray bytes() {
                return bytes;
            }
        }
    }

    public interface Statistics {

        /**
         * The size of a prefix is 1 unsigned byte; i.e. min-value = 0 and max-value = 255.
         */
        enum JobType {
            ATTRIBUTE_VERTEX(0),
            HAS_EDGE(1);

            private final byte key;
            private final ByteArray bytes;

            JobType(int key) {
                this.key = unsignedByte(key);
                this.bytes = ByteArray.of(new byte[]{this.key});
            }

            public static JobType of(ByteArray key) {
                if (key.length() == 1) {
                    if (key.get(0) == ATTRIBUTE_VERTEX.key) return ATTRIBUTE_VERTEX;
                    else if (key.get(0) == HAS_EDGE.key) return HAS_EDGE;
                    else throw TypeDBException.of(UNRECOGNISED_VALUE);
                }
                throw TypeDBException.of(UNRECOGNISED_VALUE);
            }

            public byte key() {
                return key;
            }

            public ByteArray bytes() {
                return bytes;
            }
        }

        /**
         * The size of a prefix is 1 unsigned byte; i.e. min-value = 0 and max-value = 255.
         */
        enum JobOperation {
            CREATED(0),
            DELETED(1);

            private final byte key;
            private final ByteArray bytes;

            JobOperation(int key) {
                this.key = unsignedByte(key);
                this.bytes = ByteArray.of(new byte[]{this.key});
            }

            public static JobOperation of(ByteArray key) {
                if (key.length() == 1) {
                    if (key.get(0) == CREATED.key) return CREATED;
                    else if (key.get(0) == DELETED.key) return DELETED;
                    else throw TypeDBException.of(UNRECOGNISED_VALUE);
                }
                throw TypeDBException.of(UNRECOGNISED_VALUE);
            }

            public byte key() {
                return key;
            }

            public ByteArray bytes() {
                return bytes;
            }
        }

        /**
         * The size of a prefix is 1 unsigned byte; i.e. min-value = 0 and max-value = 255.
         */
        enum Infix {
            VERTEX_COUNT(0),
            VERTEX_TRANSITIVE_COUNT(1),
            HAS_EDGE_COUNT(2),
            HAS_EDGE_TOTAL_COUNT(3);

            private final byte key;
            private final ByteArray bytes;

            Infix(int key) {
                this.key = unsignedByte(key);
                this.bytes = ByteArray.of(new byte[]{this.key});
            }

            public byte key() {
                return key;
            }

            public ByteArray bytes() {
                return bytes;
            }
        }
    }

    public enum System {

        TRANSACTION_DUMMY_WRITE(0);

        private final ByteArray bytes;

        System(int key) {
            byte b = unsignedByte(key);
            this.bytes = ByteArray.join(Prefix.SYSTEM.bytes(), ByteArray.of(new byte[]{b}));
        }

        public ByteArray bytes() {
            return bytes;
        }

    }

    private static class ByteMap<T> {

        private final ArrayList<T> listOrderedByByteValue;

        private ByteMap(ArrayList<T> listOrderedByByteValue) {
            this.listOrderedByByteValue = listOrderedByByteValue;
        }

        @SafeVarargs
        public static <T> ByteMap<T> create(Pair<Byte, T>... byteIndices) {
            ArrayList<T> indexList = new ArrayList<>(Collections.nCopies(255, (T) null));
            for (Pair<Byte, T> index : byteIndices) indexList.set(index.first() + 128, index.second());
            return new ByteMap<>(indexList);
        }

        T get(byte b) {
            return listOrderedByByteValue.get(b + 128);
        }
    }
}
