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

package com.vaticle.typedb.core.graph.iid;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.util.ByteArray;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.common.Encoding.ValueEncoding;

import javax.annotation.Nullable;
import java.time.LocalDateTime;

import static com.vaticle.typedb.core.common.collection.Bytes.DOUBLE_SIZE;
import static com.vaticle.typedb.core.common.collection.Bytes.LONG_SIZE;
import static com.vaticle.typedb.core.common.collection.Bytes.booleanToByte;
import static com.vaticle.typedb.core.common.collection.Bytes.byteToBoolean;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.STRING_ENCODING;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.TIME_ZONE_ID;

public abstract class IndexIID extends IID {

    IndexIID(ByteArray bytes) {
        super(bytes);
    }

    public static abstract class Type extends IndexIID {

        Type(ByteArray bytes) {
            super(bytes);
        }

        public static class Label extends Type {

            Label(ByteArray bytes) {
                super(bytes);
            }

            /**
             * Returns the index address of given {@code TypeVertex}
             *
             * @param label of the {@code TypeVertex}
             * @param scope of the {@code TypeVertex}, which could be null
             * @return a byte array representing the index address of a {@code TypeVertex}
             */
            public static Label of(String label, @Nullable String scope) {
                return new Label(ByteArray.join(Encoding.Index.Prefix.TYPE.prefix().bytes(),
                                                ByteArray.encodeString(Encoding.Vertex.Type.scopedLabel(label, scope), STRING_ENCODING)));
            }

            @Override
            public String toString() {
                if (readableString == null) {
                    readableString = "[" + PrefixIID.LENGTH + ": " + Encoding.Index.Prefix.TYPE.toString() + "]" +
                            "[" + (bytes.length() - PrefixIID.LENGTH) +
                            ": " + bytes.view(PrefixIID.LENGTH, bytes.length()).decodeString(STRING_ENCODING) + "]";
                }
                return readableString;
            }
        }

        // type -> rule indexing
        public static abstract class Rule extends Type {

            Rule(ByteArray bytes) {
                super(bytes);
            }

            public int length() { return bytes.length(); }

            public static class Key extends Type.Rule {

                public Key(ByteArray bytes) {
                    super(bytes);
                }

                /**
                 * @return a byte array representing the index of a given type concluded in a given rule
                 */
                public static Key concludedVertex(VertexIID.Type typeIID, StructureIID.Rule ruleIID) {
                    return new Key(ByteArray.join(Encoding.Index.Prefix.TYPE.bytes(), typeIID.bytes(),
                                                  Encoding.Index.Infix.CONCLUDED_VERTEX.bytes(), ruleIID.bytes()));
                }

                /**
                 * @return a byte array representing the index of a given type concluded in a given rule
                 */
                public static Key concludedEdgeTo(VertexIID.Type typeIID, StructureIID.Rule ruleIID) {
                    return new Key(ByteArray.join(Encoding.Index.Prefix.TYPE.bytes(), typeIID.bytes(),
                                                  Encoding.Index.Infix.CONCLUDED_EDGE_TO.bytes(), ruleIID.bytes()));
                }


                /**
                 * @return a byte array representing the index of a given type contained in a given rule
                 */
                public static Key contained(VertexIID.Type typeIID, StructureIID.Rule ruleIID) {
                    return new Key(ByteArray.join(Encoding.Index.Prefix.TYPE.bytes(), typeIID.bytes(),
                                                  Encoding.Index.Infix.CONTAINED_TYPE.bytes(), ruleIID.bytes()));
                }

                @Override
                public String toString() {
                    if (readableString == null) {
                        String prefix = "[" + PrefixIID.LENGTH + ": " + Encoding.Index.Prefix.TYPE.toString() + "]";
                        String typeIID = "[" + (VertexIID.Type.LENGTH) + ": " +
                                bytes.view(PrefixIID.LENGTH, VertexIID.Type.LENGTH).decodeString(STRING_ENCODING) + "]";
                        String infix = "[" + (Encoding.Index.Infix.LENGTH) + ": " + Encoding.Index.Infix.of(
                                bytes.view(PrefixIID.LENGTH + VertexIID.Type.LENGTH, Encoding.Index.Infix.LENGTH)) + "]";
                        readableString = prefix + typeIID + infix;
                    }
                    return readableString;
                }
            }

            public static class Prefix extends Type.Rule {

                public Prefix(ByteArray bytes) {
                    super(bytes);
                }

                /**
                 * @return a byte array representing the the index scan prefix of a given type concluded in rules
                 */
                public static Prefix concludedVertex(VertexIID.Type typeIID) {
                    return new Prefix(ByteArray.join(Encoding.Index.Prefix.TYPE.bytes(), typeIID.bytes(),
                                                     Encoding.Index.Infix.CONCLUDED_VERTEX.bytes()));
                }

                /**
                 * @return a byte array representing the index prefix scan of a given type concluded in rules
                 */
                public static Prefix concludedEdgeTo(VertexIID.Type typeIID) {
                    return new Prefix(ByteArray.join(Encoding.Index.Prefix.TYPE.bytes(), typeIID.bytes(),
                                                     Encoding.Index.Infix.CONCLUDED_EDGE_TO.bytes()));
                }

                /**
                 * @return a byte array representing the index scan prefix of a given type contained in rules
                 */
                public static Prefix contained(VertexIID.Type typeIID) {
                    return new Prefix(ByteArray.join(Encoding.Index.Prefix.TYPE.bytes(), typeIID.bytes(),
                                                     Encoding.Index.Infix.CONTAINED_TYPE.bytes()));
                }

                @Override
                public String toString() {
                    if (readableString == null) {
                        String prefix = "[" + PrefixIID.LENGTH + ": " + Encoding.Index.Prefix.TYPE.toString() + "]";
                        String typeIID = "[" + (VertexIID.Type.LENGTH) + ": " +
                                bytes.view(PrefixIID.LENGTH, VertexIID.Type.LENGTH).decodeString(STRING_ENCODING) + "]";
                        String infix = "[" + (Encoding.Index.Infix.LENGTH) + ": " + Encoding.Index.Infix.of(
                                bytes.view(PrefixIID.LENGTH + VertexIID.Type.LENGTH, Encoding.Index.Infix.LENGTH)) + "]";
                        String ruleIID = "[" + (StructureIID.Rule.LENGTH) + ": " +
                                bytes.view(PrefixIID.LENGTH + VertexIID.Type.LENGTH + Encoding.Index.Infix.LENGTH,
                                                StructureIID.Rule.LENGTH).decodeString(STRING_ENCODING) + "]";
                        readableString = prefix + typeIID + infix + ruleIID;
                    }
                    return readableString;
                }
            }
        }
    }


    public static class Rule extends IndexIID {

        Rule(ByteArray bytes) { super(bytes); }

        /**
         * Returns the index address of given {@code RuleStructure}
         *
         * @param label of the {@code RuleStructure}
         * @return a byte array representing the index address of a {@code RuleStructure}
         */
        public static Rule of(String label) {
            return new Rule(ByteArray.join(prefix().bytes(), ByteArray.encodeString(label, STRING_ENCODING)));
        }

        public static Encoding.Prefix prefix() {
            return Encoding.Index.Prefix.RULE.prefix();
        }

        @Override
        public String toString() {
            if (readableString == null) {
                readableString = "[" + PrefixIID.LENGTH + ": " + Encoding.Index.Prefix.RULE.toString() + "]" +
                        "[" + (bytes.length() - PrefixIID.LENGTH) +
                        ": " + bytes.view(PrefixIID.LENGTH).decodeString(STRING_ENCODING) + "]";
            }
            return readableString;
        }
    }

    public static class Attribute extends IndexIID {

        static final int VALUE_INDEX = PrefixIID.LENGTH + VertexIID.Attribute.VALUE_TYPE_LENGTH;

        Attribute(ByteArray bytes) {
            super(bytes);
        }

        private static Attribute newAttributeIndex(ByteArray valueType, ByteArray value, ByteArray typeIID) {
            return new Attribute(ByteArray.join(Encoding.Index.Prefix.ATTRIBUTE.prefix().bytes(), valueType, value, typeIID));
        }

        public static Attribute of(boolean value, VertexIID.Type typeIID) {
            return newAttributeIndex(Encoding.ValueType.BOOLEAN.bytes(), ByteArray.of(new byte[]{booleanToByte(value)}), typeIID.bytes);
        }

        public static Attribute of(long value, VertexIID.Type typeIID) {
            return newAttributeIndex(Encoding.ValueType.LONG.bytes(), ValueEncoding.longToBytes(value), typeIID.bytes);
        }

        public static Attribute of(double value, VertexIID.Type typeIID) {
            return newAttributeIndex(Encoding.ValueType.DOUBLE.bytes(), ValueEncoding.doubleToBytes(value), typeIID.bytes);
        }

        public static Attribute of(String value, VertexIID.Type typeIID) {
            ByteArray stringBytes;
            try {
                stringBytes = ByteArray.encodeString(value, STRING_ENCODING);
            } catch (Exception e) {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
            return newAttributeIndex(Encoding.ValueType.STRING.bytes(), stringBytes, typeIID.bytes);
        }

        public static Attribute of(LocalDateTime value, VertexIID.Type typeIID) {
            return newAttributeIndex(Encoding.ValueType.DATETIME.bytes(), ValueEncoding.dateTimeToBytes(value, TIME_ZONE_ID), typeIID.bytes);
        }

        @Override
        public String toString() {
            if (readableString == null) {
                Encoding.ValueType valueType = Encoding.ValueType.of(bytes.get(PrefixIID.LENGTH));
                String value;
                switch (valueType) {
                    case BOOLEAN:
                        value = byteToBoolean(bytes.get(VALUE_INDEX)).toString();
                        break;
                    case LONG:
                        value = ValueEncoding.bytesToLong(bytes.view(VALUE_INDEX, VALUE_INDEX + LONG_SIZE)) + "";
                        break;
                    case DOUBLE:
                        value = ValueEncoding.bytesToDouble(bytes.view(VALUE_INDEX, VALUE_INDEX + DOUBLE_SIZE)) + "";
                        break;
                    case STRING:
                        value = ValueEncoding.bytesToString(bytes.view(VALUE_INDEX, bytes.length() - VertexIID.Type.LENGTH), STRING_ENCODING);
                        break;
                    case DATETIME:
                        value = ValueEncoding.bytesToDateTime(bytes.view(VALUE_INDEX, bytes.length() - VertexIID.Type.LENGTH), TIME_ZONE_ID).toString();
                        break;
                    default:
                        value = "";
                        break;
                }

                readableString = "[" + PrefixIID.LENGTH + ": " + Encoding.Index.Prefix.ATTRIBUTE.toString() + "]" +
                        "[" + VertexIID.Attribute.VALUE_TYPE_LENGTH + ": " + valueType.toString() + "]" +
                        "[" + (bytes.length() - (PrefixIID.LENGTH + VertexIID.Attribute.VALUE_TYPE_LENGTH + VertexIID.Type.LENGTH)) + ": " + value + "]" +
                        "[" + VertexIID.Type.LENGTH + ": " + VertexIID.Type.of(bytes.copyRange(bytes.length() - VertexIID.Type.LENGTH)).toString() + "]";
            }
            return readableString;
        }
    }
}
