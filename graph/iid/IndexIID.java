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

package grakn.core.graph.iid;

import grakn.core.common.exception.GraknException;
import grakn.core.graph.util.Encoding;

import javax.annotation.Nullable;
import java.time.LocalDateTime;

import static grakn.core.common.collection.Bytes.DOUBLE_SIZE;
import static grakn.core.common.collection.Bytes.LONG_SIZE;
import static grakn.core.common.collection.Bytes.booleanToByte;
import static grakn.core.common.collection.Bytes.byteToBoolean;
import static grakn.core.common.collection.Bytes.bytesToDateTime;
import static grakn.core.common.collection.Bytes.bytesToString;
import static grakn.core.common.collection.Bytes.dateTimeToBytes;
import static grakn.core.common.collection.Bytes.doubleToSortedBytes;
import static grakn.core.common.collection.Bytes.join;
import static grakn.core.common.collection.Bytes.longToSortedBytes;
import static grakn.core.common.collection.Bytes.sortedBytesToDouble;
import static grakn.core.common.collection.Bytes.sortedBytesToLong;
import static grakn.core.common.collection.Bytes.stringToBytes;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.graph.util.Encoding.ValueType.STRING_ENCODING;
import static grakn.core.graph.util.Encoding.ValueType.TIME_ZONE_ID;
import static java.util.Arrays.copyOfRange;

public abstract class IndexIID extends IID {

    IndexIID(byte[] bytes) {
        super(bytes);
    }


    public static abstract class Type extends IndexIID {

        Type(byte[] bytes) {
            super(bytes);
        }

        public static class Label extends Type {

            Label(byte[] bytes) {
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
                return new Label(join(Encoding.Index.Prefix.TYPE.prefix().bytes(),
                                      Encoding.Vertex.Type.scopedLabel(label, scope).getBytes(STRING_ENCODING)));
            }

            @Override
            public String toString() {
                if (readableString == null) {
                    readableString = "[" + PrefixIID.LENGTH + ": " + Encoding.Index.Prefix.TYPE.toString() + "]" +
                            "[" + (bytes.length - PrefixIID.LENGTH) +
                            ": " + new String(copyOfRange(bytes, PrefixIID.LENGTH, bytes.length), STRING_ENCODING) + "]";
                }
                return readableString;
            }
        }

        // type -> rule indexing
        public static abstract class Rule extends Type {

            Rule(byte[] bytes) {
                super(bytes);
            }

            public int length() { return bytes.length; }

            public static class Key extends Type.Rule {

                public Key(byte[] bytes) {
                    super(bytes);
                }

                /**
                 * @return a byte array representing the index of a given type concluded in a given rule
                 */
                public static Key concludedVertex(VertexIID.Type typeIID, StructureIID.Rule ruleIID) {
                    return new Key(join(Encoding.Index.Prefix.TYPE.bytes(), typeIID.bytes(),
                                        Encoding.Index.Infix.CONCLUDED_VERTEX.bytes(), ruleIID.bytes()));
                }

                /**
                 * @return a byte array representing the index of a given type concluded in a given rule
                 */
                public static Key concludedEdgeTo(VertexIID.Type typeIID, StructureIID.Rule ruleIID) {
                    return new Key(join(Encoding.Index.Prefix.TYPE.bytes(), typeIID.bytes(),
                                        Encoding.Index.Infix.CONCLUDED_EDGE_TO.bytes(), ruleIID.bytes()));
                }


                /**
                 * @return a byte array representing the index of a given type contained in a given rule
                 */
                public static Key contained(VertexIID.Type typeIID, StructureIID.Rule ruleIID) {
                    return new Key(join(Encoding.Index.Prefix.TYPE.bytes(), typeIID.bytes(),
                                        Encoding.Index.Infix.CONTAINED_TYPE.bytes(), ruleIID.bytes()));
                }

                @Override
                public String toString() {
                    if (readableString == null) {
                        String prefix = "[" + PrefixIID.LENGTH + ": " + Encoding.Index.Prefix.TYPE.toString() + "]";
                        String typeIID = "[" + (VertexIID.Type.LENGTH) + ": " + bytesToString(
                                copyOfRange(bytes, PrefixIID.LENGTH, VertexIID.Type.LENGTH), STRING_ENCODING) + "]";
                        String infix = "[" + (Encoding.Index.Infix.LENGTH) + ": " + Encoding.Index.Infix.of(
                                copyOfRange(bytes, PrefixIID.LENGTH + VertexIID.Type.LENGTH, Encoding.Index.Infix.LENGTH)) + "]";
                        readableString = prefix + typeIID + infix;
                    }
                    return readableString;
                }
            }

            public static class Prefix extends Type.Rule {

                public Prefix(byte[] bytes) {
                    super(bytes);
                }

                /**
                 * @return a byte array representing the the index scan prefix of a given type concluded in rules
                 */
                public static Prefix concludedVertex(VertexIID.Type typeIID) {
                    return new Prefix(join(Encoding.Index.Prefix.TYPE.bytes(), typeIID.bytes(),
                                           Encoding.Index.Infix.CONCLUDED_VERTEX.bytes()));
                }

                /**
                 * @return a byte array representing the index prefix scan of a given type concluded in rules
                 */
                public static Prefix concludedEdgeTo(VertexIID.Type typeIID) {
                    return new Prefix(join(Encoding.Index.Prefix.TYPE.bytes(), typeIID.bytes(),
                                           Encoding.Index.Infix.CONCLUDED_EDGE_TO.bytes()));
                }

                /**
                 * @return a byte array representing the index scan prefix of a given type contained in rules
                 */
                public static Prefix contained(VertexIID.Type typeIID) {
                    return new Prefix(join(Encoding.Index.Prefix.TYPE.bytes(), typeIID.bytes(),
                                           Encoding.Index.Infix.CONTAINED_TYPE.bytes()));
                }

                @Override
                public String toString() {
                    if (readableString == null) {
                        String prefix = "[" + PrefixIID.LENGTH + ": " + Encoding.Index.Prefix.TYPE.toString() + "]";
                        String typeIID = "[" + (VertexIID.Type.LENGTH) + ": " + bytesToString(
                                copyOfRange(bytes, PrefixIID.LENGTH, VertexIID.Type.LENGTH), STRING_ENCODING) + "]";
                        String infix = "[" + (Encoding.Index.Infix.LENGTH) + ": " + Encoding.Index.Infix.of(
                                copyOfRange(bytes, PrefixIID.LENGTH + VertexIID.Type.LENGTH, Encoding.Index.Infix.LENGTH)) + "]";
                        String ruleIID = "[" + (StructureIID.Rule.LENGTH) + ": " + bytesToString(
                                copyOfRange(bytes, PrefixIID.LENGTH + VertexIID.Type.LENGTH + Encoding.Index.Infix.LENGTH,
                                            StructureIID.Rule.LENGTH), STRING_ENCODING) + "]";
                        readableString = prefix + typeIID + infix + ruleIID;
                    }
                    return readableString;
                }
            }
        }
    }


    public static class Rule extends IndexIID {

        Rule(byte[] bytes) { super(bytes); }

        /**
         * Returns the index address of given {@code RuleStructure}
         *
         * @param label of the {@code RuleStructure}
         * @return a byte array representing the index address of a {@code RuleStructure}
         */
        public static Rule of(String label) {
            return new Rule(join(prefix().bytes(), label.getBytes(STRING_ENCODING)));
        }

        public static Encoding.Prefix prefix() {
            return Encoding.Index.Prefix.RULE.prefix();
        }

        @Override
        public String toString() {
            if (readableString == null) {
                readableString = "[" + PrefixIID.LENGTH + ": " + Encoding.Index.Prefix.RULE.toString() + "]" +
                        "[" + (bytes.length - PrefixIID.LENGTH) +
                        ": " + new String(copyOfRange(bytes, PrefixIID.LENGTH, bytes.length), STRING_ENCODING) + "]";
            }
            return readableString;
        }
    }

    public static class Attribute extends IndexIID {

        static final int VALUE_INDEX = PrefixIID.LENGTH + VertexIID.Attribute.VALUE_TYPE_LENGTH;

        Attribute(byte[] bytes) {
            super(bytes);
        }

        private static Attribute newAttributeIndex(byte[] valueType, byte[] value, byte[] typeIID) {
            return new Attribute(join(Encoding.Index.Prefix.ATTRIBUTE.prefix().bytes(), valueType, value, typeIID));
        }

        public static Attribute of(boolean value, VertexIID.Type typeIID) {
            return newAttributeIndex(Encoding.ValueType.BOOLEAN.bytes(), new byte[]{booleanToByte(value)}, typeIID.bytes);
        }

        public static Attribute of(long value, VertexIID.Type typeIID) {
            return newAttributeIndex(Encoding.ValueType.LONG.bytes(), longToSortedBytes(value), typeIID.bytes);
        }

        public static Attribute of(double value, VertexIID.Type typeIID) {
            return newAttributeIndex(Encoding.ValueType.DOUBLE.bytes(), doubleToSortedBytes(value), typeIID.bytes);
        }

        public static Attribute of(String value, VertexIID.Type typeIID) {
            byte[] stringBytes;
            try {
                stringBytes = stringToBytes(value, STRING_ENCODING);
            } catch (Exception e) {
                throw GraknException.of(ILLEGAL_STATE);
            }
            return newAttributeIndex(Encoding.ValueType.STRING.bytes(), stringBytes, typeIID.bytes);
        }

        public static Attribute of(LocalDateTime value, VertexIID.Type typeIID) {
            return newAttributeIndex(Encoding.ValueType.DATETIME.bytes(), dateTimeToBytes(value, TIME_ZONE_ID), typeIID.bytes);
        }

        @Override
        public String toString() {
            if (readableString == null) {
                final Encoding.ValueType valueType = Encoding.ValueType.of(bytes[PrefixIID.LENGTH]);
                final String value;
                switch (valueType) {
                    case BOOLEAN:
                        value = byteToBoolean(bytes[VALUE_INDEX]).toString();
                        break;
                    case LONG:
                        value = sortedBytesToLong(copyOfRange(bytes, VALUE_INDEX, VALUE_INDEX + LONG_SIZE)) + "";
                        break;
                    case DOUBLE:
                        value = sortedBytesToDouble(copyOfRange(bytes, VALUE_INDEX, VALUE_INDEX + DOUBLE_SIZE)) + "";
                        break;
                    case STRING:
                        value = bytesToString(copyOfRange(bytes, VALUE_INDEX, bytes.length - VertexIID.Type.LENGTH), STRING_ENCODING);
                        break;
                    case DATETIME:
                        value = bytesToDateTime(copyOfRange(bytes, VALUE_INDEX, bytes.length - VertexIID.Type.LENGTH), TIME_ZONE_ID).toString();
                        break;
                    default:
                        value = "";
                        break;
                }

                readableString = "[" + PrefixIID.LENGTH + ": " + Encoding.Index.Prefix.ATTRIBUTE.toString() + "]" +
                        "[" + VertexIID.Attribute.VALUE_TYPE_LENGTH + ": " + valueType.toString() + "]" +
                        "[" + (bytes.length - (PrefixIID.LENGTH + VertexIID.Attribute.VALUE_TYPE_LENGTH + VertexIID.Type.LENGTH)) + ": " + value + "]" +
                        "[" + VertexIID.Type.LENGTH + ": " + VertexIID.Type.of(copyOfRange(bytes, bytes.length - VertexIID.Type.LENGTH, bytes.length)).toString() + "]";
            }
            return readableString;
        }
    }
}
