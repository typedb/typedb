/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.encoding.iid;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.Encoding.Index;
import com.vaticle.typedb.core.encoding.key.Key;

import javax.annotation.Nullable;

import static com.vaticle.typedb.core.common.collection.ByteArray.encodeString;
import static com.vaticle.typedb.core.common.collection.ByteArray.join;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.STRING_ENCODING;

public abstract class IndexIID extends PartitionedIID {

    IndexIID(ByteArray bytes) {
        super(bytes);
    }

    @Override
    public Partition partition() {
        return Partition.DEFAULT;
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
                return new Label(join(Index.Prefix.TYPE.prefix().bytes(),
                        encodeString(Encoding.Vertex.Type.scopedLabel(label, scope), STRING_ENCODING)));
            }

            @Override
            public String toString() {
                if (readableString == null) {
                    readableString = "[" + PrefixIID.LENGTH + ": " + Index.Prefix.TYPE.toString() + "]" +
                            "[" + (bytes.length() - PrefixIID.LENGTH) +
                            ": " + bytes.view(PrefixIID.LENGTH, bytes.length()).decodeString(STRING_ENCODING) + "]";
                }
                return readableString;
            }
        }

        // type -> rule indexing
        public static class RuleUsage extends Type {

            private RuleUsage(ByteArray bytes) {
                super(bytes);
            }

            /**
             * @return a byte array representing the index of a given type concluded in a given rule
             */
            public static RuleUsage concludedVertex(VertexIID.Type typeIID, StructureIID.Rule ruleIID) {
                return new RuleUsage(join(Index.Prefix.TYPE.bytes(), typeIID.bytes,
                        Index.Infix.CONCLUDED_VERTEX.bytes(), ruleIID.bytes));
            }

            /**
             * @return a byte array representing the index of a given type concluded in a given rule
             */
            public static RuleUsage concludedEdgeTo(VertexIID.Type typeIID, StructureIID.Rule ruleIID) {
                return new RuleUsage(join(Index.Prefix.TYPE.bytes(), typeIID.bytes,
                        Index.Infix.CONCLUDED_EDGE_TO.bytes(), ruleIID.bytes));
            }

            /**
             * @return a byte array representing the index of a given type contained in a given rule
             */
            public static RuleUsage contained(VertexIID.Type typeIID, StructureIID.Rule ruleIID) {
                return new RuleUsage(join(Index.Prefix.TYPE.bytes(), typeIID.bytes,
                        Index.Infix.CONTAINED_TYPE.bytes(), ruleIID.bytes));
            }

            /**
             * @return a byte array representing the the index scan prefix of a given type concluded in rules
             */
            public static Key.Prefix<RuleUsage> prefixConcludedVertex(VertexIID.Type typeIID) {
                return new Key.Prefix<>(
                        join(Index.Prefix.TYPE.bytes(), typeIID.bytes, Index.Infix.CONCLUDED_VERTEX.bytes()),
                        Partition.DEFAULT,
                        RuleUsage::new
                );
            }

            /**
             * @return a byte array representing the index prefix scan of a given type concluded in rules
             */
            public static Key.Prefix<RuleUsage> prefixConcludedEdgeTo(VertexIID.Type typeIID) {
                return new Key.Prefix<>(
                        join(Index.Prefix.TYPE.bytes(), typeIID.bytes, Index.Infix.CONCLUDED_EDGE_TO.bytes()),
                        Partition.DEFAULT,
                        RuleUsage::new
                );
            }

            /**
             * @return a byte array representing the index scan prefix of a given type contained in rules
             */
            public static Key.Prefix<RuleUsage> prefixContained(VertexIID.Type typeIID) {
                return new Key.Prefix<>(
                        join(Index.Prefix.TYPE.bytes(), typeIID.bytes, Index.Infix.CONTAINED_TYPE.bytes()),
                        Partition.DEFAULT,
                        RuleUsage::new
                );
            }

            @Override
            public String toString() {
                if (readableString == null) {
                    String prefix = "[" + PrefixIID.LENGTH + ": " + Index.Prefix.TYPE.toString() + "]";
                    String typeIID = "[" + (VertexIID.Type.LENGTH) + ": " +
                            bytes.view(PrefixIID.LENGTH, VertexIID.Type.LENGTH).decodeString(STRING_ENCODING) + "]";
                    String infix = "[" + (Index.Infix.LENGTH) + ": " + Index.Infix.of(
                            bytes.view(PrefixIID.LENGTH + VertexIID.Type.LENGTH, Index.Infix.LENGTH)) + "]";
                    String rule = "[" + (StructureIID.Rule.LENGTH) + ": " + bytes.view(PrefixIID.LENGTH +
                            VertexIID.Type.LENGTH + Index.Infix.LENGTH) + "]";
                    String partition = "[partition: " + partition() + "]";
                    readableString = prefix + typeIID + infix + rule + partition;
                }
                return readableString;
            }
        }
    }

    public static class Rule extends IndexIID {

        Rule(ByteArray bytes) {
            super(bytes);
        }

        /**
         * Returns the index address of given {@code RuleStructure}
         *
         * @param label of the {@code RuleStructure}
         * @return a byte array representing the index address of a {@code RuleStructure}
         */
        public static Rule of(String label) {
            return new Rule(join(Index.Prefix.RULE.prefix().bytes(), encodeString(label, STRING_ENCODING)));
        }

        public static Key.Prefix<Rule> prefix() {
            return new Key.Prefix<>(Index.Prefix.RULE.prefix().bytes(), Partition.DEFAULT, Rule::new);
        }

        @Override
        public String toString() {
            if (readableString == null) {
                readableString = "[" + PrefixIID.LENGTH + ": " + Index.Prefix.RULE.toString() + "]" +
                        "[" + (bytes.length() - PrefixIID.LENGTH) +
                        ": " + bytes.view(PrefixIID.LENGTH).decodeString(STRING_ENCODING) + "]" +
                        "[partition: " + partition() + "]";
            }
            return readableString;
        }
    }
}
