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
import com.vaticle.typedb.core.encoding.key.Key;
import com.vaticle.typedb.core.encoding.key.KeyGenerator;

import static com.vaticle.typedb.core.common.collection.ByteArray.join;

public abstract class StructureIID extends PartitionedIID {

    public static final int LENGTH = PrefixIID.LENGTH + 2;

    private static final Partition PARTITION = Partition.DEFAULT;

    private StructureIID(ByteArray bytes) {
        super(bytes);
        assert bytes.length() == LENGTH;
    }

    @Override
    public Partition partition() {
        return PARTITION;
    }

    public static class Rule extends StructureIID {

        private Rule(ByteArray bytes) {
            super(bytes);
        }

        public static Rule of(ByteArray bytes) {
            return new Rule(bytes);
        }

        public static Rule extract(ByteArray bytes, int from) {
            return new Rule(bytes.view(from, from + LENGTH));
        }

        public static Key.Prefix<Rule> prefix() {
            return new Key.Prefix<>(Encoding.Structure.RULE.prefix().bytes(), PARTITION, Rule::of);
        }

        /**
         * Generate an IID for a {@code RuleStructure} for a given {@code Encoding}
         *
         * @param keyGenerator to generate the IID for a {@code RuleStructure}
         * @return a byte array representing a new IID for a {@code RuleStructure}
         */
        public static Rule generate(KeyGenerator.Schema keyGenerator) {
            return of(join(Encoding.Structure.RULE.prefix().bytes(), keyGenerator.forRule()));
        }

        public Encoding.Structure.Rule encoding() {
            return Encoding.Structure.RULE;
        }

        @Override
        public String toString() {
            if (readableString == null) {
                readableString = "[" + PrefixIID.LENGTH + ": " + encoding().toString() + "][" +
                        (LENGTH - PrefixIID.LENGTH) + ": " +
                        bytes.view(PrefixIID.LENGTH, LENGTH).decodeSortedAsShort() + "]" +
                        "[partition: " + partition() + "]";
            }
            return readableString;
        }
    }
}
