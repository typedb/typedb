/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
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
