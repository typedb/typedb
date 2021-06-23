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
 */

package com.vaticle.typedb.core.graph.iid;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.collection.Bytes;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.common.KeyGenerator;

import static com.vaticle.typedb.core.common.collection.ByteArray.join;

public abstract class StructureIID<T extends StructureIID<T>> extends IID {

    StructureIID(ByteArray bytes) {
        super(bytes);
    }

    abstract Encoding.Structure encoding();

    @Override
    public String toString() {
        return null;
    }

    public static class Rule extends StructureIID<Rule> {

        public static final int LENGTH = PrefixIID.LENGTH + 2;

        Rule(ByteArray bytes) {
            super(bytes);
            assert bytes.length() == LENGTH;
        }

        public static Rule of(ByteArray bytes) {
            return new Rule(bytes);
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

        @Override
        public Encoding.Structure.Rule encoding() {
            return Encoding.Structure.RULE;
        }

        @Override
        public String toString() {
            if (readableString == null) {
                readableString = "[" + PrefixIID.LENGTH + ": " + encoding().toString() + "][" +
                        (LENGTH - PrefixIID.LENGTH) + ": " +
                        bytes.view(PrefixIID.LENGTH, LENGTH).decodeSortedAsShort() + "]";
            }
            return readableString;
        }
    }
}
