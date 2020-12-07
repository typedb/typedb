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
 */

package grakn.core.graph.iid;

import grakn.core.graph.util.Encoding;
import grakn.core.graph.util.KeyGenerator;

import static grakn.core.common.collection.Bytes.join;
import static grakn.core.common.collection.Bytes.sortedBytesToShort;
import static java.util.Arrays.copyOfRange;

public abstract class LogicIID extends IID {

    LogicIID(byte[] bytes) {
        super(bytes);
    }

    abstract Encoding.Logic encoding();

    @Override
    public String toString() {
        return null;
    }

    public static class Rule extends LogicIID {

        public static final int LENGTH = PrefixIID.LENGTH + 2;

        Rule(byte[] bytes) {
            super(bytes);
            assert bytes.length == LENGTH;
        }

        public static Rule of(byte[] bytes) {
            return new Rule(bytes);
        }

        /**
         * Generate an IID for a {@code RuleLogic} for a given {@code Encoding}
         *
         * @param keyGenerator to generate the IID for a {@code RuleLogic}
         * @return a byte array representing a new IID for a {@code RuleLogic}
         */
        public static Rule generate(KeyGenerator.Schema keyGenerator) {
            return of(join(Encoding.Logic.RULE.prefix().bytes(), keyGenerator.forRule()));
        }

        @Override
        public Encoding.Logic.Rule encoding() {
            return Encoding.Logic.RULE;
        }

        @Override
        public String toString() {
            if (readableString == null) {
                readableString = "[" + PrefixIID.LENGTH + ": " + encoding().toString() + "][" +
                        (LENGTH - PrefixIID.LENGTH) + ": " +
                        sortedBytesToShort(copyOfRange(bytes, PrefixIID.LENGTH, LENGTH)) + "]";
            }
            return readableString;
        }
    }
}
