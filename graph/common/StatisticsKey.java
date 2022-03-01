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
import com.vaticle.typedb.core.common.collection.Bytes;
import com.vaticle.typedb.core.graph.iid.VertexIID;

import static com.vaticle.typedb.core.common.collection.ByteArray.join;

public class StatisticsKey implements Storage.Key {

    private static final Partition PARTITION = Partition.STATISTICS;

    private final ByteArray bytes;

    private StatisticsKey(ByteArray bytes) {
        this.bytes = bytes;
    }

    public ByteArray bytes() {
        return bytes;
    }

    @Override
    public Partition partition() {
        return PARTITION;
    }

    public static StatisticsKey txnCommitted(long txnID) {
        return new StatisticsKey(join(
                Encoding.Metadata.Statistics.Prefix.TX_COMMITTED_ID.bytes(),
                ByteArray.encodeLong(txnID)
        ));
    }

    public static StatisticsKey vertexCount(VertexIID.Type typeIID) {
        return new StatisticsKey(join(
                Encoding.Metadata.Statistics.Prefix.VERTEX_COUNT.bytes(),
                typeIID.bytes()
        ));
    }

    public static StatisticsKey hasEdgeCount(VertexIID.Type thingTypeIID, VertexIID.Type attTypeIID) {
        return new StatisticsKey(join(
                Encoding.Metadata.Statistics.Prefix.HAS_TYPE_EDGE_COUNT.bytes(),
                thingTypeIID.bytes(),
                attTypeIID.bytes()
        ));
    }

    public static StatisticsKey version() {
        return new StatisticsKey(Encoding.Metadata.Statistics.Prefix.VERSION.bytes());
    }

    public static class MisCount extends StatisticsKey {

        private MisCount(ByteArray bytes) {
            super(bytes);
        }

        private static MisCount of(ByteArray bytes) {
            assert bytes.hasPrefix(Encoding.Metadata.Statistics.Prefix.MISCOUNT.bytes());
            return new MisCount(bytes);
        }

        public static Prefix<MisCount> prefix() {
            return new Prefix<>(Encoding.Metadata.Statistics.Prefix.MISCOUNT.bytes(), Partition.STATISTICS, MisCount::of);
        }

        private byte infix() {
            return bytes().get(Encoding.Metadata.Statistics.Prefix.LENGTH + Bytes.LONG_SIZE);
        }

        public boolean isAttrConditionalOvercount() {
            return infix() == Encoding.Metadata.Statistics.Infix.CONDITIONAL_OVERCOUNT_ATTRIBUTE.key();
        }

        public boolean isAttrConditionalUndercount() {
            return infix() ==
                    Encoding.Metadata.Statistics.Infix.CONDITIONAL_UNDERCOUNT_ATTRIBUTE.key();
        }

        public boolean isHasConditionalOvercount() {
            return infix() == Encoding.Metadata.Statistics.Infix.CONDITIONAL_OVERCOUNT_HAS.key();
        }

        public boolean isHasConditionalUndercount() {
            return infix() == Encoding.Metadata.Statistics.Infix.CONDITIONAL_UNDERCOUNT_HAS.key();
        }

        public VertexIID.Attribute<?> attributeMiscounted() {
            assert isAttrConditionalOvercount() || isAttrConditionalUndercount();
            return VertexIID.Attribute.extract(
                    this.bytes(),
                    Encoding.Metadata.Statistics.Prefix.LENGTH + Bytes.LONG_SIZE + Encoding.Metadata.Statistics.Infix.LENGTH
            );
        }

        public Pair<VertexIID.Thing, VertexIID.Attribute<?>> hasMiscounted() {
            assert isHasConditionalOvercount() || isHasConditionalUndercount();
            VertexIID.Thing owner = VertexIID.Thing.extract(
                    this.bytes(),
                    Encoding.Metadata.Statistics.Prefix.LENGTH + Bytes.LONG_SIZE + Encoding.Metadata.Statistics.Infix.LENGTH
            );
            return new Pair<>(
                    owner,
                    VertexIID.Attribute.extract(
                            this.bytes(),
                            Encoding.Metadata.Statistics.Prefix.LENGTH + Bytes.LONG_SIZE + Encoding.Metadata.Statistics.Infix.LENGTH + owner.bytes().length()
                    )
            );
        }

        public static MisCount attrConditionalOvercount(long txnID, VertexIID.Attribute<?> attIID) {
            return new MisCount(join(
                    Encoding.Metadata.Statistics.Prefix.MISCOUNT.bytes(),
                    ByteArray.encodeLong(txnID),
                    Encoding.Metadata.Statistics.Infix.CONDITIONAL_OVERCOUNT_ATTRIBUTE.bytes(),
                    attIID.bytes()
            ));
        }

        public static MisCount attrConditionalUndercount(long txnID, VertexIID.Attribute<?> attIID) {
            return new MisCount(join(
                    Encoding.Metadata.Statistics.Prefix.MISCOUNT.bytes(),
                    ByteArray.encodeLong(txnID),
                    Encoding.Metadata.Statistics.Infix.CONDITIONAL_UNDERCOUNT_ATTRIBUTE.bytes(),
                    attIID.bytes()
            ));
        }

        public static MisCount hasConditionalOvercount(long txnID, VertexIID.Thing thingIID, VertexIID.Attribute<?> attIID) {
            return new MisCount(join(
                    Encoding.Metadata.Statistics.Prefix.MISCOUNT.bytes(),
                    ByteArray.encodeLong(txnID),
                    Encoding.Metadata.Statistics.Infix.CONDITIONAL_OVERCOUNT_HAS.bytes(),
                    thingIID.bytes(),
                    attIID.bytes()
            ));
        }

        public static MisCount hasConditionalUndercount(long txnID, VertexIID.Thing thingIID, VertexIID.Attribute<?> attIID) {
            return new MisCount(join(
                    Encoding.Metadata.Statistics.Prefix.MISCOUNT.bytes(),
                    ByteArray.encodeLong(txnID),
                    Encoding.Metadata.Statistics.Infix.CONDITIONAL_UNDERCOUNT_HAS.bytes(),
                    thingIID.bytes(),
                    attIID.bytes()
            ));
        }
    }
}
