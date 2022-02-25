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

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.graph.common.Storage.Key;
import com.vaticle.typedb.core.graph.iid.VertexIID;

import static com.vaticle.typedb.core.common.collection.ByteArray.join;

public class StatisticsKey implements Key {

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

    public static StatisticsKey vertexCount(VertexIID.Type typeIID) {
        return new StatisticsKey(join(
                Encoding.Metadata.Statistics.Prefix.VERTEX_COUNT.bytes(),
                typeIID.bytes()
        ));
    }

    public static StatisticsKey vertexTransitiveCount(VertexIID.Type typeIID) {
        return new StatisticsKey(join(
                Encoding.Metadata.Statistics.Prefix.VERTEX_COUNT_TRANSITIVE.bytes(),
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

    public static StatisticsKey hasEdgeTotalCount(VertexIID.Type thingTypeIID) {
        return new StatisticsKey(join(
                Encoding.Metadata.Statistics.Prefix.HAS_EDGE_COUNT.bytes(),
                thingTypeIID.bytes()
        ));
    }

    public static StatisticsKey snapshot() {
        return new StatisticsKey(Encoding.Metadata.Statistics.Prefix.VERSION.bytes());
    }

    public static class MisCountKey extends StatisticsKey {

        private MisCountKey(ByteArray bytes) {
            super(bytes);
        }

        private static MisCountKey of(ByteArray bytes) {
            assert bytes.hasPrefix(Encoding.Metadata.Statistics.Prefix.MISCOUNT.bytes());
            return new MisCountKey(bytes);
        }

        public static Key.Prefix<MisCountKey> prefix() {
            return new Key.Prefix<>(Encoding.Metadata.Statistics.Prefix.MISCOUNT.bytes(), Partition.STATISTICS, MisCountKey::of);
        }

        public static MisCountKey attConditionalOvercount(long txnID, VertexIID.Attribute<?> attIID) {
            return new MisCountKey(join(
                    Encoding.Metadata.Statistics.Prefix.MISCOUNT.bytes(),
                    ByteArray.encodeLong(txnID),
                    Encoding.Metadata.Statistics.Infix.CONDITIONAL_OVERCOUNT_ATTRIBUTE.bytes(),
                    attIID.bytes()
            ));
        }

        public static MisCountKey attConditionalUndercount(long txnID, VertexIID.Attribute<?> attIID) {
            return new MisCountKey(join(
                    Encoding.Metadata.Statistics.Prefix.MISCOUNT.bytes(),
                    ByteArray.encodeLong(txnID),
                    Encoding.Metadata.Statistics.Infix.CONDITIONAL_UNDERCOUNT_ATTRIBUTE.bytes(),
                    attIID.bytes()
            ));
        }

        public static MisCountKey hasConditionalOvercount(long txnID, VertexIID.Thing thingIID, VertexIID.Attribute<?> attIID) {
            return new MisCountKey(join(
                    Encoding.Metadata.Statistics.Prefix.MISCOUNT.bytes(),
                    ByteArray.encodeLong(txnID),
                    Encoding.Metadata.Statistics.Infix.CONDITIONAL_OVERCOUNT_HAS.bytes(),
                    thingIID.bytes(),
                    attIID.bytes()
            ));
        }

        public static MisCountKey hasConditionalUndercount(long txnID, VertexIID.Thing thingIID, VertexIID.Attribute<?> attIID) {
            return new MisCountKey(join(
                    Encoding.Metadata.Statistics.Prefix.MISCOUNT.bytes(),
                    ByteArray.encodeLong(txnID),
                    Encoding.Metadata.Statistics.Infix.CONDITIONAL_UNDERCOUNT_HAS.bytes(),
                    thingIID.bytes(),
                    attIID.bytes()
            ));
        }
    }

//    public static class CountJobKey extends StatisticsKey {
//
//        private CountJobKey(ByteArray key) {
//            super(key);
//        }
//
//        public static CountJobKey of(ByteArray bytes) {
//            assert bytes.get(0) == Encoding.Prefix.STATISTICS_COUNT_JOB.bytes().get(0) && (
//                    bytes.get(1) == Encoding.Statistics.JobType.ATTRIBUTE_VERTEX.bytes().get(0) ||
//                            bytes.get(1) == Encoding.Statistics.JobType.HAS_EDGE.bytes().get(0)
//            );
//            return new CountJobKey(bytes);
//        }
//
//        public static Key.Prefix<CountJobKey> prefix() {
//            return new Key.Prefix<>(Encoding.Prefix.STATISTICS_COUNT_JOB.bytes(), Partition.STATISTICS, CountJobKey::of);
//        }
//
//        public static StatisticsKey attribute(VertexIID.Attribute<?> attIID) {
//            return new StatisticsKey(join(
//                    Encoding.Prefix.STATISTICS_COUNT_JOB.bytes(),
//                    Encoding.Statistics.JobType.ATTRIBUTE_VERTEX.bytes(),
//                    attIID.bytes()
//            ));
//        }
//
//        public static StatisticsKey hasEdge(VertexIID.Thing thingIID, VertexIID.Attribute<?> attIID) {
//            return new StatisticsKey(join(
//                    Encoding.Prefix.STATISTICS_COUNT_JOB.bytes(),
//                    Encoding.Statistics.JobType.HAS_EDGE.bytes(),
//                    thingIID.bytes(),
//                    attIID.bytes()
//            ));
//        }
//    }
}
