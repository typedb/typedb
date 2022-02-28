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
import com.vaticle.typedb.core.common.collection.Bytes;
import com.vaticle.typedb.core.graph.iid.VertexIID;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.vaticle.typedb.core.common.collection.ByteArray.join;

public interface StatisticsKeyValue {

    class Key implements Storage.Key {

        private static final Partition PARTITION = Partition.STATISTICS;

        private final ByteArray bytes;

        private Key(ByteArray bytes) {
            this.bytes = bytes;
        }

        public ByteArray bytes() {
            return bytes;
        }

        @Override
        public Partition partition() {
            return PARTITION;
        }

        public static Key vertexCount(VertexIID.Type typeIID) {
            return new Key(join(
                    Encoding.Metadata.Statistics.Prefix.VERTEX_COUNT.bytes(),
                    typeIID.bytes()
            ));
        }

        public static Key vertexTransitiveCount(VertexIID.Type typeIID) {
            return new Key(join(
                    Encoding.Metadata.Statistics.Prefix.VERTEX_COUNT_TRANSITIVE.bytes(),
                    typeIID.bytes()
            ));
        }

        public static Key hasEdgeCount(VertexIID.Type thingTypeIID, VertexIID.Type attTypeIID) {
            return new Key(join(
                    Encoding.Metadata.Statistics.Prefix.HAS_TYPE_EDGE_COUNT.bytes(),
                    thingTypeIID.bytes(),
                    attTypeIID.bytes()
            ));
        }

        public static Key hasEdgeTotalCount(VertexIID.Type thingTypeIID) {
            return new Key(join(
                    Encoding.Metadata.Statistics.Prefix.HAS_EDGE_COUNT.bytes(),
                    thingTypeIID.bytes()
            ));
        }

        public static Key snapshot() {
            return new Key(Encoding.Metadata.Statistics.Prefix.VERSION.bytes());
        }

        public static class MisCount extends Key {

            private MisCount(ByteArray bytes) {
                super(bytes);
            }

            private static MisCount of(ByteArray bytes) {
                assert bytes.hasPrefix(Encoding.Metadata.Statistics.Prefix.MISCOUNT.bytes());
                return new MisCount(bytes);
            }

            public static Key.Prefix<MisCount> prefix() {
                return new Key.Prefix<>(Encoding.Metadata.Statistics.Prefix.MISCOUNT.bytes(), Partition.STATISTICS, MisCount::of);
            }

            public static MisCount attConditionalOvercount(long txnID, VertexIID.Attribute<?> attIID) {
                return new MisCount(join(
                        Encoding.Metadata.Statistics.Prefix.MISCOUNT.bytes(),
                        ByteArray.encodeLong(txnID),
                        Encoding.Metadata.Statistics.Infix.CONDITIONAL_OVERCOUNT_ATTRIBUTE.bytes(),
                        attIID.bytes()
                ));
            }

            public static MisCount attConditionalUndercount(long txnID, VertexIID.Attribute<?> attIID) {
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

    class Value {

        public static ByteArray encodeLongSet(Set<Long> longs) {
            ByteArray[] encoded = new ByteArray[longs.size()];
            Iterator<Long> iterator = longs.iterator();
            int i = 0;
            while (iterator.hasNext()) {
                encoded[i] = ByteArray.encodeLong(iterator.next());
                assert encoded[i].length() == Bytes.LONG_SIZE;
                i++;
            }
            return ByteArray.join(encoded);
        }

        public static Set<Long> decodeLongSet(ByteArray bytes) {
            assert bytes.length() % Bytes.LONG_SIZE == 0;
            Set<Long> longs = new HashSet<>();
            for (int i = 0; i < bytes.length(); i += Bytes.LONG_SIZE) {
                longs.add(bytes.view(i, i + Bytes.LONG_SIZE).decodeLong());
            }
            return longs;
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
