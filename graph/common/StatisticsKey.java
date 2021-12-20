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
                Encoding.Prefix.STATISTICS_THINGS.bytes(),
                typeIID.bytes(),
                Encoding.Statistics.Infix.VERTEX_COUNT.bytes()
        ));
    }

    public static StatisticsKey vertexTransitiveCount(VertexIID.Type typeIID) {
        return new StatisticsKey(join(
                Encoding.Prefix.STATISTICS_THINGS.bytes(),
                typeIID.bytes(),
                Encoding.Statistics.Infix.VERTEX_TRANSITIVE_COUNT.bytes()
        ));
    }

    public static StatisticsKey hasEdgeCount(VertexIID.Type thingTypeIID, VertexIID.Type attTypeIID) {
        return new StatisticsKey(join(
                Encoding.Prefix.STATISTICS_THINGS.bytes(),
                thingTypeIID.bytes(),
                Encoding.Statistics.Infix.HAS_EDGE_COUNT.bytes(),
                attTypeIID.bytes()
        ));
    }

    public static StatisticsKey hasEdgeTotalCount(VertexIID.Type thingTypeIID) {
        return new StatisticsKey(join(
                Encoding.Prefix.STATISTICS_THINGS.bytes(),
                thingTypeIID.bytes(),
                Encoding.Statistics.Infix.HAS_EDGE_TOTAL_COUNT.bytes()
        ));
    }


    public static StatisticsKey attributeCounted(VertexIID.Attribute<?> attIID) {
        return new StatisticsKey(join(Encoding.Prefix.STATISTICS_COUNTED.bytes(), attIID.bytes()));
    }

    public static StatisticsKey hasEdgeCounted(VertexIID.Thing thingIID, VertexIID.Attribute<?> attIID) {
        return new StatisticsKey(join(
                Encoding.Prefix.STATISTICS_COUNTED.bytes(),
                thingIID.bytes(),
                Encoding.Statistics.Infix.HAS_EDGE_COUNT.bytes(),
                attIID.bytes()
        ));
    }

    public static StatisticsKey snapshot() {
        return new StatisticsKey(Encoding.Prefix.STATISTICS_SNAPSHOT.bytes());
    }

    public static class CountJobKey extends StatisticsKey {

        private CountJobKey(ByteArray key) {
            super(key);
        }

        public static CountJobKey of(ByteArray bytes) {
            assert bytes.get(0) == Encoding.Prefix.STATISTICS_COUNT_JOB.bytes().get(0) && (
                    bytes.get(1) == Encoding.Statistics.JobType.ATTRIBUTE_VERTEX.bytes().get(0) ||
                            bytes.get(1) == Encoding.Statistics.JobType.HAS_EDGE.bytes().get(0)
            );
            return new CountJobKey(bytes);
        }

        public static Key.Prefix<CountJobKey> prefix() {
            return new Key.Prefix<>(Encoding.Prefix.STATISTICS_COUNT_JOB.bytes(), Partition.STATISTICS, CountJobKey::of);
        }

        public static StatisticsKey attribute(VertexIID.Attribute<?> attIID) {
            return new StatisticsKey(join(
                    Encoding.Prefix.STATISTICS_COUNT_JOB.bytes(),
                    Encoding.Statistics.JobType.ATTRIBUTE_VERTEX.bytes(),
                    attIID.bytes()
            ));
        }

        public static StatisticsKey hasEdge(VertexIID.Thing thingIID, VertexIID.Attribute<?> attIID) {
            return new StatisticsKey(join(
                    Encoding.Prefix.STATISTICS_COUNT_JOB.bytes(),
                    Encoding.Statistics.JobType.HAS_EDGE.bytes(),
                    thingIID.bytes(),
                    attIID.bytes()
            ));
        }
    }
}
