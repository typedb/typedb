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
 *
 */

package grakn.core.graph.util;

import grakn.core.graph.iid.VertexIID;

import static grakn.core.common.collection.Bytes.join;

public class StatisticsBytes {
    public static byte[] vertexCountKey(VertexIID.Type typeIID) {
        return join(
                Encoding.Prefix.STATISTICS_THINGS.bytes(),
                typeIID.bytes(),
                Encoding.Statistics.Infix.VERTEX_COUNT.bytes());
    }

    public static byte[] vertexTransitiveCountKey(VertexIID.Type typeIID) {
        return join(
                Encoding.Prefix.STATISTICS_THINGS.bytes(),
                typeIID.bytes(),
                Encoding.Statistics.Infix.VERTEX_TRANSITIVE_COUNT.bytes());
    }

    public static byte[] hasEdgeCountKey(VertexIID.Type thingTypeIID, VertexIID.Type attTypeIID) {
        return join(
                Encoding.Prefix.STATISTICS_THINGS.bytes(),
                thingTypeIID.bytes(),
                Encoding.Statistics.Infix.HAS_EDGE_COUNT.bytes(),
                attTypeIID.bytes());
    }

    public static byte[] hasEdgeTotalCountKey(VertexIID.Type thingTypeIID) {
        return join(
                Encoding.Prefix.STATISTICS_THINGS.bytes(),
                thingTypeIID.bytes(),
                Encoding.Statistics.Infix.HAS_EDGE_TOTAL_COUNT.bytes());
    }

    public static byte[] countJobKey() {
        return join(
                Encoding.Prefix.STATISTICS_COUNT_JOB.bytes());
    }

    public static byte[] attributeCountJobKey(VertexIID.Attribute<?> attIID) {
        return join(
                Encoding.Prefix.STATISTICS_COUNT_JOB.bytes(),
                Encoding.Statistics.JobType.ATTRIBUTE_VERTEX.bytes(),
                attIID.bytes());
    }

    public static byte[] attributeCountedKey(VertexIID.Attribute<?> attIID) {
        return join(
                Encoding.Prefix.STATISTICS_COUNTED.bytes(),
                attIID.bytes());
    }

    public static byte[] hasEdgeCountJobKey(VertexIID.Thing thingIID, VertexIID.Attribute<?> attIID) {
        return join(
                Encoding.Prefix.STATISTICS_COUNT_JOB.bytes(),
                Encoding.Statistics.JobType.HAS_EDGE.bytes(),
                thingIID.bytes(),
                attIID.bytes()
        );
    }

    public static byte[] hasEdgeCountedKey(VertexIID.Thing thingIID, VertexIID.Attribute<?> attIID) {
        return join(
                Encoding.Prefix.STATISTICS_COUNTED.bytes(),
                thingIID.bytes(),
                Encoding.Statistics.Infix.HAS_EDGE_COUNT.bytes(),
                attIID.bytes()
        );
    }

    public static byte[] snapshotKey() {
        return Encoding.Prefix.STATISTICS_SNAPSHOT.bytes();
    }
}
