/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graph.core.util;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.JanusGraph;
import grakn.core.graph.core.JanusGraphException;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.schema.Index;
import grakn.core.graph.core.schema.JanusGraphIndex;
import grakn.core.graph.core.schema.JanusGraphManagement;
import grakn.core.graph.core.schema.RelationTypeIndex;
import grakn.core.graph.core.schema.SchemaAction;
import grakn.core.graph.diskstorage.util.time.TimestampProvider;
import grakn.core.graph.graphdb.database.StandardJanusGraph;
import org.apache.commons.lang3.StringUtils;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalUnit;

/**
 * This class is only used in tests!!!!!!!!
 */

public class ManagementUtil {

    /**
     * This method blocks and waits until the provided index has been updated across the entire JanusGraph cluster
     * and reached a stable state.
     * This method will wait for the given period of time and throw an exception if the index did not reach a
     * final state within that time. The method simply returns when the index has reached the final state
     * prior to the time period expiring.
     * <p>
     * This is a utility method to be invoked between two {@link JanusGraphManagement#updateIndex(Index, SchemaAction)} calls
     * to ensure that the previous update has successfully persisted.
     */
    public static void awaitGraphIndexUpdate(JanusGraph g, String indexName, long time, TemporalUnit unit) {
        awaitIndexUpdate(g, indexName, null, time, unit);
    }

    public static void awaitVertexIndexUpdate(JanusGraph g, String indexName, String relationTypeName, long time, TemporalUnit unit) {
        awaitIndexUpdate(g, indexName, relationTypeName, time, unit);
    }

    private static void awaitIndexUpdate(JanusGraph g, String indexName, String relationTypeName, long time, TemporalUnit unit) {
        Preconditions.checkArgument(g != null && g.isOpen(), "Need to provide valid, open graph instance");
        Preconditions.checkArgument(time > 0 && unit != null, "Need to provide valid time interval");
        Preconditions.checkArgument(StringUtils.isNotBlank(indexName), "Need to provide an index name");
        StandardJanusGraph graph = (StandardJanusGraph) g;
        TimestampProvider times = graph.getConfiguration().getTimestampProvider();
        Instant end = times.getTime().plus(Duration.of(time, unit));
        boolean isStable = false;
        while (times.getTime().isBefore(end)) {
            JanusGraphManagement management = graph.openManagement();
            try {
                if (StringUtils.isNotBlank(relationTypeName)) {
                    RelationTypeIndex idx = management.getRelationIndex(management.getRelationType(relationTypeName)
                            , indexName);
                    Preconditions.checkNotNull(idx, "Index could not be found: %s @ %s", indexName, relationTypeName);
                    isStable = idx.getIndexStatus().isStable();
                } else {
                    JanusGraphIndex idx = management.getGraphIndex(indexName);
                    Preconditions.checkNotNull(idx, "Index could not be found: %s", indexName);
                    isStable = true;
                    for (PropertyKey key : idx.getFieldKeys()) {
                        if (!idx.getIndexStatus(key).isStable()) isStable = false;
                    }
                }
            } finally {
                management.rollback();
            }
            if (isStable) {
                break;
            }
            try {
                times.sleepFor(Duration.ofMillis(500));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (!isStable) {
            throw new JanusGraphException("Index did not stabilize within the given amount of time. For sufficiently long " +
                    "wait periods this is most likely caused by a failed/incorrectly shut down JanusGraph instance or a lingering transaction.");
        }
    }

}
