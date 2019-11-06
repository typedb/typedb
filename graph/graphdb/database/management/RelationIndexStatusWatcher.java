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

package grakn.core.graph.graphdb.database.management;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.JanusGraph;
import grakn.core.graph.core.schema.JanusGraphManagement;
import grakn.core.graph.core.schema.RelationTypeIndex;
import grakn.core.graph.core.schema.SchemaStatus;
import grakn.core.graph.diskstorage.util.time.Timer;
import grakn.core.graph.diskstorage.util.time.TimestampProviders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelationIndexStatusWatcher
        extends AbstractIndexStatusWatcher<RelationIndexStatusReport, RelationIndexStatusWatcher> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RelationIndexStatusWatcher.class);

    private final String relationIndexName;
    private final String relationTypeName;

    public RelationIndexStatusWatcher(JanusGraph g, String relationIndexName, String relationTypeName) {
        super(g);
        this.relationIndexName = relationIndexName;
        this.relationTypeName = relationTypeName;
    }

    @Override
    protected RelationIndexStatusWatcher self() {
        return this;
    }

    /**
     * Poll a relation index until it has a certain {@link SchemaStatus},
     * or until a configurable timeout is exceeded.
     *
     * @return a report with information about schema state, execution duration, and the index
     */
    @Override
    public RelationIndexStatusReport call() throws InterruptedException {
        Preconditions.checkNotNull(g, "Graph instance must not be null");
        Preconditions.checkNotNull(relationIndexName, "Index name must not be null");
        Preconditions.checkNotNull(statuses, "Target statuses must not be null");
        Preconditions.checkArgument(statuses.size() > 0, "Target statuses must include at least one status");

        RelationTypeIndex idx;

        Timer t = new Timer(TimestampProviders.MILLI).start();
        boolean timedOut;
        while (true) {
            SchemaStatus actualStatus;
            JanusGraphManagement management = null;
            try {
                management = g.openManagement();
                idx = management.getRelationIndex(management.getRelationType(relationTypeName), relationIndexName);
                actualStatus = idx.getIndexStatus();
                LOGGER.info("Index {} (relation type {}) has status {}", relationIndexName, relationTypeName, actualStatus);
                if (statuses.contains(actualStatus)) {
                    return new RelationIndexStatusReport(true, relationIndexName, relationTypeName, actualStatus, statuses, t.elapsed());
                }
            } finally {
                if (null != management) {
                    management.rollback(); // Let an exception here propagate up the stack
                }
            }

            timedOut = null != timeout && 0 < t.elapsed().compareTo(timeout);

            if (timedOut) {
                LOGGER.info("Timed out ({}) while waiting for index {} (relation type {}) to reach status(es) {}",
                        timeout, relationIndexName, relationTypeName, statuses);
                return new RelationIndexStatusReport(false, relationIndexName, relationTypeName, actualStatus, statuses, t.elapsed());
            }

            Thread.sleep(poll.toMillis());
        }
    }

}
