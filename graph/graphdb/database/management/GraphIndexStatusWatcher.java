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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import grakn.core.graph.core.JanusGraph;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.schema.JanusGraphIndex;
import grakn.core.graph.core.schema.JanusGraphManagement;
import grakn.core.graph.core.schema.SchemaStatus;
import grakn.core.graph.diskstorage.util.time.Timer;
import grakn.core.graph.diskstorage.util.time.TimestampProviders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class GraphIndexStatusWatcher
        extends AbstractIndexStatusWatcher<GraphIndexStatusReport, GraphIndexStatusWatcher> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphIndexStatusWatcher.class);

    private final String graphIndexName;

    public GraphIndexStatusWatcher(JanusGraph g, String graphIndexName) {
        super(g);
        this.graphIndexName = graphIndexName;
    }

    @Override
    protected GraphIndexStatusWatcher self() {
        return this;
    }

    @Override
    public GraphIndexStatusReport call() throws InterruptedException {
        Preconditions.checkNotNull(g, "Graph instance must not be null");
        Preconditions.checkNotNull(graphIndexName, "Index name must not be null");
        Preconditions.checkNotNull(statuses, "Target statuses must not be null");
        Preconditions.checkArgument(statuses.size() > 0, "Target statuses must include at least one status");

        Map<String, SchemaStatus> notConverged = new HashMap<>();
        Map<String, SchemaStatus> converged = new HashMap<>();
        JanusGraphIndex idx;

        Timer t = new Timer(TimestampProviders.MILLI).start();
        boolean timedOut;
        while (true) {
            JanusGraphManagement management = null;
            try {
                management = g.openManagement();
                idx = management.getGraphIndex(graphIndexName);
                for (PropertyKey pk : idx.getFieldKeys()) {
                    SchemaStatus s = idx.getIndexStatus(pk);
                    LOGGER.debug("Key {} has status {}", pk, s);
                    if (!statuses.contains(s))
                        notConverged.put(pk.toString(), s);
                    else
                        converged.put(pk.toString(), s);
                }
            } finally {
                if (null != management)
                    management.rollback(); // Let an exception here propagate up the stack
            }

            String waitingOn = Joiner.on(",").withKeyValueSeparator("=").join(notConverged);
            if (!notConverged.isEmpty()) {
                LOGGER.info("Some key(s) on index {} do not currently have status(es) {}: {}", graphIndexName, statuses, waitingOn);
            } else {
                LOGGER.info("All {} key(s) on index {} have status(es) {}", converged.size(), graphIndexName, statuses);
                return new GraphIndexStatusReport(true, graphIndexName, statuses, notConverged, converged, t.elapsed());
            }

            timedOut = null != timeout && 0 < t.elapsed().compareTo(timeout);

            if (timedOut) {
                LOGGER.info("Timed out ({}) while waiting for index {} to converge on status(es) {}",
                        timeout, graphIndexName, statuses);
                return new GraphIndexStatusReport(false, graphIndexName, statuses, notConverged, converged, t.elapsed());
            }
            notConverged.clear();
            converged.clear();

            Thread.sleep(poll.toMillis());
        }
    }
}

