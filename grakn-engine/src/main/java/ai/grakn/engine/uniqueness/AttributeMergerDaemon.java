/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.uniqueness;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.util.Schema;
import com.google.auto.value.AutoValue;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * This class is responsible for merging attribute duplicates which is done in order to maintain attribute uniqueness.
 * It is an always-on background thread which continuously merge duplicates. This means that eventually,
 * every attribute instance will have a unique value, as the duplicates will have been removed.
 * A new incoming attribute will immediately trigger the merge operation, meaning that duplicates are merged in almost real-time speed.
 * It is fault-tolerant and will re-process incoming attributes if Grakn crashes during a merge process.
 *
 * @author Ganeshwara Herawan Hananda
 */
public class AttributeMergerDaemon {
    private static Logger LOG = LoggerFactory.getLogger(AttributeMergerDaemon.class);

    public static AttributeMergerDaemon singleton = create();

    public static final int QUEUE_GET_BATCH_MIN = 1;
    public static final int QUEUE_GET_BATCH_MAX = 1;
    public static final int QUEUE_GET_BATCH_WAIT_TIME_LIMIT_MS = 1000;

    private Queue newAttributeQueue = new Queue();
    private MergeAlgorithm mergeAlgorithm = new MergeAlgorithm();
    private boolean stopDaemon = false;

    private static AttributeMergerDaemon create() {
        AttributeMergerDaemon singleton = new AttributeMergerDaemon();
//        singleton.startDaemon(); // TODO: enable
        return singleton;
    }

    /**
     * Stops the attribute merger daemon
     */
    public void stopDaemon() {
        stopDaemon = true;
    }

    /**
     * Starts the attribute merger daemon, which continuously merge attribute duplicates.
     * The thread listens to the {@link Queue} queue for incoming attributes and applies
     * the merge algorithm as implemented in {@link MergeAlgorithm}.
     *
     */
    private CompletableFuture<Void> startDaemon() {
        CompletableFuture<Void> daemon = CompletableFuture.supplyAsync(() -> {
            LOG.info("startDaemon() - start");
            while (!stopDaemon) {
                merge(QUEUE_GET_BATCH_MIN, QUEUE_GET_BATCH_MAX, QUEUE_GET_BATCH_WAIT_TIME_LIMIT_MS);
            }
            LOG.info("startDaemon() - stop");
            return null;
        });

        daemon.exceptionally(e -> {
            LOG.error("An exception has occurred in the AttributeMergerDaemon. ", e);
            return null;
        });

        return daemon;
    }

    public void merge(int min, int max, int waitTimeLimitMs) {
        try {
            Attributes newAttrs = newAttributeQueue.takeBatch(min, max, waitTimeLimitMs);
            LOG.info("starting a new batch to process these new attributes: " + newAttrs);
            Map<KeyspaceAndValue, List<Attribute>> groupByKeyspaceAndValue = newAttrs.attributes().stream()
                    .collect(Collectors.groupingBy(attr -> KeyspaceAndValue.create(attr.keyspace(), attr.value())));
            groupByKeyspaceAndValue.forEach((groupName, group) -> LOG.info("startDaemon() - group: " + groupName + " = " + group));
            groupByKeyspaceAndValue.forEach((keyspaceAndValue, attrValue) -> {
                try (EmbeddedGraknSession s  = EmbeddedGraknSession.create(keyspaceAndValue.keyspace(), "localhost:4567");
                     EmbeddedGraknTx tx = s.transaction(GraknTxType.WRITE)) {
                    mergeAlgorithm.merge(tx, attrValue);
                    tx.commitSubmitNoLogs();
                }
            });
//                    newAttrs.markProcessed(); // TODO: enable after takeBatch is changed to processBatch()
            LOG.info("new attributes processed.");
        } catch (RuntimeException e) {
            LOG.error("An exception has occurred in the AttributeMergerDaemon. ", e);
        }
    }

    public void add(Keyspace keyspace, String value, ConceptId conceptId) {
        final Attribute newAttribute = Attribute.create(keyspace, value, conceptId);
        LOG.info("add(" + newAttribute + ")");
        newAttributeQueue.add(newAttribute);
    }
}

