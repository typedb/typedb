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

package ai.grakn.engine.attribute.uniqueness;

import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.engine.attribute.uniqueness.queue.Attribute;
import ai.grakn.engine.attribute.uniqueness.queue.Attributes;
import ai.grakn.engine.attribute.uniqueness.queue.InMemoryQueue;
import ai.grakn.engine.attribute.uniqueness.queue.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

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

    private Queue newAttributeQueue = new InMemoryQueue();
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
     * The thread listens to the {@link InMemoryQueue} queue for incoming attributes and applies
     * the merge algorithm as implemented in {@link MergeAlgorithm}.
     *
     */
    private CompletableFuture<Void> startDaemon() {
        CompletableFuture<Void> daemon = CompletableFuture.supplyAsync(() -> {
            LOG.info("startDaemon() - start");
            while (!stopDaemon) {
                mergeNow(QUEUE_GET_BATCH_MIN, QUEUE_GET_BATCH_MAX, QUEUE_GET_BATCH_WAIT_TIME_LIMIT_MS);
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

    public int mergeNow(int min, int max, int waitTimeLimitMs) {
        Attributes newAttrs = newAttributeQueue.readAttributes(min, max, waitTimeLimitMs);
        mergeAlgorithm.merge(newAttrs);
        return newAttributeQueue.size();
    }

    public void add(Keyspace keyspace, String value, ConceptId conceptId) {
        final Attribute newAttribute = Attribute.create(keyspace, value, conceptId);
        LOG.info("add(" + newAttribute + ")");
        newAttributeQueue.insertAttribute(newAttribute);
    }
}

