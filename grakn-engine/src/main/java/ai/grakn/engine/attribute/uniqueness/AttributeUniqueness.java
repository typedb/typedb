/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package ai.grakn.engine.attribute.uniqueness;

import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.engine.attribute.uniqueness.queue.Attribute;
import ai.grakn.engine.attribute.uniqueness.queue.Attributes;
import ai.grakn.engine.attribute.uniqueness.queue.InMemoryQueue;
import ai.grakn.engine.attribute.uniqueness.queue.Queue;
import ai.grakn.engine.attribute.uniqueness.queue.RocksDbQueue;
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
public class AttributeUniqueness {
    private static Logger LOG = LoggerFactory.getLogger(AttributeUniqueness.class);

    public static AttributeUniqueness singleton = create();

    public static final int QUEUE_GET_BATCH_MAX = 1;

    private Queue newAttributeQueue = new RocksDbQueue();
    private MergeAlgorithm mergeAlgorithm = new MergeAlgorithm();
    private boolean stopDaemon = false;

    private static AttributeUniqueness create() {
        AttributeUniqueness singleton = new AttributeUniqueness();
        singleton.startDaemon(); // TODO: enable
        return singleton;
    }

    public void insertAttribute(Keyspace keyspace, String value, ConceptId conceptId) {
        final Attribute newAttribute = Attribute.create(keyspace, value, conceptId);
        LOG.info("insertAttribute(" + newAttribute + ")");
        newAttributeQueue.insertAttribute(newAttribute);
    }

    public void mergeNow(int max) {
        try {
            Attributes newAttrs = newAttributeQueue.readAttributes(max);
            mergeAlgorithm.merge(newAttrs);
            newAttributeQueue.ackAttributes(newAttrs);
        }
        catch (InterruptedException e) {
            LOG.error("mergeNow() failed with an exception. ", e);
        }
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
                mergeNow(QUEUE_GET_BATCH_MAX);
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

    /**
     * Stops the attribute merger daemon
     */
    public void stopDaemon() {
        stopDaemon = true;
    }
}

