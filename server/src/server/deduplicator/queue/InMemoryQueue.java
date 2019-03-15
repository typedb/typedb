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

package grakn.core.server.deduplicator.queue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * In-memory queue implemented using a ConcurrentHashMap
 * It supports three operations: insert, read, and ack.
 * <p>
 * The read and ack should be used together in order to provide fault-tolerance.
 * The attribute de-duplicator can read attributes from the queue and only ack after everything has been processed.
 */
public class InMemoryQueue {
    private final Map<String, Attribute> queue;

    /**
     * Instantiates the class
     *
     */
    public InMemoryQueue() {
        queue = new ConcurrentHashMap<>();
    }

    /**
     * insert a new attribute at the end of the queue.
     *
     * @param attribute the attribute to be inserted
     */
    public void insert(Attribute attribute) {
        queue.put(attribute.conceptId().getValue(), attribute);
        synchronized (this) {
            notifyAll();
        }
    }

    /**
     * Read at most N attributes from the beginning of the queue. Read everything if there are less than N attributes in the queue.
     * If the queue is empty, the method will block until the queue receives a new attribute.
     * The attributes won't be removed from the queue until you call {@link #ack(List<Attribute>)} on the returned attributes.
     *
     * @param limit the maximum number of items to be returned.
     * @return a list of {@link Attribute}
     * @throws InterruptedException
     * @see #ack(List<Attribute>)
     */
    public List<Attribute> read(int limit) throws InterruptedException {
        // blocks until the queue contains at least 1 element
        while (queue.isEmpty()) {
            synchronized (this) {
                wait();
            }
        }

        return queue.values().stream().limit(limit).collect(Collectors.toList());
    }

    /**
     * Remove attributes from the queue.
     *
     * @param attributes the attributes which will be removed
     */
    public void ack(List<Attribute> attributes) {
        for (Attribute attr : attributes) {
            queue.remove(attr.conceptId().getValue());
        }
    }

}