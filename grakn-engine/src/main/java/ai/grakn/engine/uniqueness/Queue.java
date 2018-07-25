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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * TODO
 * @author Ganeshwara Herawan Hananda
 */
public class Queue {
    private static Logger LOG = LoggerFactory.getLogger(Queue.class);
    private java.util.Queue<Attribute> newAttributeQueue = new LinkedBlockingQueue<>();

    /**
     * Enqueue a new attribute to the queue
     * @param attribute
     */
    public void add(Attribute attribute) {
        newAttributeQueue.add(attribute);
    }

    // TODO: change to read
    /**
     * get n attributes where min <= n <= max. For fault tolerance, attributes are not deleted from the queue until Attributes::markProcessed() is called.
     *
     * @param min minimum number of items to be returned. the method will block until it is reached.
     * @param max the maximum number of items to be returned.
     * @param timeLimit specifies the maximum waiting time where the method will immediately return the items it has if larger than what is specified in the min param.
     * @return an {@link Attributes} instance containing a list of duplicates
     */
    Attributes takeBatch(int min, int max, long timeLimit) {
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        List<Attribute> batch = new LinkedList<>();

        for (int i = 0; i < max && batch.size() <= max; ++i) {
            Attribute e = newAttributeQueue.poll();
            if (e != null) batch.add(e);
        }

        return new Attributes(batch);
    }

    // TODO
    void markRead(Attributes batch) {
    }
}
