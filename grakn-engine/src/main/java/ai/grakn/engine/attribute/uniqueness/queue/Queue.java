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

package ai.grakn.engine.attribute.uniqueness.queue;

/**
 * TODO
 * @author Ganeshwara Herawan Hananda
 */
public interface Queue {
    /**
     * Enqueue a new attribute to the queue
     * @param attribute
     */
    void insert(Attribute attribute);

    /**
     * get n attributes where min <= n < limit. For fault tolerance, attributes are not deleted from the queue until Attributes::markProcessed() is called.
     *
     * @param limit the maximum number of items to be returned.
     * @return an {@link Attributes} instance containing a list of duplicates
     */
    Attributes read(int limit) throws InterruptedException;

    void ack(Attributes batch);
}
