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
    void insertAttribute(Attribute attribute);

    /**
     * get n attributes where min <= n <= max. For fault tolerance, attributes are not deleted from the queue until Attributes::markProcessed() is called.
     *
     * @param min minimum number of items to be returned. the method will block until it is reached.
     * @param max the maximum number of items to be returned.
     * @param timeLimit specifies the maximum waiting time where the method will immediately return the items it has if larger than what is specified in the min param.
     * @return an {@link Attributes} instance containing a list of duplicates
     */
    Attributes readAttributes(int min, int max, long timeLimit);

    void ackAttributes(Attributes batch);

    int size();
}
