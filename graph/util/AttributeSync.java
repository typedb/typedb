/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package hypergraph.graph.util;

/**
 * A system to coordinate (aka. "synchronise") the commits of Attribute vertices.
 *
 * Attributes are immutable, and also globally unique (by their type and value).
 * Thus, only one instance of the attribute needs to be committed in the database,
 * even when multiple copies are instantiated across different transactions.
 */
public interface AttributeSync {

    /**
     * Returns the {@code CommitSync} for a given attribute
     *
     * @param attributeIID of the attribute in which the commit we want to sync
     * @return the {@code CommitSync} for a given attribute
     */
    CommitSync get(IID.Vertex.Attribute attributeIID);

    /**
     * Removes the {@code CommitSync} for a given attribute.
     *
     * @param attributeIID of the attribute of the {@code CommitSync} to remove
     */
    void remove(IID.Vertex.Attribute attributeIID);

    /**
     * Acquires the lock to work with the {@code AttributeSync}.
     *
     * Only one transaction can work with the {@code AttributeSync} at a time.
     * Whichever transaction gets hold of the lock must release the lock by
     * calling {@code unlock()}.
     */
    void lock();

    /**
     * Releases the lock to work with the {@code AttributeSync}.
     *
     * Only one transaction can work with the {@code AttributeSync} at a time.
     * Whichever transaction gets hold of the lock by calling {@code lock()}
     * must release the lock by calling this method.
     */
    void unlock();

    /**
     * A system to synchronise a commit.
     */
    interface CommitSync {

        enum Status {
            NONE, WRITTEN, DELETED
        }

        Status status();

        void status(Status status);

        long snapshot();

        void snapshot(long number);
    }
}
