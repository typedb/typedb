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
     * A system to synchronise a commit.
     */
    interface CommitSync {

        /**
         * Returns the current sync status and sets it to true.
         *
         * This is an atomic operation that gets the sync status and sets it to
         * true in one atomic step. If the returned value is 'false' that means
         * the commit is not yet synced, and the caller of this method needs to
         * commit the attribute in which this {@code CommitSync} represented.
         *
         * @return the current sync status before setting it to true
         */
        boolean checkIsSyncedAndSetTrue();
    }
}
