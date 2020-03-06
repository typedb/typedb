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
 */

package grakn.core.graph.diskstorage;

/**
 * Represents a transaction for a particular storage backend.
 *
 */

public interface BaseTransaction {

    /**
     * Commits the transaction and persists all modifications to the backend.
     * 
     * Call either this method or #rollback() at most once per instance.
     */
    void commit() throws BackendException;

    /**
     * Aborts (or rolls back) the transaction.
     *
     * Call either this method or #commit() at most once per instance.
     *
     */
    void rollback() throws BackendException;

}
