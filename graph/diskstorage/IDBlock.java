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
 * Represents a block of ids. #numIds() return how many ids are in this block and #getId(long) retrieves
 * the id at the given position, where the position must be smaller than the number of ids in this block (similar to array access).
 * <p>
 * Any IDBlock implementation must be completely thread-safe.
 *
 */
public interface IDBlock {

    /**
     * Number of ids in this block.
     *
     * @return
     */
    long numIds();

    /**
     * Returns the id at the given index. Index must be non-negative and smaller than #numIds().
     *
     * @param index
     * @return
     */
    long getId(long index);

}
