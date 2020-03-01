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

package grakn.core.graph.core.log;

/**
 * Identifies the type of change has undergone.
 * <p>
 * #ADDED applies to elements that have been added to the graph, #REMOVED is for removed elements, and
 * #ANY is used to retrieve all elements that have undergone change.
 * <p>
 * #ADDED and #REMOVED are considered proper change states.
 *
 */
public enum Change {

    ADDED, REMOVED, ANY;

    public boolean isProper() {
        switch(this) {
            case ADDED:
            case REMOVED:
                return true;
            default:
                return false;
        }
    }

}
