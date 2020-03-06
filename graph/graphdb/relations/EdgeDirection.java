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

package grakn.core.graph.graphdb.relations;

import org.apache.tinkerpop.gremlin.structure.Direction;

/**
 * IMPORTANT: The byte values of the proper directions must be sequential,
 * i.e. the byte values of proper and improper directions may NOT be mixed.
 * This is crucial IN the retrieval for proper edges where we make this assumption.
 */
public class EdgeDirection {

    public static Direction fromPosition(int pos) {
        switch (pos) {
            case 0:
                return Direction.OUT;

            case 1:
                return Direction.IN;

            default:
                throw new IllegalArgumentException("Invalid position:" + pos);
        }
    }

    public static int position(Direction dir) {
        switch (dir) {
            case OUT:
                return 0;

            case IN:
                return 1;

            default:
                throw new IllegalArgumentException("Invalid direction: " + dir);
        }
    }
}
