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

package grakn.core.graph.util.stats;

import com.google.common.base.Preconditions;


public class NumberUtil {

    public static boolean isPowerOf2(long value) {
        return value > 0 && Long.highestOneBit(value) == value;
    }

    /**
     * Returns an integer X such that 2^X=value. Throws an exception
     * if value is not a power of 2.
     */
    public static int getPowerOf2(long value) {
        Preconditions.checkArgument(isPowerOf2(value));
        return Long.SIZE - (Long.numberOfLeadingZeros(value) + 1);
    }

}
