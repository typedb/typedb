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

package grakn.core.graph.util.encoding;

/**
 * This file is copied verbatim from Apache Lucene NumericUtils.java
 * Only the double/float to sortable long/int conversions are retained.
 */
public final class NumericUtils {

  private NumericUtils() {} // no instance!

  /**
   * Converts a <code>double</code> value to a sortable signed <code>long</code>.
   * The value is converted by getting their IEEE 754 floating-point &quot;double format&quot;
   * bit layout and then some bits are swapped, to be able to compare the result as long.
   * By this the precision is not reduced, but the value can easily used as a long.
   * The sort order (including Double#NaN) is defined by
   * Double#compareTo; {@code NaN} is greater than positive infinity.
   * see #sortableLongToDouble
   */
  public static long doubleToSortableLong(double val) {
    return sortableDoubleBits(Double.doubleToLongBits(val));
  }

  /**
   * Converts a sortable <code>long</code> back to a <code>double</code>.
   * see #doubleToSortableLong
   */
  public static double sortableLongToDouble(long val) {
    return Double.longBitsToDouble(sortableDoubleBits(val));
  }

  /**
   * Converts a <code>float</code> value to a sortable signed <code>int</code>.
   * The value is converted by getting their IEEE 754 floating-point &quot;float format&quot;
   * bit layout and then some bits are swapped, to be able to compare the result as int.
   * By this the precision is not reduced, but the value can easily used as an int.
   * The sort order (including Float#NaN) is defined by
   * Float#compareTo; {@code NaN} is greater than positive infinity.
   * see #sortableIntToFloat
   */
  public static int floatToSortableInt(float val) {
    return sortableFloatBits(Float.floatToIntBits(val));
  }

  /**
   * Converts a sortable <code>int</code> back to a <code>float</code>.
   * see #floatToSortableInt
   */
  public static float sortableIntToFloat(int val) {
    return Float.intBitsToFloat(sortableFloatBits(val));
  }
  
  /** Converts IEEE 754 representation of a double to sortable order (or back to the original) */
  public static long sortableDoubleBits(long bits) {
    return bits ^ (bits >> 63) & 0x7fffffffffffffffL;
  }
  
  /** Converts IEEE 754 representation of a float to sortable order (or back to the original) */
  public static int sortableFloatBits(int bits) {
    return bits ^ (bits >> 31) & 0x7fffffff;
  }


}
