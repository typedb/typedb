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

package grakn.core.graph.diskstorage.util.time;

import com.google.common.base.Preconditions;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Utility methods for dealing with Duration
 */
public class Durations {

    public static Duration min(Duration x, Duration y) {
        return x.compareTo(y) <= 0 ? x : y;
    }

    /*
     * This method is based on the method of the same name in Stopwatch.java in
     * Google Guava 14.0.1, where it was defined with private visibility.
     */
    private static String abbreviate(ChronoUnit unit) {
        switch (unit) {
            case NANOS:
                return "ns";
            case MICROS:
                return "\u03bcs"; // μs
            case MILLIS:
                return "ms";
            case SECONDS:
                return "s";
            case MINUTES:
                return "m";
            case HOURS:
                return "h";
            case DAYS:
                return "d";
            default:
                throw new AssertionError("Unexpected time unit: " + unit);
        }

    }

    private static final Map<String, TemporalUnit> unitNames = new HashMap<String, TemporalUnit>() {{
        for (ChronoUnit unit : Arrays.asList(ChronoUnit.NANOS, ChronoUnit.MICROS, ChronoUnit.MILLIS, ChronoUnit.SECONDS, ChronoUnit.MINUTES, ChronoUnit.HOURS, ChronoUnit.DAYS)) {
            put(abbreviate(unit), unit); //abbreviated name
            String name = unit.toString().toLowerCase();
            put(name, unit); //abbreviated name in singular
            put(name.substring(0, name.length() - 1), unit);
        }
        put("us", ChronoUnit.MICROS);
    }};

    public static TemporalUnit parse(String unitName) {
        TemporalUnit unit = unitNames.get(unitName.toLowerCase());
        Preconditions.checkNotNull(unit, "Unknown unit time: %s", unitName);
        return unit;
    }

    public static int compare(long length1, TimeUnit unit1, long length2, TimeUnit unit2) {
        /*
         * Don't do this:
         *
         * return (int)(o.getLength(unit) - getLength(unit));
         *
         * 2^31 ns = 2.14 seconds and 2^31 us = 36 minutes. The narrowing cast
         * from long to integer is practically guaranteed to cause failures at
         * either nanosecond resolution (where almost everything will fail) or
         * microsecond resolution (where the failures would be more insidious;
         * perhaps lock expiration malfunctioning).
         *
         * The following implementation is ugly, but unlike subtraction-based
         * implementations, it is affected by neither arithmetic overflow
         * (because it does no arithmetic) nor loss of precision from
         * long-to-integer casts (because it does not cast).
         */
        long length2Adj = unit1.convert(length2, unit2);
        if (length1 < length2Adj) {
            return -1;
        } else if (length2Adj < length1) {
            return 1;
        }
        return 0;
    }

}
