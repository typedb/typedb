/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graph.diskstorage.keycolumnvalue.scan;

/**
 * Counters associated with a {@link ScanJob}.
 * <p>
 * Conceptually, this interface contains two separate stores of counters:
 * <ul>
 *     <li>the standard store, accessed via {@code get} and {@code increment}</li>
 *     <li>the custom store, accessed via methods with {@code custom} in their names</li>
 * </ul>
 * All counters values automatically start at zero.
 */
public interface ScanMetrics {

    /**
     * An enum of standard counters.  A value of this enum is the only parameter
     * accepted by {@link #get(ScanMetrics.Metric)}
     * and {@link #increment(ScanMetrics.Metric)}.
     */
    enum Metric {FAILURE, SUCCESS}

    /**
     * Get the value of a custom counter.  Only the effects of prior calls to
     * {@link #incrementCustom(String)} and {@link #incrementCustom(String, long)}
     * should be observable through this method, never the effects of prior calls to
     * {@link #increment(ScanMetrics.Metric)}.
     */
    long getCustom(String metric);

    /**
     * Increment a custom counter by {@code delta}.  The effects of calls
     * to method should only be observable through {@link #getCustom(String)},
     * never through {@link #get(ScanMetrics.Metric)}.
     *
     * @param metric the name of the counter
     * @param delta  the amount to add to the counter
     */
    void incrementCustom(String metric, long delta);

    /**
     * Like {@link #incrementCustom(String, long)}, except the {@code delta} is 1.
     *
     * @param metric the name of the counter to increment by 1
     */
    void incrementCustom(String metric);

    /**
     * Get the value of a standard counter.
     *
     * @param metric the standard counter whose value should be returned
     * @return the value of the standard counter
     */
    long get(Metric metric);

    /**
     * Increment a standard counter by 1.
     *
     * @param metric the standard counter whose value will be increased by 1
     */
    void increment(Metric metric);

}
