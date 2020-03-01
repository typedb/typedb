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

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * System time interface that abstracts time units, resolution, and measurements of time.
 */
public interface TimestampProvider {

    /**
     * Returns the current time based on this timestamp provider
     * as a Instant.
     *
     */
    Instant getTime();

    /**
     * Returns the given time as a Instant based off of this timestamp providers units
     */
    Instant getTime(long sinceEpoch);

    /**
     * Return the units of #getTime(). This method's return value must
     * be constant over at least the life of the object implementing this
     * interface.
     *
     * @return this instance's time unit
     */
    ChronoUnit getUnit();

    /**
     * Block until the current time as returned by #getTime() is greater
     * than the given timepoint.
     *
     * @param futureTime The time to sleep past
     *
     * @return the current time in the same units as the {@code unit} argument
     * @throws InterruptedException
     *             if externally interrupted
     */
    Instant sleepPast(Instant futureTime) throws InterruptedException;

    /**
     * Sleep for the given duration of time.
     */
    void sleepFor(Duration duration) throws InterruptedException;

    /**
     * Returns a Timer based on this timestamp provider
     */
    Timer getTimer();


    /**
     * Returns the scalar value for this instant given the configured time unit
     */
    long getTime(Instant timestamp);
}
