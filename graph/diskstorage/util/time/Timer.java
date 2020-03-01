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
import java.time.Instant;
import java.time.temporal.TemporalUnit;

/**
 * A utility to measure time durations.
 * <p>
 * Differs from Guava Stopwatch in the following ways:
 *
 * <ul>
 * <li>encapsulates longs behind Instant/Duration</li>
 * <li>replacing the underlying Ticker with a TimestampProvider</li>
 * <li>can only be started and stopped once</li>
 * </ul>
 */
public class Timer {

    private final TimestampProvider times;
    private Instant start;
    private Instant stop;

    public Timer(TimestampProvider times) {
        this.times = times;
    }

    public Timer start() {
        Preconditions.checkState(null == start, "Timer can only be started once");
        start = times.getTime();
        return this;
    }

    public Instant getStartTime() {
        Preconditions.checkNotNull(start, "Timer never started");
        return start;
    }

    public Timer stop() {
        Preconditions.checkNotNull(start, "Timer stopped before it was started");
        stop = times.getTime();
        return this;
    }

    public Duration elapsed() {
        if (null == start) {
            return Duration.ZERO;
        }
        final Instant stopTime = (null==stop? times.getTime() : stop);
        return Duration.between(start, stopTime);
    }

    public String toString() {
        TemporalUnit u = times.getUnit();
        if (start==null) return "Initialized";
        if (stop==null) return String.format("Started at %d %s",times.getTime(start),u);
        return String.format("%d %s", times.getTime(stop) - times.getTime(start), u);
    }
}
