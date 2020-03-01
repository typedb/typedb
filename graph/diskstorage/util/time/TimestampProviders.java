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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

/**
 * Implementations of TimestampProvider for different resolutions of time:
 * <ul>
 * <li>NANO: nano-second time resolution based on System.nanoTime using a base-time established
 * by System.currentTimeMillis(). The exact resolution depends on the particular JVM and host machine.</li>
 * <li>MICRO: micro-second time which is actually at milli-second resolution.</li>
 * <li>MILLI: milli-second time resolution</li>
 * </ul>
 */
public enum TimestampProviders implements TimestampProvider {
    NANO {
        /**
         * This returns the approximate number of nanoseconds
         * elapsed since the UNIX Epoch.  The least significant
         * bit is overridden to 1 or 0 depending on whether
         * setLSB is true or false (respectively).
         * <p/>
         * This timestamp rolls over about every 2^63 ns, or
         * just over 292 years.  The first rollover starting
         * from the UNIX Epoch would be sometime in 2262.
         *
         * @return a timestamp as described above
         */
        @Override
        public Instant getTime() {
            return Instant.now();
        }

        @Override
        public Instant getTime(long sinceEpoch) {
            return Instant.ofEpochSecond(0, sinceEpoch);
        }

        @Override
        public ChronoUnit getUnit() {
            return ChronoUnit.NANOS;
        }

        @Override
        public long getTime(Instant timestamp) {
            return timestamp.getEpochSecond() * 1000000000L + timestamp.getNano();
        }
    },

    MICRO {
        @Override
        public Instant getTime() {
            return Instant.now();
        }

        @Override
        public Instant getTime(long sinceEpoch) {
            return Instant.ofEpochSecond(0, (sinceEpoch * 1000L));
        }

        @Override
        public ChronoUnit getUnit() {
            return ChronoUnit.MICROS;
        }

        @Override
        public long getTime(Instant timestamp) {
            return timestamp.getEpochSecond() * 1000000L + timestamp.getNano() / 1000;

        }
    },

    MILLI {
        @Override
        public Instant getTime() {
            return Instant.now();
        }

        @Override
        public Instant getTime(long sinceEpoch) {
            return Instant.ofEpochMilli(sinceEpoch);
        }

        @Override
        public ChronoUnit getUnit() {
            return ChronoUnit.MILLIS;
        }

        @Override
        public long getTime(Instant timestamp) {
            return timestamp.getEpochSecond() * 1000 + timestamp.getNano() / 1000000;
        }
    };

    private static final Logger LOG = LoggerFactory.getLogger(TimestampProviders.class);

    @Override
    public Instant sleepPast(Instant futureTime) throws InterruptedException {

        Instant now;
        ChronoUnit unit = getUnit();
        /*
         * Distributed storage managers that rely on timestamps play with the
         * least significant bit in timestamp longs, turning it on or off to
         * ensure that deletions are logically ordered before additions within a
         * single batch mutation. This is not a problem at microsecond
         * resolution because we pretend to have microsecond resolution by
         * multiplying currentTimeMillis by 1000, so the LSB can vary freely.
         * It's also not a problem with nanosecond resolution because the
         * resolution is just too fine, relative to how long a mutation takes,
         * for it to matter in practice. But it can lead to corruption at
         * millisecond resolution (and does, in testing).
         */
        if (unit.equals(ChronoUnit.MILLIS)) {
            futureTime = futureTime.plusMillis(1L);
        }

        while ((now = getTime()).compareTo(futureTime) <= 0) {

            long delta = getTime(futureTime) - getTime(now);

            if (0L == delta) {
                delta = 1L;
            }

            LOG.trace("Sleeping: now={} targettime={} delta={} {}", now, futureTime, delta, unit);

            Temporals.timeUnit(unit).sleep(delta);
        }

        return now;
    }

    @Override
    public void sleepFor(Duration duration) throws InterruptedException {
        if (duration.isZero()) return;

        TimeUnit.NANOSECONDS.sleep(duration.toNanos());
    }

    @Override
    public Timer getTimer() {
        return new Timer(this);
    }

    @Override
    public String toString() {
        return name();
    }


}
