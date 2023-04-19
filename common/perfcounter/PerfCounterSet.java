/*
 * Copyright (C) 2022 Vaticle
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
 *
 */

package com.vaticle.typedb.core.common.perfcounter;

import com.vaticle.typedb.core.common.exception.TypeDBException;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;

public abstract class PerfCounterSet<KEY_TYPE extends PerfCounterSet.Key> {

    public interface Key {
        int ordinal();

        String name();
    }

    private AtomicLong[] counters;
    private final Map<String, CustomCounter> customCounters;

    public PerfCounterSet(int keySetSize) {
        this.counters = new AtomicLong[keySetSize];
        for (int i = 0; i < keySetSize; i++) {
            this.counters[i] = new AtomicLong();
        }
        this.customCounters = new HashMap<>();
    }

    public static String prettyPrint(Map<Key, Long> counterMap) {
        StringBuilder sb = new StringBuilder();
        counterMap.keySet().stream().sorted((o1, o2) -> {
            int first =  Integer.compare(o1.ordinal(), o2.ordinal());
            return first == 0 ? String.CASE_INSENSITIVE_ORDER.compare(o1.name(), o2.name()) : first;
        }).forEach(key -> {
            sb.append(String.format("%-48s: %-20d\n", key, counterMap.get(key)));
        });

        return sb.toString();
    }

    public abstract KEY_TYPE[] keys();

    public void add(KEY_TYPE key, long delta) {
        counters[key.ordinal()].addAndGet(delta);
    }

    public long get(KEY_TYPE key) {
        return counters[key.ordinal()].get();
    }

    public AtomicLong getCounter(KEY_TYPE key) {
        return counters[key.ordinal()];
    }

    public TimeCounterMillis addTimerMillis(KEY_TYPE key) {
        return new TimeCounterMillis(counters[key.ordinal()]);
    }

    public CustomCounter customCounter(String name) {
        synchronized (customCounters) {
            if (!customCounters.containsKey(name)) {
                customCounters.put(name, new CustomCounter(name));
            }
        }
        return customCounters.get(name);
    }

    public Map<Key, Long> toMapUnsynchronised() {
        Map<Key, Long> unsynchronisedMap = new HashMap<>();
        for (KEY_TYPE key: keys()) {
            unsynchronisedMap.put(key, get(key));
        }

        for (CustomCounter customCounter: customCounters.values()) {
            unsynchronisedMap.put(customCounter, customCounter.get());
        }

        return unsynchronisedMap;
    }

    public class CustomCounter implements Key {
        private final String name;
        private final AtomicLong counter;

        private CustomCounter(String name) {
            this.name = name;
            this.counter = new AtomicLong();
        }

        public void add(long delta) {
            counter.addAndGet(delta);
        }

        public long get() {
            return counter.get();
        }

        public AtomicLong getCounter() {
            return counter;
        }

        @Override
        public int ordinal() {
            return Integer.MAX_VALUE;
        }

        public String name() {
            return name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    public class StopWatch {

        private final Instant startInstant;

        public StopWatch() {
            this.startInstant = Instant.now();
        }

        public Duration lap() {
            return Duration.between(startInstant, Instant.now());
        }
    }

    public class TimeCounterMillis implements AutoCloseable {
        private AtomicLong addTo;
        private final StopWatch stopWatch;

        private TimeCounterMillis(AtomicLong addTo) {
            this.addTo = addTo;
            this.stopWatch = new StopWatch();
        }

        public Duration stopAndAdd() {
            Duration duration = stopWatch.lap();
            if (addTo == null) throw TypeDBException.of(RESOURCE_CLOSED);
            addTo.addAndGet(duration.toMillis());
            this.addTo = null;
            return duration;
        }

        @Override
        public void close() {
            stopAndAdd();
        }
    }
}
