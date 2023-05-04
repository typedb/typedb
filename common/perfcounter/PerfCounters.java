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

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class PerfCounters {
    interface CounterCreator {
        Counter create(String name);
    }

    public interface Counter {
        String name();

        void add(long delta);

        long get();
    }

    public static final CounterCreator NOOP_CREATOR = name -> new NoOpCounter(name);
    public static final CounterCreator ATOMICLONG_CREATOR = name -> new AtomicLongCounter(name);

    private final CounterCreator counterCreator;
    private final Queue<Counter> counters;

    public PerfCounters(boolean enabled) {
        this.counterCreator = enabled ? ATOMICLONG_CREATOR : NOOP_CREATOR;
        this.counters = new ConcurrentLinkedQueue<>();
    }

    public Counter register(String name) {
        Counter ctr = counterCreator.create(name);
        this.counters.add(ctr);
        return ctr;
    }

    public Counter register(String name, long initialValue) {
        Counter counter = register(name);
        counter.add(initialValue);
        return counter;
    }

    public Map<String, Long> snapshotUnsynchronised() {
        Map<String, Long> unsynchronisedMap = new HashMap<>();
        counters.forEach(c -> unsynchronisedMap.put(c.name(), c.get()));
        return unsynchronisedMap;
    }

    public static long getNanos() {
        return System.nanoTime();
    }

    public static String prettyPrint(Map<String, Long> counters) {
        StringBuilder sb = new StringBuilder();
        counters.keySet().stream().sorted(String::compareTo).forEach(name -> {
            sb.append(String.format("%-48s: %-20d\n", name, counters.get(name)));
        });
        return sb.toString();
    }

    private static class NoOpCounter implements Counter {
        private final String name;

        private NoOpCounter(String name) {
            this.name = name;
        }

        public String name() {
            return name;
        }

        public void add(long delta) {
        }

        public long get() {
            return 0;
        }
    }

    private static class AtomicLongCounter implements Counter {
        private final String name;
        private final AtomicLong ctr;

        private AtomicLongCounter(String name) {
            this.name = name;
            this.ctr = new AtomicLong();
        }

        public String name() {
            return name;
        }

        public void add(long delta) {
            ctr.addAndGet(delta);
        }

        public long get() {
            return ctr.get();
        }
    }
}
