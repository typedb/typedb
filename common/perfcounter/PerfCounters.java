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

import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class PerfCounters {
    public interface Counter {
        String name();

        void add(long delta);

        long get();
    }

    private final Function<String, Counter> counterCreator;
    private final Collection<Counter> counters;
    private final boolean enabled;

    public PerfCounters(boolean enabled) {
        this.enabled = enabled;
        this.counterCreator = enabled ? AtomicLongCounter::new : NoOpCounter::new;
        this.counters = new ConcurrentLinkedQueue<>();
    }

    public Counter register(String name) {
        Counter ctr = counterCreator.apply(name);
        this.counters.add(ctr);
        return ctr;
    }

    public Counter register(String name, long initialValue) {
        Counter counter = register(name);
        counter.add(initialValue);
        return counter;
    }

    public Collection<Counter> counters() {
        return counters;
    }

    public PerfCounters cloneUnsynchronised() {
        PerfCounters cloned = new PerfCounters(enabled);
        counters.forEach(counter -> cloned.register(counter.name(), counter.get()));
        return cloned;
    }

    public String prettyPrintUnsynchronised() {
        StringBuilder sb = new StringBuilder();
        counters.stream().sorted(Comparator.comparing(Counter::name)).forEach(counter -> {
            sb.append(String.format("%-48s: %-20d\n", counter.name(), counter.get()));
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
