/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.perfcounter;

import com.vaticle.typedb.common.collection.ConcurrentSet;

import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class PerfCounters {
    public interface Counter {
        String name();

        void add(long delta);

        long get();
    }

    protected final boolean enabled;
    private final Function<String, Counter> counterConstructor;
    private final ConcurrentSet<Counter> counters;

    public PerfCounters(boolean enabled) {
        this.enabled = enabled;
        this.counterConstructor = enabled ? AtomicLongCounter::new : NoOpCounter::new;
        this.counters = new ConcurrentSet<>();
    }

    public Counter register(String name) {
        Counter ctr = counterConstructor.apply(name);
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

    @Override
    public String toString() {
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
