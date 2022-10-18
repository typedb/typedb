package com.vaticle.typedb.core.common.util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public abstract class PerfCounter {

    public static PerfCounterImpl createCounter() { return new PerfCounterImpl(); }

    public static NoOpPerfCounter createNoOpCounter() { return new NoOpPerfCounter(); }

    public abstract long get(Object identifier);

    public abstract void add(Object identifier, long delta);

    private static class PerfCounterImpl extends PerfCounter {

        private final Map<Object, AtomicLong> counters;

        public PerfCounterImpl() {
            counters = new HashMap<>();
        }

        public long get(Object identifier) {
            return counters.computeIfAbsent(identifier, id -> new AtomicLong(0)).get();
        }

        public void add(Object identifier, long delta) {
            counters.computeIfAbsent(identifier, id -> new AtomicLong(0)).addAndGet(delta);
        }
    }

    private static class NoOpPerfCounter extends PerfCounter {

        @Override
        public long get(Object identifier) { return -1; }

        @Override
        public void add(Object identifier, long delta) { }
    }
}
