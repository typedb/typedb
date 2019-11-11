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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;


public class StandardScanMetrics implements ScanMetrics {

    private final EnumMap<Metric, AtomicLong> metrics;
    private final ConcurrentMap<String, AtomicLong> customMetrics;

    private static final Logger LOG = LoggerFactory.getLogger(StandardScanMetrics.class);

    public StandardScanMetrics() {
        metrics = new EnumMap<>(ScanMetrics.Metric.class);
        for (Metric m : Metric.values()) {
            metrics.put(m, new AtomicLong(0));
        }
        customMetrics = new ConcurrentHashMap<>();
    }

    @Override
    public long getCustom(String metric) {
        AtomicLong counter = customMetrics.get(metric);
        if (counter == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("[{}:{}] Returning zero by default (was null)", System.identityHashCode(customMetrics), metric);
            }
            return 0;
        } else {
            long v = counter.get();
            if (LOG.isDebugEnabled()) {
                LOG.debug("[{}:{}] Returning {}", System.identityHashCode(customMetrics), metric, v);
            }
            return v;
        }
    }

    @Override
    public void incrementCustom(String metric, long delta) {
        AtomicLong counter = customMetrics.get(metric);
        if (counter == null) {
            customMetrics.putIfAbsent(metric, new AtomicLong(0));
            counter = customMetrics.get(metric);
        }
        counter.addAndGet(delta);
        if (LOG.isDebugEnabled()) {
            LOG.debug("[{}:{}] Incremented by {}", System.identityHashCode(customMetrics), metric, delta);
        }
    }

    @Override
    public void incrementCustom(String metric) {
        incrementCustom(metric, 1);
    }

    @Override
    public long get(Metric metric) {
        return metrics.get(metric).get();
    }

    @Override
    public void increment(Metric metric) {
        metrics.get(metric).incrementAndGet();
    }


}
