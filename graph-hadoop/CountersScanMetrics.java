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

package grakn.core.graph.hadoop;

import org.apache.hadoop.mapreduce.Counters;
import grakn.core.graph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import grakn.core.graph.hadoop.scan.HadoopContextScanMetrics;

public class CountersScanMetrics implements ScanMetrics {

    private final Counters counters;

    public CountersScanMetrics(Counters counters) {
        this.counters = counters;
    }

    @Override
    public long getCustom(String metric) {
        return counters.getGroup(HadoopContextScanMetrics.CUSTOM_COUNTER_GROUP).findCounter(metric).getValue();
    }

    @Override
    public void incrementCustom(String metric, long delta) {
        counters.getGroup(HadoopContextScanMetrics.CUSTOM_COUNTER_GROUP).findCounter(metric).increment(delta);
    }

    @Override
    public void incrementCustom(String metric) {
        incrementCustom(metric, 1L);
    }

    @Override
    public long get(Metric metric) {
        return counters.getGroup(HadoopContextScanMetrics.STANDARD_COUNTER_GROUP).findCounter(metric.name()).getValue();
    }

    @Override
    public void increment(Metric metric) {
        counters.getGroup(HadoopContextScanMetrics.STANDARD_COUNTER_GROUP).findCounter(metric.name()).increment(1L);
    }
}
