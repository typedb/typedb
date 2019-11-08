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

package grakn.core.graph.hadoop.scan;

import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import grakn.core.graph.diskstorage.keycolumnvalue.scan.ScanMetrics;

public class HadoopContextScanMetrics implements ScanMetrics {

    private final TaskInputOutputContext taskIOCtx;
    // TODO make these configurable (?)
    public static final String CUSTOM_COUNTER_GROUP = "ScanJob.Custom";
    public static final String STANDARD_COUNTER_GROUP = "ScanJob.Standard";

    public HadoopContextScanMetrics(TaskInputOutputContext taskIOCtx) {
        this.taskIOCtx = taskIOCtx;
    }

    @Override
    public long getCustom(String metric) {
        return taskIOCtx.getCounter(CUSTOM_COUNTER_GROUP, metric).getValue();
    }

    @Override
    public void incrementCustom(String metric, long delta) {
        taskIOCtx.getCounter(CUSTOM_COUNTER_GROUP, metric).increment(delta);
    }

    @Override
    public void incrementCustom(String metric) {
        incrementCustom(metric, 1L);
    }

    @Override
    public long get(Metric metric) {
        return taskIOCtx.getCounter(STANDARD_COUNTER_GROUP, metric.name()).getValue();
    }

    @Override
    public void increment(Metric metric) {
        taskIOCtx.getCounter(STANDARD_COUNTER_GROUP, metric.name()).increment(1L);
    }
}
