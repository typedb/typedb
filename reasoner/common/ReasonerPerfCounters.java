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

package com.vaticle.typedb.core.reasoner.common;

import com.vaticle.typedb.core.common.perfcounter.PerfCounters;
import com.vaticle.typedb.core.concurrent.executor.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ReasonerPerfCounters extends PerfCounters {

    private static final Logger LOG = LoggerFactory.getLogger(ReasonerPerfCounters.class);

    public static final String PLANNING_TIME_NS = "planner_time_planning_ns";
    public static final String MATERIALISATIONS = "processor_materialisations";
    public static final String CONJUNCTION_PROCESSORS = "processors_conjunction_processors";
    public static final String COMPOUND_STREAMS = "streams_compound_streams";
    public static final String COMPOUND_STREAM_MESSAGES_RECEIVED = "streams_compound_stream_messages_received";
    public static final String RETRIEVABLE_PROCESSORS = "processors_retrievable";

    public final Counter timePlanning;
    public final Counter materialisations;
    public final Counter conjunctionProcessors;
    public final Counter compoundStreams;
    public final Counter compoundStreamMessagesReceived;
    public final Counter retrievableProcessors;

    public ReasonerPerfCounters(boolean enabled) {
        super(enabled);
        timePlanning = register(PLANNING_TIME_NS);
        materialisations = register(MATERIALISATIONS);
        conjunctionProcessors = register(CONJUNCTION_PROCESSORS);
        compoundStreams = register(COMPOUND_STREAMS);
        compoundStreamMessagesReceived = register(COMPOUND_STREAM_MESSAGES_RECEIVED);
        retrievableProcessors = register(RETRIEVABLE_PROCESSORS);
    }

    public void logCounters() {
        if (enabled) LOG.debug("Perf counters:\n{}", this);
    }

    private ScheduledFuture<?> printingTask;

    public synchronized void startPeriodicPrinting() {
        if (printingTask == null) {
            printingTask = Executors.scheduled().scheduleAtFixedRate(this::logCounters, 10, 10, TimeUnit.SECONDS);
        }
    }

    public synchronized void stopPrinting() {
        if (printingTask != null) {
            printingTask.cancel(false);
            printingTask = null;
        }
    }
}
