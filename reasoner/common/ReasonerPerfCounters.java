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

public class ReasonerPerfCounters extends PerfCounters {

    public static final String PLANNING_TIME_NS = "planner_time_planning_ns";
    public static final String MATERIALISATIONS = "processor_materialisations";
    public static final String CONJUNCTION_PROCESSORS = "processors_conjunction_processors";
    public static final String COMPOUND_STREAMS = "streams_compound_streams";

    public final PerfCounters.Counter timePlanning;
    public final PerfCounters.Counter materialisations;
    public final PerfCounters.Counter conjunctionProcessors;
    public final PerfCounters.Counter compoundStreams;

    public ReasonerPerfCounters(boolean enabled) {
        super(enabled);
        timePlanning = register(PLANNING_TIME_NS);
        materialisations = register(MATERIALISATIONS);
        conjunctionProcessors = register(CONJUNCTION_PROCESSORS);
        compoundStreams = register(COMPOUND_STREAMS);
    }
}
