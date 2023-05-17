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

    public static final String KEY_TIME_PLANNING = "time_planning_ms";
    public static final String KEY_COUNT_MATERIALISATIONS = "count_materialisations";
    public static final String KEY_COUNT_CONJUNCTION_PROCESSORS = "count_conjunction_processors";
    public static final String KEY_COUNT_COMPOUND_STREAMS = "count_compound_streams";

    public final PerfCounters.Counter timePlanning;
    public final PerfCounters.Counter countMaterialisations;
    public final PerfCounters.Counter countConjunctionProcessors;
    public final PerfCounters.Counter countCompoundStreams;

    public ReasonerPerfCounters(boolean enabled) {
        super(enabled);
        timePlanning = register(KEY_TIME_PLANNING);
        countMaterialisations = register(KEY_COUNT_MATERIALISATIONS);
        countConjunctionProcessors = register(KEY_COUNT_CONJUNCTION_PROCESSORS);
        countCompoundStreams = register(KEY_COUNT_COMPOUND_STREAMS);
    }
}