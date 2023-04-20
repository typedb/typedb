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
    public final Counter TIME_PLANNING_MS;

    public final Counter COUNT_MATERIALISATIONS;
    public final Counter COUNT_CONJUNCTION_PROCESSORS;
    public final Counter COUNT_COMPOUND_STREAMS;

    public ReasonerPerfCounters(boolean enabled) {
        super( enabled ? ATOMICLONG_CREATOR : NOOP_CREATOR);
        TIME_PLANNING_MS = register("time_planning_ms");
        COUNT_MATERIALISATIONS = register("count_materialisations");
        COUNT_CONJUNCTION_PROCESSORS = register("count_conjunction_processors");
        COUNT_COMPOUND_STREAMS = register("count_compound_streams");
    }

}
