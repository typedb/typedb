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

import com.vaticle.typedb.core.common.perfcounter.PerfCounterSet;

public class ReasonerPerfCounters extends PerfCounterSet<ReasonerPerfCounters.Key> {

    public ReasonerPerfCounters() {
        super(Key.END_MARKER.ordinal() + 1);
    }

    public enum Key implements PerfCounterSet.Key {
        TIME_PLANNING_MS,

        COUNT_MATERIALISATIONS,
        COUNT_CONJUNCTION_PROCESSORS,
        COUNT_COMPOUND_STREAMS,
        
        END_MARKER;
    }

    @Override
    public Key[] keys() {
        return Key.values();
    }
}
