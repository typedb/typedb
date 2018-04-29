/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.query.runner;

import ai.grakn.ComputeJob;
import ai.grakn.GraknComputer;
import ai.grakn.factory.EmbeddedGraknSession;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A compute query job executed against a {@link GraknComputer}.
 *
 * @author Felix Chapman
 */
class TinkerComputeJob<T> implements ComputeJob<T> {

    private final GraknComputer computer;

    private final Supplier<T> supplier;

    public TinkerComputeJob (GraknComputer computer, Function<GraknComputer, T> function) {
        this.computer = computer;
        this.supplier = () -> function.apply(this.computer);
    }

    @Override
    public T get() {
        return supplier.get();
    }

    @Override
    public void kill() {
        computer.killJobs();
    }
}
