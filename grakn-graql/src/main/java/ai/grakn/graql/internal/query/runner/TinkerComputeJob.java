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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
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

    private TinkerComputeJob(GraknComputer computer, Supplier<T> supplier) {
        this.computer = computer;
        this.supplier = supplier;
    }

    static <T> TinkerComputeJob<T> create(EmbeddedGraknSession session, Function<GraknComputer, T> function) {
        GraknComputer computer = session.getGraphComputer();

        return new TinkerComputeJob<>(computer, () -> function.apply(computer));
    }

    @Override
    public T get() {
        return supplier.get();
    }

    @Override
    public void kill() {
        computer.killJobs();
    }

    public <S> TinkerComputeJob<S> map(Function<T, S> function) {
        return new TinkerComputeJob<>(computer, () -> function.apply(supplier.get()));
    }
}
