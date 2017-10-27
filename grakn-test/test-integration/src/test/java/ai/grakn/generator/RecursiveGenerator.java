/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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
 *
 */

package ai.grakn.generator;

import com.pholser.junit.quickcheck.generator.GenerationStatus;

/**
 * Generator for recursive types. Designed to stop after a certain depth.
 *
 * @author Felix Chapman
 */
public abstract class RecursiveGenerator<T> extends AbstractGenerator<T> {

    RecursiveGenerator(Class<T> type) {
        super(type);
    }

    private static final GenerationStatus.Key<Integer> DEPTH = new GenerationStatus.Key<>("depth", Integer.class);
    private static final int MAX_DEPTH = 3;

    @Override
    protected final T generate() {
        int depth = status.valueOf(DEPTH).orElse(MAX_DEPTH);

        status.setValue(DEPTH, depth - 1);

        T result;

        if (depth > 0) {
            result = generateRecurse();
        } else {
            result = generateBase();
        }

        status.setValue(DEPTH, depth);

        return result;
    }

    protected abstract T generateBase();

    protected abstract T generateRecurse();
}