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
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;

/**
 * Abstract class for generating test objects that handles some boilerplate.
 * @param <T> the type to generate
 */
public abstract class AbstractGenerator<T> extends Generator<T> {

    SourceOfRandomness random;
    GenerationStatus status;

    AbstractGenerator(Class<T> type) {
        super(type);
    }

    @Override
    public T generate(SourceOfRandomness random, GenerationStatus status) {
        this.random = random;
        this.status = status;
        return generate();
    }

    protected abstract T generate();

    final <S> S gen(Class<S> clazz) {
        return gen().type(clazz).generate(random, status);
    }
}
