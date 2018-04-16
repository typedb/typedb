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

package ai.grakn.generator;

/*-
 * #%L
 * grakn-test-tools
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Abstract class for generating test objects that handles some boilerplate.
 *
 * <p>
 *     New generators should extend this class and implement {@link #generate()}.
 * </p>
 *
 * @param <T> the type to generate
 *
 * @author Felix Chapman
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

    protected <S> Set<S> setOf(Class<S> clazz, int minSize, int maxSize) {
        return fillWith(Sets.newHashSet(), clazz, minSize, maxSize);
    }

    protected <S> List<S> listOf(Class<S> clazz, int minSize, int maxSize) {
        return fillWith(Lists.newArrayList(), clazz, minSize, maxSize);
    }

    private <S, U extends Collection<S>> U fillWith(U collection, Class<S> clazz, int minSize, int maxSize) {
        for (int i = 0; i < random.nextInt(minSize, maxSize); i ++) {
            collection.add(gen(clazz));
        }

        return collection;
    }
}
