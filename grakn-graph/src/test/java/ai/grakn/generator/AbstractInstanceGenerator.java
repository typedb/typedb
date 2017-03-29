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

import ai.grakn.concept.Instance;
import ai.grakn.concept.Type;
import ai.grakn.generator.AbstractTypeGenerator.Meta;

import java.util.Collection;

public abstract class AbstractInstanceGenerator<T extends Instance, S extends Type> extends FromGraphGenerator<T> {

    private final Class<? extends AbstractTypeGenerator<S>> generatorClass;

    AbstractInstanceGenerator(Class<T> type, Class<? extends AbstractTypeGenerator<S>> generatorClass) {
        super(type);
        this.generatorClass = generatorClass;
    }

    @Override
    protected final T generateFromGraph() {
        S type = genFromGraph(generatorClass).excludeMeta().generate(random, status);

        Collection<T> instances = (Collection<T>) type.instances();
        if (instances.isEmpty()) {
            return newInstance(type);
        } else {
            return random.choose(instances);
        }
    }

    public final void configure(Meta meta) {
        // Instances are never meta types
        if (meta.value()) {
            throw new IllegalArgumentException("Cannot generate meta instances");
        }
    }

    protected abstract T newInstance(S type);
}
