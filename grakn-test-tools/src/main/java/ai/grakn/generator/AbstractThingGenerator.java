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

import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.generator.AbstractOntologyConceptGenerator.NonMeta;

import java.util.Collection;

/**
 * Abstract class for generating {@link Thing}s.
 *
 * @param <T> The kind of {@link Thing} to generate.
 * @param <S> The {@link Type} of the {@link Thing} being generated.
 *
 * @author Felix Chapman
 */
public abstract class AbstractThingGenerator<T extends Thing, S extends Type> extends FromGraphGenerator<T> {

    private final Class<? extends AbstractTypeGenerator<S>> generatorClass;

    AbstractThingGenerator(Class<T> type, Class<? extends AbstractTypeGenerator<S>> generatorClass) {
        super(type);
        this.generatorClass = generatorClass;
    }

    @Override
    protected final T generateFromGraph() {
        S type = genFromGraph(generatorClass).makeExcludeAbstractTypes().excludeMeta().generate(random, status);

        Collection<T> instances = (Collection<T>) type.instances();
        if (instances.isEmpty()) {
            return newInstance(type);
        } else {
            return random.choose(instances);
        }
    }

    public final void configure(NonMeta nonMeta) {
    }

    protected abstract T newInstance(S type);
}
