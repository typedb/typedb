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

import ai.grakn.concept.Label;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.generator.AbstractOntologyConceptGenerator.NonMeta;
import com.pholser.junit.quickcheck.generator.GeneratorConfiguration;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Collection;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.stream.Collectors.toSet;

/**
 * Abstract class for generating {@link Thing}s.
 *
 * @param <T> The kind of {@link Thing} to generate.
 * @param <S> The {@link Type} of the {@link Thing} being generated.
 *
 * @author Felix Chapman
 */
public abstract class AbstractThingGenerator<T extends Thing, S extends Type> extends FromGraphGenerator<T> {
    private boolean withResource = false;
    private final Class<? extends AbstractTypeGenerator<S>> generatorClass;

    AbstractThingGenerator(Class<T> type, Class<? extends AbstractTypeGenerator<S>> generatorClass) {
        super(type);
        this.generatorClass = generatorClass;
    }

    @Override
    protected final T generateFromGraph() {
        T thing;
        S type = genFromGraph(generatorClass).makeExcludeAbstractTypes().excludeMeta().generate(random, status);

        //noinspection unchecked
        Collection<T> instances = (Collection<T> ) type.instances().collect(toSet());
        if (instances.isEmpty()) {
            thing = newInstance(type);
        } else {
            thing = random.choose(instances);
        }

        if(withResource && !thing.resources().findAny().isPresent()){
            //A new resource type is created every time a resource is lacking.
            //Existing resource types and resources of those types are not used because we end up mutating the
            // the ontology in strange ways. This approach is less complex but ensures everything has a resource
            // without conflicting with the ontology

            //Create a new resource type
            ResourceType.DataType<?> dataType = gen(ResourceType.DataType.class);
            Label label = genFromGraph(Labels.class).mustBeUnused().generate(random, status);
            ResourceType resourceType = graph().putResourceType(label, dataType);

            //Create new resource
            Resource resource = newResource(resourceType);

            //Link everything together
            type.resource(resourceType);
            thing.resource(resource);
        }

        return thing;
    }

    protected Resource newResource(ResourceType type) {
        ResourceType.DataType<?> dataType = type.getDataType();
        Object value = gen().make(ResourceValues.class).dataType(dataType).generate(random, status);
        return type.putResource(value);
    }

    public final void configure(NonMeta nonMeta) {
    }

    public final void configure(WithResource withResource) {
        this.withResource = true;
    }

    protected abstract T newInstance(S type);

    /**
     * Specify if the generated {@link Thing} should be connected to a {@link ai.grakn.concept.Resource}
     */
    @Target({PARAMETER, FIELD, ANNOTATION_TYPE, TYPE_USE})
    @Retention(RUNTIME)
    @GeneratorConfiguration
    public @interface WithResource {

    }
}
