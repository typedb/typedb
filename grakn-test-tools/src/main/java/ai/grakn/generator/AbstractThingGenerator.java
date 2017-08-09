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
import java.util.Set;
import java.util.stream.Stream;

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

        if(withResource){
            //Getting the resource type we need
            Set<ResourceType> availableResourceTypes = Stream.concat(type.resources(), type.keys()).
                    filter(rt -> !rt.isAbstract()).
                    filter(rt -> !rt.equals(type)).
                    collect(toSet());

            ResourceType resourceType;
            if(availableResourceTypes.isEmpty()){
                ResourceType.DataType<?> dataType = gen(ResourceType.DataType.class);
                Label label = genFromGraph(Labels.class).mustBeUnused().generate(random, status);
                resourceType = graph().putResourceType(label, dataType);
                //resourceType = genFromGraph(ResourceTypes.class).makeExcludeAbstractTypes().excludeMeta().generate(random, status);
                System.out.println("Creating new resource type: " + resourceType.getLabel());
                type.resource(resourceType);
            } else {
                resourceType = random.choose(availableResourceTypes);
                System.out.println("Using old resource type: " + resourceType.getLabel());
            }

            //Getting the resource we need
            //noinspection unchecked
            Set<Resource> availableResources = (Set<Resource>) resourceType.instances().collect(toSet());

            Resource resource;
            if(availableResources.isEmpty()){
                System.out.println("Creating new resource from type: " + resourceType.getLabel());
                resource = newResource(resourceType);
            } else {
                System.out.println("Choosing old resource from type: " + resourceType.getLabel());
                resource = random.choose(availableResources);
            }

            System.out.println("Give resource of type [" + resource.type().getLabel() + "] to a thing of type [" + thing.type().getLabel() + "]");
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
