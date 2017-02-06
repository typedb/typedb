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

import ai.grakn.concept.ResourceType;
import ai.grakn.concept.TypeName;
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

public class ResourceTypes extends FromGraphGenerator<ResourceType> {

    private Unique unique;
    private boolean excludeMeta = false;

    public ResourceTypes() {
        super(ResourceType.class);
    }

    @Override
    public ResourceType<?> generate() {
        Collection<ResourceType<?>> resourceTypes = graph().admin().getMetaResourceType().subTypes();

        if (unique != null) {
            resourceTypes = resourceTypes.stream().filter(r -> r.isUnique().equals(unique.value())).collect(toSet());
        }

        if (excludeMeta) {
            resourceTypes.remove(graph().admin().getMetaResourceType());
        }

        if (resourceTypes.isEmpty()) {
            TypeName name = unusedName();
            ResourceType.DataType<?> dataType = gen(ResourceType.DataType.class);

            boolean shouldBeUnique = unique != null ? unique.value() : random.nextBoolean();

            if (shouldBeUnique) {
                return graph().putResourceTypeUnique(name, dataType);
            } else {
                return graph().putResourceType(name, dataType);
            }
        } else {
            return random.choose(resourceTypes);
        }
    }

    public void configure(Unique unique) {
        this.unique = unique;
    }

    public void configure(NotMeta notMeta) {
        excludeMeta();
    }

    ResourceTypes excludeMeta() {
        this.excludeMeta = true;
        return this;
    }

    @Target({PARAMETER, FIELD, ANNOTATION_TYPE, TYPE_USE})
    @Retention(RUNTIME)
    @GeneratorConfiguration
    public @interface Unique {
        boolean value() default true;
    }

}
