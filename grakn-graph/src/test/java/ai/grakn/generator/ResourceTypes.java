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

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public class ResourceTypes extends AbstractTypeGenerator<ResourceType> {

    private Unique unique;

    public ResourceTypes() {
        super(ResourceType.class);
    }

    @Override
    protected ResourceType newType(TypeName name) {
        ResourceType.DataType<?> dataType = gen(ResourceType.DataType.class);

        boolean shouldBeUnique = unique != null ? unique.value() : random.nextBoolean();

        if (shouldBeUnique) {
            return graph().putResourceTypeUnique(name, dataType);
        } else {
            return graph().putResourceType(name, dataType);
        }
    }

    @Override
    protected ResourceType metaType() {
        return graph().admin().getMetaResourceType();
    }

    @Override
    protected boolean filter(ResourceType type) {
        return unique == null || type.isUnique().equals(unique.value());
    }

    public void configure(Unique unique) {
        this.unique = unique;
    }

    @Target({PARAMETER, FIELD, ANNOTATION_TYPE, TYPE_USE})
    @Retention(RUNTIME)
    @GeneratorConfiguration
    public @interface Unique {
        boolean value() default true;
    }

}
