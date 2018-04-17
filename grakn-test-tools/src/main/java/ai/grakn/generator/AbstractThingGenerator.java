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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.generator;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Label;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.generator.AbstractSchemaConceptGenerator.NonMeta;
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
public abstract class AbstractThingGenerator<T extends Thing, S extends Type> extends FromTxGenerator<T> {
    private boolean withResource = false;
    private final Class<? extends AbstractTypeGenerator<S>> generatorClass;

    AbstractThingGenerator(Class<T> type, Class<? extends AbstractTypeGenerator<S>> generatorClass) {
        super(type);
        this.generatorClass = generatorClass;
    }

    @Override
    protected final T generateFromTx() {
        T thing;
        S type = genFromTx(generatorClass).makeExcludeAbstractTypes().excludeMeta().generate(random, status);

        //noinspection unchecked
        Collection<T> instances = (Collection<T> ) type.instances().collect(toSet());
        if (instances.isEmpty()) {
            thing = newInstance(type);
        } else {
            thing = random.choose(instances);
        }

        if(withResource && !thing.attributes().findAny().isPresent()){
            //A new attribute type is created every time a attribute is lacking.
            //Existing attribute types and resources of those types are not used because we end up mutating the
            // the schema in strange ways. This approach is less complex but ensures everything has a attribute
            // without conflicting with the schema

            //Create a new attribute type
            AttributeType.DataType<?> dataType = gen(AttributeType.DataType.class);
            Label label = genFromTx(Labels.class).mustBeUnused().generate(random, status);
            AttributeType attributeType = tx().putAttributeType(label, dataType);

            //Create new attribute
            Attribute attribute = newResource(attributeType);

            //Link everything together
            type.attribute(attributeType);
            thing.attribute(attribute);
        }

        return thing;
    }

    protected Attribute newResource(AttributeType type) {
        AttributeType.DataType<?> dataType = type.getDataType();
        Object value = gen().make(ResourceValues.class).dataType(dataType).generate(random, status);
        return type.putAttribute(value);
    }

    @SuppressWarnings("unused") /**Used through annotation {@link NonMeta}*/
    public final void configure(@SuppressWarnings("unused") NonMeta nonMeta) {
    }

    @SuppressWarnings("unused") /**Used through annotation {@link WithResource}*/
    public final void configure(@SuppressWarnings("unused") WithResource withResource) {
        this.withResource = true;
    }

    protected abstract T newInstance(S type);

    /**
     * Specify if the generated {@link Thing} should be connected to a {@link Attribute}
     */
    @Target({PARAMETER, FIELD, ANNOTATION_TYPE, TYPE_USE})
    @Retention(RUNTIME)
    @GeneratorConfiguration
    public @interface WithResource {

    }
}
