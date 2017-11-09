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
 */

package ai.grakn.kb.internal.concept;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.PropertyNotUniqueException;
import ai.grakn.kb.internal.structure.VertexElement;
import ai.grakn.util.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 *     An ontological element which models and categorises the various {@link Attribute} in the graph.
 * </p>
 *
 * <p>
 *     This ontological element behaves similarly to {@link ai.grakn.concept.Type} when defining how it relates to other
 *     types. It has two additional functions to be aware of:
 *     1. It has a {@link AttributeType.DataType} constraining the data types of the values it's instances may take.
 *     2. Any of it's instances are unique to the type.
 *     For example if you have a {@link AttributeType} modelling month throughout the year there can only be one January.
 * </p>
 *
 * @author fppt
 *
 * @param <D> The data type of this resource type.
 *           Supported Types include: {@link String}, {@link Long}, {@link Double}, and {@link Boolean}
 */
public class AttributeTypeImpl<D> extends TypeImpl<AttributeType<D>, Attribute<D>> implements AttributeType<D> {
    final Logger LOG = LoggerFactory.getLogger(AttributeTypeImpl.class);

    private AttributeTypeImpl(VertexElement vertexElement) {
        super(vertexElement);
    }

    private AttributeTypeImpl(VertexElement vertexElement, AttributeType<D> type, DataType<D> dataType) {
        super(vertexElement, type);
        vertex().propertyImmutable(Schema.VertexProperty.DATA_TYPE, dataType, getDataType(), DataType::getName);
    }

    public static <D> AttributeTypeImpl<D> get(VertexElement vertexElement){
        return new AttributeTypeImpl<>(vertexElement);
    }

    public static <D> AttributeTypeImpl<D> create(VertexElement vertexElement, AttributeType<D> type, DataType<D> dataType) {
        return new AttributeTypeImpl<>(vertexElement, type, dataType);
    }

    /**
     * This method is overridden so that we can check that the regex of the new super type (if it has a regex)
     * can be applied to all the existing instances.
     */
    @Override
    public AttributeType<D> sup(AttributeType<D> superType){
        ((AttributeTypeImpl<D>) superType).superSet().forEach(st -> checkInstancesMatchRegex(st.getRegex()));
        return super.sup(superType);
    }

    /**
     * @param regex The regular expression which instances of this resource must conform to.
     * @return The {@link AttributeType} itself.
     */
    @Override
    public AttributeType<D> setRegex(String regex) {
        if(getDataType() == null || !getDataType().equals(DataType.STRING)) {
            throw GraknTxOperationException.cannotSetRegex(this);
        }

        checkInstancesMatchRegex(regex);

        return property(Schema.VertexProperty.REGEX, regex);
    }

    /**
     * Checks that existing instances match the provided regex.
     *
     * @throws GraknTxOperationException when an instance does not match the provided regex
     * @param regex The regex to check against
     */
    private void checkInstancesMatchRegex(@Nullable String regex){
        if(regex != null) {
            Pattern pattern = Pattern.compile(regex);
            instances().forEach(resource -> {
                String value = (String) resource.getValue();
                Matcher matcher = pattern.matcher(value);
                if(!matcher.matches()){
                    throw GraknTxOperationException.regexFailure(this, value, regex);
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Attribute<D> putAttribute(D value) {
        Objects.requireNonNull(value);

        BiFunction<VertexElement, AttributeType<D>, Attribute<D>> instanceBuilder = (vertex, type) -> {
            if(getDataType().equals(DataType.STRING)) checkConformsToRegexes(value);
            Object persistenceValue = castValue(value);
            AttributeImpl<D> resource = vertex().tx().factory().buildResource(vertex, type, persistenceValue);

            try{
                resource.vertex().propertyUnique(Schema.VertexProperty.INDEX, Schema.generateAttributeIndex(getLabel(), value.toString()));
            } catch (PropertyNotUniqueException e){
                //This happens when another attribute ends up being created between checking if the attribute exists and
                // creating the actual attribute. In this case we dynamically merge
                resource.delete();
                return getAttribute(value);
            }

            return resource;
        };

        return putInstance(Schema.BaseType.ATTRIBUTE,
                () -> getAttribute(value), instanceBuilder);
    }

    /**
     * This is to handle casting longs and doubles when the type allows for the data type to be a number
     * @param value The value of the resource
     * @return The value casted to the correct type
     */
    private Object castValue(D value){
        AttributeType.DataType<D> dataType = getDataType();
        try {
            if (dataType.equals(AttributeType.DataType.DOUBLE)) {
                return ((Number) value).doubleValue();
            } else if (dataType.equals(AttributeType.DataType.LONG)) {
                if (value instanceof Double) {
                    throw new ClassCastException();
                }
                return ((Number) value).longValue();
            } else {
                return dataType.getPersistenceValue(value);
            }
        } catch (ClassCastException e) {
            throw GraknTxOperationException.invalidResourceValue(value, dataType);
        }
    }

    /**
     * Checks if all the regex's of the types of this resource conforms to the value provided.
     *
     * @throws GraknTxOperationException when the value does not conform to the regex of its types
     * @param value The value to check the regexes against.
     */
    private void checkConformsToRegexes(D value){
        //Not checking the datatype because the regex will always be null for non strings.
        superSet().forEach(sup -> {
            String regex = sup.getRegex();
            if (regex != null && !Pattern.matches(regex, (String) value)) {
                throw GraknTxOperationException.regexFailure(this, (String) value, regex);
            }
        });
    }

    @Override
    public Attribute<D> getAttribute(D value) {
        String index = Schema.generateAttributeIndex(getLabel(), value.toString());
        return vertex().tx().<Attribute<D>>getConcept(Schema.VertexProperty.INDEX, index).orElse(null);
    }

    /**
     * @return The data type which instances of this resource must conform to.
     */
    //This unsafe cast is suppressed because at this stage we do not know what the type is when reading from the rootGraph.
    @SuppressWarnings({"unchecked", "SuspiciousMethodCalls"})
    @Override
    public DataType<D> getDataType() {
        return (DataType<D>) DataType.SUPPORTED_TYPES.get(vertex().property(Schema.VertexProperty.DATA_TYPE));
    }

    /**
     * @return The regular expression which instances of this resource must conform to.
     */
    @Override
    public String getRegex() {
        return vertex().property(Schema.VertexProperty.REGEX);
    }

}
