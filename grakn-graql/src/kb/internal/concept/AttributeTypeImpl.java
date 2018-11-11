/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.kb.internal.concept;

import grakn.core.concept.Attribute;
import grakn.core.concept.AttributeType;
import grakn.core.exception.GraknTxOperationException;
import grakn.core.kb.internal.structure.VertexElement;
import grakn.core.util.Schema;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 *     An ontological element which models and categorises the various {@link Attribute} in the graph.
 * </p>
 *
 * <p>
 *     This ontological element behaves similarly to {@link grakn.core.concept.Type} when defining how it relates to other
 *     types. It has two additional functions to be aware of:
 *     1. It has a {@link AttributeType.DataType} constraining the data types of the values it's instances may take.
 *     2. Any of it's instances are unique to the type.
 *     For example if you have a {@link AttributeType} modelling month throughout the year there can only be one January.
 * </p>
 *
 *
 * @param <D> The data type of this resource type.
 *           Supported Types include: {@link String}, {@link Long}, {@link Double}, and {@link Boolean}
 */
public class AttributeTypeImpl<D> extends TypeImpl<AttributeType<D>, Attribute<D>> implements AttributeType<D> {
    private AttributeTypeImpl(VertexElement vertexElement) {
        super(vertexElement);
    }

    private AttributeTypeImpl(VertexElement vertexElement, AttributeType<D> type, DataType<D> dataType) {
        super(vertexElement, type);
        vertex().propertyImmutable(Schema.VertexProperty.DATA_TYPE, dataType, dataType(), DataType::getName);
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
        ((AttributeTypeImpl<D>) superType).sups().forEach(st -> checkInstancesMatchRegex(st.regex()));
        return super.sup(superType);
    }

    /**
     * @param regex The regular expression which instances of this resource must conform to.
     * @return The {@link AttributeType} itself.
     */
    @Override
    public AttributeType<D> regex(String regex) {
        if(dataType() == null || !dataType().equals(DataType.STRING)) {
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
                String value = (String) resource.value();
                Matcher matcher = pattern.matcher(value);
                if(!matcher.matches()){
                    throw GraknTxOperationException.regexFailure(this, value, regex);
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Attribute<D> create(D value) {
        return putAttribute(value, false);
    }

    public Attribute<D> putAttributeInferred(D value) {
        return putAttribute(value, true);
    }

    private Attribute<D> putAttribute(D value, boolean isInferred) {
        Objects.requireNonNull(value);

        BiFunction<VertexElement, AttributeType<D>, Attribute<D>> instanceBuilder = (vertex, type) -> {
            if(dataType().equals(DataType.STRING)) checkConformsToRegexes(value);
            return vertex().tx().factory().buildAttribute(vertex, type, value);
        };

        return putInstance(Schema.BaseType.ATTRIBUTE, () -> attribute(value), instanceBuilder, isInferred);
    }

    /**
     * Utility method used to create or find an instance of this type
     *
     * @param instanceBaseType The base type of the instances of this type
     * @param finder The method to find the instrance if it already exists
     * @param producer The factory method to produce the instance if it doesn't exist
     * @return A new or already existing instance
     */
    private Attribute<D> putInstance(Schema.BaseType instanceBaseType, Supplier<Attribute<D>> finder, BiFunction<VertexElement, AttributeType<D>, Attribute<D>> producer, boolean isInferred) {
        Attribute<D> instance = finder.get();
        if(instance == null) {
            instance = addInstance(instanceBaseType, producer, isInferred);
        } else {
            if(isInferred && !instance.isInferred()){
                throw GraknTxOperationException.nonInferredThingExists(instance);
            }
        }
        return instance;
    }



    /**
     * Checks if all the regex's of the types of this resource conforms to the value provided.
     *
     * @throws GraknTxOperationException when the value does not conform to the regex of its types
     * @param value The value to check the regexes against.
     */
    private void checkConformsToRegexes(D value){
        //Not checking the datatype because the regex will always be null for non strings.
        this.sups().forEach(sup -> {
            String regex = sup.regex();
            if (regex != null && !Pattern.matches(regex, (String) value)) {
                throw GraknTxOperationException.regexFailure(this, (String) value, regex);
            }
        });
    }

    @Override
    public Attribute<D> attribute(D value) {
        String index = Schema.generateAttributeIndex(label(), value.toString());
        return vertex().tx().<Attribute<D>>getConcept(Schema.VertexProperty.INDEX, index).orElse(null);
    }

    /**
     * @return The data type which instances of this resource must conform to.
     */
    //This unsafe cast is suppressed because at this stage we do not know what the type is when reading from the rootGraph.
    @SuppressWarnings({"unchecked", "SuspiciousMethodCalls"})
    @Override
    public DataType<D> dataType() {
        return (DataType<D>) DataType.SUPPORTED_TYPES.get(vertex().property(Schema.VertexProperty.DATA_TYPE));
    }

    /**
     * @return The regular expression which instances of this resource must conform to.
     */
    @Override
    public String regex() {
        return vertex().property(Schema.VertexProperty.REGEX);
    }

    public static AttributeTypeImpl from(AttributeType attributeType){
        return (AttributeTypeImpl) attributeType;
    }

}
