/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.concept.impl;

import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.concept.manager.ConceptNotificationChannel;
import grakn.core.kb.concept.structure.VertexElement;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An ontological element which models and categorises the various Attribute in the graph.
 * This ontological element behaves similarly to Type when defining how it relates to other
 * types. It has two additional functions to be aware of:
 * 1. It has a AttributeType.DataType constraining the data types of the values it's instances may take.
 * 2. Any of it's instances are unique to the type.
 * For example if you have a AttributeType modelling month throughout the year there can only be one January.
 *
 * @param <D> The data type of this resource type.
 *            Supported Types include: String, Long, Double, and Boolean
 */
public class AttributeTypeImpl<D> extends TypeImpl<AttributeType<D>, Attribute<D>> implements AttributeType<D> {
    public AttributeTypeImpl(VertexElement vertexElement, ConceptManager conceptManager, ConceptNotificationChannel conceptNotificationChannel) {
        super(vertexElement, conceptManager, conceptNotificationChannel);
    }

    public static AttributeTypeImpl from(AttributeType attributeType) {
        return (AttributeTypeImpl) attributeType;
    }

    /**
     * This method is overridden so that we can check that the regex of the new super type (if it has a regex)
     * can be applied to all the existing instances.
     */
    @Override
    public AttributeType<D> sup(AttributeType<D> superType) {
        ((AttributeTypeImpl<D>) superType).sups().forEach(st -> checkInstancesMatchRegex(st.regex()));
        return super.sup(superType);
    }

    /**
     * @param regex The regular expression which instances of this resource must conform to.
     * @return The AttributeType itself.
     */
    @Override
    public AttributeType<D> regex(String regex) {
        if (dataType() == null || !dataType().equals(DataType.STRING)) {
            throw GraknConceptException.cannotSetRegex(this);
        }

        checkInstancesMatchRegex(regex);

        return property(Schema.VertexProperty.REGEX, regex);
    }

    /**
     * Checks that existing instances match the provided regex.
     *
     * @param regex The regex to check against
     * @throws GraknConceptException when an instance does not match the provided regex
     */
    private void checkInstancesMatchRegex(@Nullable String regex) {
        if (regex != null) {
            Pattern pattern = Pattern.compile(regex);
            instances().forEach(resource -> {
                String value = (String) resource.value();
                Matcher matcher = pattern.matcher(value);
                if (!matcher.matches()) {
                    throw GraknConceptException.regexFailure(this, value, regex);
                }
            });
        }
    }

    @Override
    public Attribute<D> create(D value) {
        return putAttribute(value, false);
    }

    @Override
    public Attribute<D> putAttributeInferred(D value) {
        return putAttribute(value, true);
    }

    /**
     * Method through which all new instance creations of attributes pass through
     */
    private Attribute<D> putAttribute(D value, boolean isInferred) {
        Objects.requireNonNull(value);

        if (dataType().equals(DataType.STRING)) checkConformsToRegexes((String) value);

        Attribute<D> instance = getAttribute(value);
        if (instance == null) {
            // create a brand new vertex and concept
            instance = conceptManager.createAttribute(this, value, isInferred);
        } else {
            if (isInferred && !instance.isInferred()) {
                throw GraknConceptException.nonInferredThingExists(instance);
            }
        }
        return instance;
    }


    /**
     * Checks if all the regex's of the types of this resource conforms to the value provided.
     *
     * @param value The value to check the regexes against.
     * @throws GraknConceptException when the value does not conform to the regex of its types
     */
    private void checkConformsToRegexes(String value) {
        //Not checking the datatype because the regex will always be null for non strings.
        this.sups().forEach(sup -> {
            String regex = sup.regex();
            if (regex != null && !Pattern.matches(regex, value)) {
                throw GraknConceptException.regexFailure(this, value, regex);
            }
        });
    }

    @Override
    @Nullable
    public Attribute<D> attribute(D value) {
        String index = Schema.generateAttributeIndex(label(), value.toString());
        Attribute<D> concept = conceptManager.getCachedAttribute(index);
        if (concept != null) return concept;
        return conceptManager.getConcept(Schema.VertexProperty.INDEX, index);
    }

    /**
     * This is only used when checking if attribute exists before trying to create a new one.
     */
    private Attribute<D> getAttribute(D value) {
        String index = Schema.generateAttributeIndex(label(), value.toString());
        return conceptManager.getAttribute(index);
    }

    /**
     * @return The data type which instances of this resource must conform to.
     */
    //This unsafe cast is suppressed because at this stage we do not know what the type is when reading from the rootGraph.
    @SuppressWarnings({"unchecked"})
    @Nullable
    @Override
    public DataType<D> dataType() {
        String className = vertex().property(Schema.VertexProperty.DATA_TYPE);
        if (className == null) return null;

        try {
            return (DataType<D>) DataType.of(Class.forName(className));
        } catch (ClassNotFoundException e) {
            throw GraknConceptException.unsupportedDataType(className);
        }
    }

    /**
     * @return The regular expression which instances of this resource must conform to.
     */
    @Override
    public String regex() {
        return vertex().property(Schema.VertexProperty.REGEX);
    }

    @Override
    void trackRolePlayers() {
        conceptNotificationChannel.trackAttributeInstancesRolesPlayed(this);
    }
}
