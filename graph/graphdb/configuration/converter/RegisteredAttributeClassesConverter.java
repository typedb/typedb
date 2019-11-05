/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graph.graphdb.configuration.converter;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.attribute.AttributeSerializer;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration;
import grakn.core.graph.graphdb.configuration.RegisteredAttributeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Converter which converts {@link Configuration} into a List of {@link RegisteredAttributeClass}
 */
public class RegisteredAttributeClassesConverter {

    private static RegisteredAttributeClassesConverter registeredAttributeClassesConverter;

    private RegisteredAttributeClassesConverter() {
    }

    public static RegisteredAttributeClassesConverter getInstance() {
        if (registeredAttributeClassesConverter == null) {
            registeredAttributeClassesConverter = new RegisteredAttributeClassesConverter();
        }
        return registeredAttributeClassesConverter;
    }

    public List<RegisteredAttributeClass<?>> convert(Configuration configuration) {
        Set<String> attributeIds = configuration.getContainedNamespaces(GraphDatabaseConfiguration.CUSTOM_ATTRIBUTE_NS);
        List<RegisteredAttributeClass<?>> all = new ArrayList<>(attributeIds.size());

        for (String attributeId : attributeIds) {
            final int position = getAttributePosition(attributeId);
            final Class<?> clazz = getAttributeClass(configuration, attributeId);
            final AttributeSerializer<?> serializer = getAttributeSerializer(configuration, attributeId);

            RegisteredAttributeClass reg = new RegisteredAttributeClass(position, clazz, serializer);
            if (all.contains(reg)) {
                throw new IllegalArgumentException("Duplicate attribute registration: " + reg);
            }
            all.add(reg);
        }

        return all;
    }

    private int getAttributePosition(String attributeId) {
        Preconditions.checkArgument(attributeId.startsWith(GraphDatabaseConfiguration.ATTRIBUTE_PREFIX),
                "Invalid attribute definition: %s", attributeId);
        try {
            return Integer.parseInt(attributeId.substring(GraphDatabaseConfiguration.ATTRIBUTE_PREFIX.length()));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Expected entry of the form [" +
                    GraphDatabaseConfiguration.ATTRIBUTE_PREFIX + "X] where X is a number but given " + attributeId);
        }
    }

    private Class<?> getAttributeClass(Configuration configuration, String attributeId) {
        String classname = configuration.get(GraphDatabaseConfiguration.CUSTOM_ATTRIBUTE_CLASS, attributeId);
        try {
            return Class.forName(classname);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not find attribute class " + classname, e);
        }
    }

    private AttributeSerializer<?> getAttributeSerializer(Configuration configuration, String attributeId) {
        Preconditions.checkArgument(configuration.has(GraphDatabaseConfiguration.CUSTOM_SERIALIZER_CLASS, attributeId));
        String serializerName = configuration.get(GraphDatabaseConfiguration.CUSTOM_SERIALIZER_CLASS, attributeId);
        try {
            Class<?> serializerClass = Class.forName(serializerName);
            return (AttributeSerializer) serializerClass.newInstance();
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not find serializer class " + serializerName);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IllegalArgumentException("Could not instantiate serializer class " + serializerName, e);
        }
    }

}
