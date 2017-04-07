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

package ai.grakn.graql.internal.analytics;

import ai.grakn.concept.ResourceType;
import ai.grakn.concept.TypeLabel;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Some helper methods for MapReduce and vertex program.
 * <p>
 *
 * @author Jason Liu
 * @author Sheldon Hall
 */

public class Utility {
    /**
     * The Grakn type property on a given Tinkerpop vertex.
     *
     * @param vertex the Tinkerpop vertex
     * @return the type
     */
    static TypeLabel getVertexType(Vertex vertex) {
        return TypeLabel.of(vertex.value(Schema.ConceptProperty.TYPE.name()));
    }

    /**
     * Whether the Tinkerpop vertex has a Grakn type property reserved for analytics.
     *
     * @param vertex the Tinkerpop vertex
     * @return if the type is reserved or not
     */
    static boolean isAnalyticsElement(Vertex vertex) {
        return CommonOLAP.analyticsElements.contains(getVertexType(vertex));
    }

    /**
     * The state of the vertex in the database. This may detect ghost nodes and allow them to be excluded from
     * computations. If the vertex is alive it is likely to be a valid Grakn concept.
     *
     * @return if the vertex is alive
     */
    static boolean isAlive(Vertex vertex) {
        if (vertex == null) return false;

        try {
            return vertex.property(Schema.BaseType.TYPE.name()).isPresent();
        } catch (IllegalStateException e) {
            return false;
        }
    }

    /**
     * Converts from the java data type to the grakn data type. Some map reduce methods require the grakn resource
     * datatype instead of the java datatype as arguments to the constructor. This method is a simple translator for
     * longs and doubles.
     *
     * @param resourceDataType either java.int.long or java.int.double
     * @return the grakn resource data type
     */
    static String graknJavaTypeConverter(String resourceDataType) {
        return resourceDataType.equals(ResourceType.DataType.LONG.getName()) ?
                Schema.ConceptProperty.VALUE_LONG.name() : Schema.ConceptProperty.VALUE_DOUBLE.name();
    }

    /**
     * A helper method for set MapReduce. It simply combines sets into one set.
     *
     * @param values the aggregated values associated with the key
     * @param <T>    the type of the set
     * @return the combined set
     */
    static <T> Set<T> reduceSet(Iterator<Set<T>> values) {
        Set<T> set = new HashSet<>();
        while (values.hasNext()) {
            set.addAll(values.next());
        }
        return set;
    }

    /**
     * Transforms an iterator of key-value pairs into a map
     *
     * @param keyValues an iterator of key-value pairs
     * @param <K>       the type of the keys
     * @param <V>       the type of the values
     * @return the resulting map
     */
    static <K, V> Map<K, V> keyValuesToMap(Iterator<KeyValue<K, V>> keyValues) {
        Map<K, V> map = new HashMap<>();
        keyValues.forEachRemaining(pair -> map.put(pair.getKey(), pair.getValue()));
        return map;
    }
}
