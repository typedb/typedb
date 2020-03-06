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
 */

package grakn.core.graql.analytics;

import com.google.common.collect.Sets;
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.LabelId;
import grakn.core.kb.concept.manager.ConceptManager;
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
 */

public class Utility {
    /**
     * The Grakn type property on a given Tinkerpop vertex.
     * If the vertex is a SchemaConcept, return invalid Label.
     *
     * @param vertex the Tinkerpop vertex
     * @return the type
     */
    static LabelId getVertexTypeId(Vertex vertex) {
        if (vertex.property(Schema.VertexProperty.THING_TYPE_LABEL_ID.name()).isPresent()) {
            return LabelId.of(vertex.value(Schema.VertexProperty.THING_TYPE_LABEL_ID.name()));
        }
        return LabelId.invalid();
    }

    static boolean vertexHasSelectedTypeId(Vertex vertex, Set<LabelId> selectedTypeIds) {
        return vertex.property(Schema.VertexProperty.THING_TYPE_LABEL_ID.name()).isPresent() &&
                selectedTypeIds.contains(LabelId.of(vertex.value(Schema.VertexProperty.THING_TYPE_LABEL_ID.name())));
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
            return Sets.newHashSet(Schema.BaseType.values()).contains(Schema.BaseType.valueOf(vertex.label()));
        } catch (IllegalStateException e) {
            return false;
        }
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

    /**
     * Check whether it is possible that there is a resource edge between the two given concepts.
     */
    public static boolean mayHaveResourceEdge(ConceptManager conceptManager, ConceptId conceptId1, ConceptId conceptId2) {
        Concept concept1 = conceptManager.getConcept(conceptId1);
        Concept concept2 = conceptManager.getConcept(conceptId2);
        return concept1 != null && concept2 != null && (concept1.isAttribute() || concept2.isAttribute());
    }


}
