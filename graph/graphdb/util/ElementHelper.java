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

package grakn.core.graph.graphdb.util;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import grakn.core.graph.core.JanusGraphElement;
import grakn.core.graph.core.JanusGraphRelation;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.JanusGraphVertexProperty;
import grakn.core.graph.core.PropertyKey;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;

import javax.annotation.Nullable;
import java.util.Collections;

public class ElementHelper {

    public static Iterable<Object> getValues(JanusGraphElement element, PropertyKey key) {
        if (element instanceof JanusGraphRelation) {
            Object value = element.valueOrNull(key);
            if (value == null) return Collections.EMPTY_LIST;
            else return ImmutableList.of(value);
        } else {
            return Iterables.transform((((JanusGraphVertex) element).query()).keys(key.name()).properties(), new Function<JanusGraphVertexProperty, Object>() {
                @Nullable
                @Override
                public Object apply(JanusGraphVertexProperty janusgraphProperty) {
                    return janusgraphProperty.value();
                }
            });
        }
    }

    public static void attachProperties(JanusGraphRelation element, Object... keyValues) {
        if (keyValues == null || keyValues.length == 0) return; //Do nothing
        org.apache.tinkerpop.gremlin.structure.util.ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (org.apache.tinkerpop.gremlin.structure.util.ElementHelper.getIdValue(keyValues).isPresent()) {
            throw Edge.Exceptions.userSuppliedIdsNotSupported();
        }
        if (org.apache.tinkerpop.gremlin.structure.util.ElementHelper.getLabelValue(keyValues).isPresent()) {
            throw new IllegalArgumentException("Cannot provide label as argument");
        }
        org.apache.tinkerpop.gremlin.structure.util.ElementHelper.attachProperties(element, keyValues);
    }

    /**
     * This is essentially an adjusted copy paste from TinkerPop's ElementHelper class.
     * The reason for copying it is so that we can determine the cardinality of a property key based on
     * JanusGraph's schema which is tied to this particular transaction and not the graph.
     */
    public static void attachProperties(JanusGraphVertex vertex, Object... propertyKeyValues) {
        for (int i = 0; i < propertyKeyValues.length; i = i + 2) {
            if (!propertyKeyValues[i].equals(T.id) && !propertyKeyValues[i].equals(T.label)) {
                vertex.property((String) propertyKeyValues[i], propertyKeyValues[i + 1]);
            }
        }
    }

}
