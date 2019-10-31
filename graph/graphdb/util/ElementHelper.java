// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grakn.core.graph.graphdb.util;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.PropertyKey;

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
        if (org.apache.tinkerpop.gremlin.structure.util.ElementHelper.getIdValue(keyValues).isPresent())
            throw Edge.Exceptions.userSuppliedIdsNotSupported();
        if (org.apache.tinkerpop.gremlin.structure.util.ElementHelper.getLabelValue(keyValues).isPresent())
            throw new IllegalArgumentException("Cannot provide label as argument");
        org.apache.tinkerpop.gremlin.structure.util.ElementHelper.attachProperties(element, keyValues);
    }

    /**
     * This is essentially an adjusted copy paste from TinkerPop's ElementHelper class.
     * The reason for copying it is so that we can determine the cardinality of a property key based on
     * JanusGraph's schema which is tied to this particular transaction and not the graph.
     *
     */
    public static void attachProperties(JanusGraphVertex vertex, Object... propertyKeyValues) {
        for (int i = 0; i < propertyKeyValues.length; i = i + 2) {
            if (!propertyKeyValues[i].equals(T.id) && !propertyKeyValues[i].equals(T.label))
                vertex.property((String) propertyKeyValues[i], propertyKeyValues[i + 1]);
        }
    }

}
