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

import ai.grakn.concept.TypeLabel;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Core Grakn implementation of the common methods on the MapReduce and VertexProgram interfaces.
 * <p>
 *
 * @author Jason Liu
 * @author Sheldon Hall
 */
public abstract class CommonOLAP {

    private static final String PREFIX_SELECTED_TYPE_KEY = "SELECTED_TYPE";
    private static final String PREFIX_PERSISTENT_PROPERTIES = "PERSISTENT";

    /**
     * The types that are reserved by analytics and are not "seen" by analytics.
     */
    static final Set<TypeLabel> analyticsElements = Collections.unmodifiableSet(Sets.newHashSet(
            Schema.Analytics.DEGREE.getLabel(),
            Schema.ImplicitType.HAS.getLabel(Schema.Analytics.DEGREE.getLabel()),
            Schema.Analytics.CLUSTER.getLabel(),
            Schema.ImplicitType.HAS.getLabel(Schema.Analytics.CLUSTER.getLabel())));

    /**
     * The concepts that can be "seen" by analytics by default.
     */
    final Set<String> baseTypes = Sets.newHashSet(
            Schema.BaseType.ENTITY.name(),
            Schema.BaseType.RELATION.name(),
            Schema.BaseType.RESOURCE.name());

    /**
     * The types that define a subgraph.
     */
    Set<TypeLabel> selectedTypes = new HashSet<>();

    /**
     * Properties that will be reloaded whenever the class is instantiated in a spark executor.
     */
    final Map<String, Object> persistentProperties = new HashMap<>();


    /**
     * Store <code>persistentProperties</code> and any hard coded fields in an apache config object for propagation to
     * spark executors.
     *
     * @param configuration the apache config object that will be propagated
     */
    public void storeState(final Configuration configuration) {
        // clear properties from vertex program
        Set<String> oldKeys = new HashSet<>();
        configuration.subset(PREFIX_SELECTED_TYPE_KEY).getKeys()
                .forEachRemaining(key -> oldKeys.add(PREFIX_SELECTED_TYPE_KEY + "." + key));
        oldKeys.forEach(configuration::clearProperty);

        // store selectedTypes
        selectedTypes.forEach(typeLabel -> configuration.addProperty(PREFIX_SELECTED_TYPE_KEY + "." + typeLabel.getValue(), typeLabel.getValue()));

        // store fields

        // store user specified properties
        persistentProperties.forEach((key, value) ->
                configuration.addProperty(PREFIX_PERSISTENT_PROPERTIES + "." + key, value));
    }

    /**
     * Load <code>persistentProperties</code> and any hard coded fields from an apache config object for use by the
     * spark executor.
     *
     * @param graph         the tinker graph
     * @param configuration the apache config object containing the values
     */
    public void loadState(final Graph graph, final Configuration configuration) {
        // load selected types
        configuration.subset(PREFIX_SELECTED_TYPE_KEY).getKeys().forEachRemaining(key ->
                selectedTypes.add(TypeLabel.of(configuration.getString(PREFIX_SELECTED_TYPE_KEY + "." + key))));

        // load fields

        // load user specified properties
        configuration.subset(PREFIX_PERSISTENT_PROPERTIES).getKeys().forEachRemaining(key ->
                persistentProperties.put(key, configuration.getProperty(PREFIX_PERSISTENT_PROPERTIES + "." + key)));
    }

    public String toString() {
        return this.getClass().getSimpleName();
    }

}
