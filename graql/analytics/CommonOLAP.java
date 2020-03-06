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

import grakn.core.kb.concept.api.LabelId;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Core Grakn implementation of the common methods on the MapReduce and VertexProgram interfaces.
 * <p>
 *
 */
public abstract class CommonOLAP {
    static final Logger LOGGER = LoggerFactory.getLogger(CommonOLAP.class);

    private static final String PREFIX_SELECTED_TYPE_KEY = "SELECTED_TYPE";
    private static final String PREFIX_PERSISTENT_PROPERTIES = "PERSISTENT";

    /**
     * The types that define a subgraph.
     */
    Set<LabelId> selectedTypes = new HashSet<>();

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
    public void storeState(Configuration configuration) {
        // clear properties from vertex program
        Set<String> oldKeys = new HashSet<>();
        configuration.subset(PREFIX_SELECTED_TYPE_KEY).getKeys()
                .forEachRemaining(key -> oldKeys.add(PREFIX_SELECTED_TYPE_KEY + "." + key));
        oldKeys.forEach(configuration::clearProperty);

        // store selectedTypes
        selectedTypes.forEach(typeId ->
                configuration.addProperty(PREFIX_SELECTED_TYPE_KEY + "." + typeId, typeId));

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
    public void loadState(Graph graph, Configuration configuration) {
        // load selected types
        configuration.subset(PREFIX_SELECTED_TYPE_KEY).getKeys().forEachRemaining(key ->
                selectedTypes.add((LabelId) configuration.getProperty(PREFIX_SELECTED_TYPE_KEY + "." + key)));

        // load user specified properties
        configuration.subset(PREFIX_PERSISTENT_PROPERTIES).getKeys().forEachRemaining(key ->
                persistentProperties.put(key, configuration.getProperty(PREFIX_PERSISTENT_PROPERTIES + "." + key)));
    }

    public String toString() {
        return this.getClass().getSimpleName();
    }

}
