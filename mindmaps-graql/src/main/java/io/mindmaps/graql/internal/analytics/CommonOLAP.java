/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.analytics;

import io.mindmaps.util.Schema;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.*;

/**
 * Core Mindmaps implementation of the common methods on the MapReduce and VertexProgram interfaces.
 */
abstract class CommonOLAP {

    private static final String PREFIX_TYPE_KEY = "TYPE";
    private static final String PREFIX_SELECTED_TYPE_KEY = "SELECTED_TYPE";
    private static final String PREFIX_PERSISTENT_PROPERTIES = "PERSISTENT";

    /**
     * The concepts that can be "seen" by analytics by default.
     */
    Set<String> baseTypes = new HashSet<>();

    /**
     * The types that define a subgraph.
     */
    Set<String> selectedTypes = new HashSet<>();

    /**
     * Properties that will be reloaded whenever the class is instantiated in a spark executor.
     */
    Map<String,Object> persistentProperties = new HashMap<>();


    /**
     * Store <code>persistentProperties</code> and any hard coded fields in an apache config object for propagation to
     * spark executors.
     *
     * @param configuration the apache config object that will be propagated
     */
    public void storeState(final Configuration configuration) {
        // store baseTypes
        configuration.addProperty(PREFIX_TYPE_KEY +"."+Schema.BaseType.ENTITY.name(), Schema.BaseType.ENTITY.name());
        configuration.addProperty(PREFIX_TYPE_KEY +"."+Schema.BaseType.RELATION.name(), Schema.BaseType.RELATION.name());
        configuration.addProperty(PREFIX_TYPE_KEY +"."+Schema.BaseType.RESOURCE.name(), Schema.BaseType.RESOURCE.name());

        // store selectedTypes
        selectedTypes.forEach(typeId -> configuration.addProperty(PREFIX_SELECTED_TYPE_KEY+"."+typeId,typeId));

        // store fields

        // store user specified properties
        persistentProperties.forEach((key, value) ->
                configuration.addProperty(PREFIX_PERSISTENT_PROPERTIES + "." + key, value));
    }

    /**
     * Load <code>persistentProperties</code> and any hard coded fields from an apache config object for use by the
     * spark executor.
     *
     * @param graph
     * @param configuration the apache config object containing the values
     */
    public void loadState(final Graph graph, final Configuration configuration) {
        // load baseTypes
        configuration.subset(PREFIX_TYPE_KEY).getKeys().forEachRemaining(key ->
                baseTypes.add(configuration.getString(PREFIX_TYPE_KEY+"."+key)));

        // load selected types
        configuration.subset(PREFIX_SELECTED_TYPE_KEY).getKeys().forEachRemaining(key ->
                selectedTypes.add(configuration.getString(PREFIX_SELECTED_TYPE_KEY+"."+key)));

        // load fields

        // load user specified properties
        configuration.subset(PREFIX_PERSISTENT_PROPERTIES).getKeys().forEachRemaining(key ->
                persistentProperties.put(key,configuration.getProperty(PREFIX_PERSISTENT_PROPERTIES + "." + key)));
    }

}
