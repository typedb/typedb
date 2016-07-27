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

package io.mindmaps.graql.internal;

/**
 * Some constant types that Graql needs to know about.
 * This is currently only the has-resource relationship, which is not automatically added to the ontology
 */
public enum GraqlType {

    /**
     * The id of the generic has-resource relationship, used for attaching resources to instances with the 'has' syntax
     */
    HAS_RESOURCE("has-%s"),

    /**
     * The id of a role in has-resource, played by the owner of the resource
     */
    HAS_RESOURCE_OWNER("has-%s-owner"),

    /**
     * The id of a role in has-resource, played by the resource
     */
    HAS_RESOURCE_VALUE("has-%s-value");

    private final String name;

    private GraqlType(String name) {
        this.name = name;
    }

    public String getId(String resourceTypeId) {
        return String.format(name, resourceTypeId);
    }
}
