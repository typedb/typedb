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

package io.mindmaps.migration.sql;

import java.util.Collection;
import java.util.stream.Collectors;

public interface Namer {

    String RESOURCE_NAME = "%s-%s-resource";
    String RELATION = "%s-relation";
    String ROLE_CHILD = "%s-child";
    String ROLE_PARENT = "%s-parent";
    String PRIMARY_KEY = "%s-";

    /**
     * Get the name of a relation between an object and a resource
     */
    default String resourceName(String tableName, String type) {
        return String.format(RESOURCE_NAME, tableName, type);
    }

    /**
     * Get the name of a relation relating something to the given type
     */
    default String relationName(String type) {
        return String.format(RELATION, type);
    }

    /**
     * Get the name of the role the owner of the given type will play
     */
    default String roleParentName(String type) {
        return String.format(ROLE_PARENT, type);
    }

    /**
     * Get the name of the role the given type will play when it is contained in another type
     */
    default String roleChildName(String type) {
        return String.format(ROLE_CHILD, type);
    }

    /**
     * Format the primary key given a table type and collection of primary key values.
     */
    default String primaryKey(String type, Collection<String> values){
        return String.format(PRIMARY_KEY, type) + values.stream().collect(Collectors.joining());
    }
}
