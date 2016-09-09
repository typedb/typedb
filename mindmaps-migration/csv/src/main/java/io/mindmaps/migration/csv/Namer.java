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

package io.mindmaps.migration.csv;

/**
 * A Namer provides functionality for mapping from CSV table and column names to the corresponding elements in a
 * Mindmaps ontology. This interface provides default implementations.
 */
public interface Namer {

    static final String RESOURCE_NAME = "%s-resource";

    /**
     * Generate the name of the resource. Appending "resource" avoids conflicts with entity names.
     */
    default String resourceName(String type) {
        return String.format(RESOURCE_NAME, type);
    }
}
