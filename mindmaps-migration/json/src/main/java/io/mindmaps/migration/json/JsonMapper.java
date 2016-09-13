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

package io.mindmaps.migration.json;

import io.mindmaps.concept.Type;
import io.mindmaps.graql.internal.util.GraqlType;

/**
 * Class for mapping certain strings from JSON schema to a mindmaps ontology
 */
class JsonMapper {

    private static final String RELATION_NAME = "has-%s";
    private static final String ROLE_OWNER = "%s-owner";
    private static final String ROLE_OTHER = "%s-role";

    private static final String SUBTYPE_NAME = "%s-%s";
    private static final String ARRAY_ITEM = "%s-item";

    /**
     * Get the name of a sub-concept-type which is specialized to represent a particular JSON type
     */
    static String subtypeName(Type conceptType, JsonType jsonType) {
        return String.format(SUBTYPE_NAME, conceptType.getId(), jsonType.getName());
    }

    /**
     * Get the name of a type representing items of a JSON array
     */
    static String arrayItemName(Type type) {
        return String.format(ARRAY_ITEM, type.getId());
    }

    /**
     * Get the name of a relation relating something to the given type
     */
    static String relationName(Type type) {
        return String.format(RELATION_NAME, type.getId());
    }

    /**
     * Get the name of the role the owner of the given type will play
     */
    static String roleOwnerName(Type type) {
        return String.format(ROLE_OWNER, type.getId());
    }

    /**
     * Get the name of the role the given type will play when it is contained in another type
     */
    static String roleOtherName(Type type) {
        return String.format(ROLE_OTHER, type.getId());
    }
    /**
     * Get the name of a relation relating something to the given type
     */
    static String resourceRelationName(Type type) {
        return GraqlType.HAS_RESOURCE.getId(type.getId());
    }

    /**
     * Get the name of the role the owner of the given type will play
     */
    static String roleOwnerResourceName(Type type) {
        return GraqlType.HAS_RESOURCE_OWNER.getId(type.getId());
    }

    /**
     * Get the name of the role the given type will play when it is contained in another type
     */
    static String roleOtherResourceName(Type type) {
        return GraqlType.HAS_RESOURCE_VALUE.getId(type.getId());
    }
}
