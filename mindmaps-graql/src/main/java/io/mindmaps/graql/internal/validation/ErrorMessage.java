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

package io.mindmaps.graql.internal.validation;

/**
 * Error messages, stored as strings which should be formatted with String.format
 */
@SuppressWarnings("JavaDoc")
public enum ErrorMessage {

    NO_TRANSACTION("no transaction provided"),

    SYNTAX_ERROR("syntax error"),

    MUST_BE_RESOURCE_TYPE("type '%s' must be a resource-type"),
    ID_NOT_FOUND("id '%s' not found"),
    NOT_A_ROLE_TYPE("'%s' is not a role type. perhaps you meant 'isa %s'?"),
    NOT_A_RELATION_TYPE("'%s' is not a relation type. perhaps you forgot to separate your statements with a ';'?"),
    NOT_ROLE_IN_RELATION("'%s' is not a valid role type for relation type '%s'. valid role types are: '%s'"),
    MULTIPLE_TYPES(
            "%s have been given multiple types: %s and %s. " +
            "perhaps you forgot to separate your statements with a ';'?"
    ),
    MULTIPLE_IDS("a concept cannot have multiple ids: %s and %s"),
    SET_GENERATED_VARIABLE_NAME("cannot set variable name '%s' on a variable without a user-defined name"),
    INSTANCE_OF_ROLE_TYPE("cannot get instances of role type %s"),

    SELECT_NONE_SELECTED("no variables have been selected. at least one variable must be selected"),
    MATCH_NO_PATTERNS("no patterns have been provided in match query. at least one pattern must be provided"),
    SELECT_VAR_NOT_IN_MATCH("$%s does not appear in match query"),

    INSERT_GET_NON_EXISTENT_ID("no concept with id '%s' exists"),
    INSERT_UNDEFINED_VARIABLE("$%s doesn't exist and doesn't have an 'isa' or an 'ako'"),
    INSERT_PREDICATE("cannot insert a concept with a predicate"),
    INSERT_RELATION_WITH_ID("a relation cannot have an id"),
    INSERT_RELATION_WITHOUT_ISA("cannot insert a relation without an isa edge"),
    INSERT_MULTIPLE_VALUES("a concept cannot have multiple values %s and '%s'"),
    INSERT_ISA_AND_AKO("cannot insert %s with an isa and an ako"),
    INSERT_NO_DATATYPE("resource type %s must have a datatype defined"),
    INSERT_NO_RESOURCE_RELATION("type %s cannot have resource type %s"),
    INSERT_METATYPE("%s cannot be a subtype of meta-type %s"),
    INSERT_RECURSIVE("%s should not refer to itself"),

    DELETE_VALUE("deleting values is not supported");

    private final String message;

    /**
     * @param message the error message string, with parameters defined using %s
     */
    ErrorMessage(String message) {
        this.message = message;
    }

    /**
     * @param args arguments to substitute into the message
     * @return the error message string, with arguments substituted
     */
    public String getMessage(Object... args) {
        return String.format(message, args);
    }
}
