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

package io.mindmaps.core.exceptions;

public enum ErrorMessage {
    NO_EDGE("Concept ['%s'] does not have any edges to a ['%s']"),
    CANNOT_DELETE("Concept ['%s'] cannot be deleted as it still has edges"),
    LOOP_DETECTED("Concept ['%s'] loops when following edges of type ['%s']"),
    MORE_THAN_ONE_EDGE("Concept ['%s'] has more than one edge of type ['%s']"),
    ID_NOT_UNIQUE("Failed to change the Id of Concept ['%s'] due to another concept already having an id of type ['%s'] with value ['%s']"),
    ID_ALREADY_TAKEN("The id ['%s'] is already taken by concept ['%s']"),
    TOO_MANY_CONCEPTS("Too many concepts found for key ['%s'] and value ['%s']"),
    TOO_MANY_CASTINGS("More than one casting found between Role ['%s'] and Instance ['%s']"),
    INVALID_DATATYPE("The value ['%s'] on concept ['%s'] must be of datatype ['%s']"),
    INVALID_OBJECT_TYPE("The concept ['%s'] is not of type ['%s']"),
    REGEX_INSTANCE_FAILURE("The regex ['%s'] cannot be applied because instance ['%s'] of the Resource Type does not conform to the regular expressions"),
    REGEX_NOT_STRING("The Resource Type ['%s'] is not of type String so it cannot support regular expressions"),
    RELATION_EXISTS("The Relation with the provided role players already exists ['%s']."),
    ID_RESERVED("The ID ['%s'] is reserved and cannot be used"),
    ROLE_IS_NULL("The provided role cannot be null with roleplayer ['%s']"),
    RESOURCE_CANNOT_HAVE_VALUE("The value ['%s'] cannot be set to the resource ['%s'] because its type is set to be unique and " +
            "another resource ['%s'] already has that value. "),
    RESOURCE_TYPE_CANNOT_BE_UNIQUE("The resource type ['%s'] cannot be set to unique because it's resources have duplicate values"),
    INVALID_EDGE("The edge ['%s'] between concept ['%s'] and ['%s'] is invalid"),
    NOT_SUPPORTED("Not supported with a ['%s'] backend"),
    CLOSED("The ['%s'] has been permanently closed"),
    TINKERGRAPH_WARNING("Tinkergraph does not actually support commits. The graph was only validated"),

    //--------------------------------------------- Validation Errors -----------------------------------------------
    VALIDATION("A structural validation error has occurred. Please correct the [`%s`] errors found. \n"),
    VALIDATION_RELATION("The relation ['%s'] has an invalid structure. " +
            "This is either due to having more role players than roles or " +
            "the Relation Type ['%s'] not having the correct has-role connection to one of the provided roles. " +
            "The provided roles('%s'): ['%s']" +
            "The provided role players('%s'): ['%s'] \n"),
    VALIDATION_CASTING("The type ['%s'] of role player ['%s'] is not allowed to play RoleType ['%s'] \n"),
    VALIDATION_IS_ABSTRACT("The abstract Type ['%s'] should not have any instances \n"),
    VALIDATION_ROLE_TYPE("RoleType ['%s'] does not have exactly one has-role connection to any RelationType. \n"),
    VALIDATION_RELATION_TYPE("Relation Type ['%s'] does not have two or more roles \n"),

    //--------------------------------------------- Factory Errors -----------------------------------------------
    INVALID_PATH_TO_CONFIG("Unable to open config file ['%s']"),
    CREATING_ONTOLOGY_ERROR("Unable to create a new meta ontology due to ['%s']"),

    //--------------------------------------------- Client Errors -----------------------------------------------
    INVALID_ENGINE_RESPONSE("Mindmaps Engine located at ['%s'] returned response ['%s'], cannot produce a graph."),
    INVALID_FACTORY("Graph Factory ['%s'] is not valid"),
    MISSING_FACTORY_DEFINITION("Graph Factor Config ['factory.internal'] missing from provided config. Cannot produce graph"),
    CONFIG_NOT_FOUND("Mindmaps Engine located at ['%s'] did not return the config as expected, due to ['%s']");

    private final String message;

    ErrorMessage(String message) {
        this.message = message;
    }

    public String getMessage(Object... args) {
        return String.format(message, args);
    }
}
