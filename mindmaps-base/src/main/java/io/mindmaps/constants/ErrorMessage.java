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

package io.mindmaps.constants;

public enum ErrorMessage {
    //--------------------------------------------- Core Errors -----------------------------------------------
    NO_EDGE("Concept ['%s'] does not have any edges to a ['%s']"),
    CANNOT_DELETE("Concept ['%s'] cannot be deleted as it still has edges"),
    LOOP_DETECTED("Concept ['%s'] loops when following edges of type ['%s']"),
    MORE_THAN_ONE_EDGE("Concept ['%s'] has more than one edge of type ['%s']"),
    ID_NOT_UNIQUE("Failed to change the Id of Concept ['%s'] due to another concept already having an id of " +
            "type ['%s'] with value ['%s']"),
    ID_ALREADY_TAKEN("The id ['%s'] is already taken by concept ['%s']"),
    TOO_MANY_CONCEPTS("Too many concepts found for key ['%s'] and value ['%s']"),
    TOO_MANY_CASTINGS("More than one casting found between Role ['%s'] and Instance ['%s']"),
    INVALID_DATATYPE("The value ['%s'] on concept ['%s'] must be of datatype ['%s']"),
    INVALID_OBJECT_TYPE("The concept ['%s'] is not of type ['%s']"),
    REGEX_INSTANCE_FAILURE("The regex ['%s'] cannot be applied because instance ['%s'] of the Resource Type does not " +
            "conform to the regular expressions"),
    REGEX_NOT_STRING("The Resource Type ['%s'] is not of type String so it cannot support regular expressions"),
    RELATION_EXISTS("The Relation with the provided role players already exists ['%s']."),
    ID_RESERVED("The ID ['%s'] is reserved and cannot be used"),
    ROLE_IS_NULL("The provided role cannot be null with roleplayer ['%s']"),
    RESOURCE_CANNOT_HAVE_VALUE("The value ['%s'] cannot be set to the resource ['%s'] because its type is set to be " +
            "unique and another resource ['%s'] already has that value. "),
    RESOURCE_TYPE_CANNOT_BE_UNIQUE("The resource type ['%s'] cannot be set to unique because it's resources have " +
            "duplicate values"),
    INVALID_EDGE("The edge ['%s'] between concept ['%s'] and ['%s'] is invalid"),
    NOT_SUPPORTED("Not supported with a ['%s'] backend"),
    CLOSED("The ['%s'] has been permanently closed"),
    TINKERGRAPH_WARNING("Tinkergraph does not actually support commits. The graph was only validated"),

    //--------------------------------------------- Validation Errors
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

    //--------------------------------------------- Factory Errors
    INVALID_PATH_TO_CONFIG("Unable to open config file ['%s']"),
    CREATING_ONTOLOGY_ERROR("Unable to create a new meta ontology due to ['%s']"),
    INVALID_COMPUTER("The graph computer ['%s'] is not supported"),

    //--------------------------------------------- Client Errors
    INVALID_ENGINE_RESPONSE("Mindmaps Engine located at ['%s'] returned response ['%s'], cannot proceed."),
    INVALID_FACTORY("Graph Factory ['%s'] is not valid"),
    MISSING_FACTORY_DEFINITION("Graph Factor Config ['factory.internal'] missing from provided config. " +
            "Cannot produce graph"),
    CONFIG_NOT_FOUND("Mindmaps Engine located at ['%s'] did not return the expected response, due to ['%s']"),

    //--------------------------------------------- Graql Errors -----------------------------------------------
    NO_TRANSACTION("no transaction provided"),

    SYNTAX_ERROR("syntax error"),

    MUST_BE_RESOURCE_TYPE("type '%s' must be a resource-type"),
    ID_NOT_FOUND("id '%s' not found"),
    NOT_A_ROLE_TYPE("'%s' is not a role type. perhaps you meant 'isa %s'?"),
    NOT_A_RELATION_TYPE("'%s' is not a relation type. perhaps you forgot to separate your statements with a ';'?"),
    NOT_ROLE_IN_RELATION("'%s' is not a valid role type for relation type '%s'. valid role types are: '%s'"),
    MULTIPLE_TYPES("%s have been given multiple types: %s and %s. " +
            "perhaps you forgot to separate your statements with a ';'?"),
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
    INSERT_TYPE_WITHOUT_ID("attempted to insert a type without an id"),

    DELETE_VALUE("deleting values is not supported"),

    //--------------------------------------------- Engine Errors -----------------------------------------------
    NO_CONFIG_FILE("Cannot find config file [%s]"),
    PARSING_EXCEPTION("Parsing exception caused by [%s]"),
    CONCEPT_ID_NOT_FOUND("ID [%s] not found in the graph."),
    FAILED_TRANSACTION("Could not commit to graph after %s retries"),
    FAILED_VALIDATION("Caught exception during validation: %s"),
    NO_PARAMETER_PROVIDED("Required Parameter [%s] is missing for this [%s] operation"),

    //Post processing Errors
    CONCEPT_POSTPROCESSING("Concept [%s] of type [%s] is doe not have any post-processing steps"),
    POSTPROCESSING_ERROR("Unexpected error during %s fix due to [%s]"),
    UNABLE_TO_ANALYSE_CONCEPT("Unable to analyse concept [%s] due to error [%s]"),
    BACK_OFF_RETRY("Unexpected failure performing backoff and retry of [%s]S"),
    //Distributed loading Errors
    ERROR_IN_DISTRIBUTED_TRANSACTION("Error while sending transaction to host: [%s]. Code: [%s] Message:[%s] \n Transaction string: [%s] ");

    private final String message;

    ErrorMessage(String message) {
        this.message = message;
    }

    public String getMessage(Object... args) {
        return String.format(message, args);
    }
}
