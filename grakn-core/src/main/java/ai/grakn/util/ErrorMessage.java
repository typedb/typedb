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

package ai.grakn.util;

import javax.annotation.CheckReturnValue;

/**
 * Enum containing error messages.
 *
 * Each error message contains a single format string, with a method {@link ErrorMessage#getMessage(Object...)} that
 * accepts arguments to be passed to the format string.
 *
 * @author Filipe Teixeira
 */
public enum ErrorMessage {
    //--------------------------------------------- Core Errors -----------------------------------------------
    CANNOT_DELETE("Type [%s] cannot be deleted as it still has incoming edges"),
    SUPER_LOOP_DETECTED("By setting the super of concept [%s] to [%s]. You will be creating a loop. This is prohibited"),
    INVALID_UNIQUE_PROPERTY_MUTATION("Property [%s] of Concept [%s] cannot be changed to [%s] as it is already taken by Concept [%s]"),
    UNIQUE_PROPERTY_TAKEN("Property [%s] with value [%s] is already taken by concept [%s]"),
    TOO_MANY_CONCEPTS("Too many concepts found for key [%s] and value [%s]"),
    INVALID_DATATYPE("The value [%s] must be of datatype [%s]"),
    INVALID_OBJECT_TYPE("The concept [%s] is not of type [%s]"),
    REGEX_INSTANCE_FAILURE("The regex [%s] of Resource Type [%s] cannot be applied because value [%s] " +
            "does not conform to the regular expression"),
    REGEX_NOT_STRING("The Resource Type [%s] is not of type String so it cannot support regular expressions"),
    CLOSED_CLEAR("The session for graph has been closed due to deleting the graph"),
    TRANSACTIONS_NOT_SUPPORTED("The graph backend [%s] does not actually support transactions. The transaction was not %s. The graph was actually effected directly"),
    IMMUTABLE_VALUE("The value [%s] cannot be changed to [%s] due to the property [%s] being immutable"),
    META_TYPE_IMMUTABLE("The meta type [%s] is immutable"),
    SCHEMA_LOCKED("Schema cannot be modified when using a batch loading graph"),
    HAS_INVALID("The type [%s] is not allowed to have a resource of type [%s]"),
    INVALID_SYSTEM_KEYSPACE("The system keyspace appears to be corrupted: [%s]."),
    BACKEND_EXCEPTION("Backend Exception."),
    INITIALIZATION_EXCEPTION("Graph for keyspace [%s] not properly initialized. Missing keyspace name resource"),
    GRAPH_CLOSED("The Graph for keyspace [%s] is closed"),
    SESSION_CLOSED("The session for graph [%s] was closed"),
    GRAPH_CLOSED_ON_ACTION("The transaction was %s and closed graph [%s]. Use the session to get a new transaction for the graph."),
    TRANSACTIONS_OPEN("Closed session on graph [%s] with [%s] open transactions"),
    LOCKING_EXCEPTION("Internal locking exception. Please clear the transaction and try again."),
    CANNOT_BE_KEY_AND_RESOURCE("The Type [%s] cannot have the Resource Type [%s] as a key and as a resource"),
    TRANSACTION_ALREADY_OPEN("A transaction is already open on this thread for graph [%s]"),
    TRANSACTION_READ_ONLY("This transaction on graph [%s] is read only"),
    IS_ABSTRACT("The Type [%s] is abstract and cannot have any instances \n"),
    CLOSE_GRAPH_FAILURE("Unable to close graph [%s]"),
    VERSION_MISMATCH("You are attempting to use Grakn Version [%s] with a graph build using version [%s], this is not supported."),
    NO_TYPE("Concept [%s] does not have a type"),
    INVALID_DIRECTION("Cannot traverse an edge in direction [%s]"),
    RESERVED_WORD("The word [%s] is reserved internally and cannot be used"),
    INVALID_PROPERTY_USE("The concept [%s] cannot contain vertex property [%s]"),
    UNKNOWN_CONCEPT("Uknown concept type [%s]"),
    INVALID_IMPLICIT_TYPE("Label [%s] is not an implicit label"),
    LABEL_TAKEN("The label [%s] has already been used"),

    //--------------------------------------------- Validation Errors
    VALIDATION("A structural validation error has occurred. Please correct the [`%s`] errors found. \n"),
    VALIDATION_RELATION_MORE_CASTING_THAN_ROLES("The relation [%s] has [%s] role players but its type [%s] " +
            "only allows [%s] roles \n"),
    VALIDATION_RELATION_CASTING_LOOP_FAIL("The relation [%s] has a role player playing the role [%s] " +
            "which it's type [%s] is not connecting to via a relates connection \n"),

    VALIDATION_CASTING("The type [%s] of role player [%s] is not allowed to play Role [%s] \n"),
    VALIDATION_ROLE_TYPE_MISSING_RELATION_TYPE("Role [%s] does not have a relates connection to any Relation Type. \n"),
    VALIDATION_RELATION_TYPE("Relation Type [%s] does not have one or more roles \n"),
    VALIDATION_INSTANCE("Thing [%s] of type [%s] does not play the required role [%s] \n"),

    VALIDATION_TOO_MANY_KEYS("Thing [%s] has more than one key of type [%s] \n"),

    VALIDATION_RELATION_TYPES_ROLES_SCHEMA("The Role [%s] which is connected to Relation Type [%s] " +
            "does not have a %s Role Type which is connected to the %s Relation Type [%s] \n"),

    VALIDATION_RELATION_DUPLICATE("You have created one or more relations with the following roles and role player [%s] \n"),
    VALIDATION_REQUIRED_RELATION("The role player [%s] of type [%s] can only play the role of [%s] once but is currently doing so [%s] times \n"),

    VALIDATION_RULE_MISSING_ELEMENTS("The [%s] of rule [%s] of type [%s] refers to type [%s] which does not exist in the graph \n"),

    VALIDATION_RULE_DISJUNCTION_IN_BODY("The rule [%s] of type [%s] does not form a valid Horn clause, as it contains a disjunction in the body\n"),

    VALIDATION_RULE_DISJUNCTION_IN_HEAD("The rule [%s] of type [%s] does not form a valid Horn clause, as it contains a disjunction in the head\n"),

    VALIDATION_RULE_HEAD_NON_ATOMIC("The rule [%s] of type [%s] does not form a valid Horn clause, as it contains a multi-atom head\n"),

    VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD("The rule [%s] of type [%s] does not form a valid Horn clause, as its head contains illegal atomics\n"),

    VALIDATION_RULE_INVALID_RELATION_TYPE("Attempting to define a rule containing a relation pattern with type [%s] which is not a relation type\n"),

    VALIDATION_RULE_INVALID_RESOURCE_TYPE("Attempting to define a rule containing a resource pattern with type [%s] which is not a resource type\n"),

    VALIDATION_RULE_RESOURCE_OWNER_CANNOT_HAVE_RESOURCE("Attempting to define a rule containing a resource pattern of type [%s] with type [%s] that cannot have this resource\n"),

    VALIDATION_RULE_ROLE_CANNOT_BE_PLAYED("Attempting to define a rule containing a relation pattern with role [%s] which cannot be played in relation [%s]\n"),

    VALIDATION_RULE_TYPE_CANNOT_PLAY_ROLE("Attempting to define a rule containing a relation pattern with type [%s] that cannot play role [%s] in relation [%s]\n"),

    //--------------------------------------------- Factory Errors
    INVALID_PATH_TO_CONFIG("Unable to open config file [%s]"),
    INVALID_COMPUTER("The graph computer [%s] is not supported"),
    CONFIG_IGNORED("The config parameter [%s] with value [%s] is ignored for this implementation"),
    CANNOT_PRODUCE_GRAPH("Cannot produce a Grakn graph using the backend [%s]"),

    //--------------------------------------------- Client Errors
    INVALID_ENGINE_RESPONSE("Grakn Engine located at [%s] returned response [%s], cannot proceed."),
    INVALID_FACTORY("Graph Factory [%s] is not valid"),
    MISSING_FACTORY_DEFINITION("Graph Factor Config ['factory.internal'] missing from provided config. " +
            "Cannot produce graph"),
    COULD_NOT_REACH_ENGINE("Could not reach Grakn engine at [%s]"),

    //--------------------------------------------- Graql Errors -----------------------------------------------
    NO_GRAPH("no graph provided"),

    SYNTAX_ERROR_NO_POINTER("syntax error at line %s:\n%s"),
    SYNTAX_ERROR("syntax error at line %s: \n%s\n%s\n%s"),

    MUST_BE_RESOURCE_TYPE("type '%s' must be a resource-type"),
    LABEL_NOT_FOUND("label '%s' not found"),
    NOT_A_ROLE_TYPE("'%s' is not a role type. perhaps you meant 'isa %s'?"),
    NOT_A_RELATION_TYPE("'%s' is not a relation type. perhaps you forgot to separate your statements with a ';'?"),
    INSTANCE_OF_ROLE_TYPE("cannot get instances of role type %s"),
    CONFLICTING_PROPERTIES("the following unique properties in '%s' conflict: '%s' and '%s'"),
    NON_POSITIVE_LIMIT("limit %s should be positive"),
    NEGATIVE_OFFSET("offset %s should be non-negative"),
    INVALID_VALUE("unsupported resource value type %s"),

    AGGREGATE_ARGUMENT_NUM("aggregate '%s' takes %s arguments, but got %s"),
    UNKNOWN_AGGREGATE("unknown aggregate '%s'"),

    VARIABLE_NOT_IN_QUERY("the variable %s is not in the query"),
    SELECT_NONE_SELECTED("no variables have been selected. at least one variable must be selected"),
    NO_PATTERNS("no patterns have been provided. at least one pattern must be provided"),
    MATCH_INVALID("cannot match on property of type [%s]"),
    NO_LABEL_SPECIFIED_FOR_HAS("no label was specified for a resource type in a 'has' property"),
    MULTIPLE_GRAPH("a graph has been specified twice for this query"),

    INSERT_RULE_WITHOUT_WHEN("the rule '%s' doesn't have a when specified"),
    INSERT_RULE_WITHOUT_THEN("the rule '%s' doesn't have a then specified"),
    INSERT_UNSUPPORTED_PROPERTY("the property '%s' can only be inserted on a '%s'"),
    INSERT_WITHOUT_TYPE("'%s' doesn't have an 'isa' or a 'sub'"),
    INSERT_UNDEFINED_VARIABLE("%s doesn't have an 'isa', a 'sub' or an 'id'"),
    INSERT_PREDICATE("cannot insert a concept with a predicate"),
    INSERT_RELATION_WITHOUT_ISA("cannot insert a relation without an isa edge"),
    INSERT_MULTIPLE_VALUES("a concept cannot have multiple values %s and '%s'"),
    INSERT_ISA_AND_SUB("cannot insert %s with an isa and a sub"),
    INSERT_NO_DATATYPE("resource type %s must have a datatype defined"),
    INSERT_METATYPE("'%s' cannot be a subtype of '%s'"),
    INSERT_RECURSIVE("%s should not refer to itself"),
    INSERT_TYPE_WITHOUT_LABEL("attempted to insert a type without a label"),
    INSERT_ABSTRACT_NOT_TYPE("the concept [%s] is not a type and cannot be set to abstract"),
    INSERT_RELATION_WITHOUT_ROLE_TYPE("attempted to insert a relation without all role types specified"),
    INSERT_RESOURCE_WITHOUT_VALUE("cannot insert a resource without specifying a value"),
    INSERT_INSTANCE_WITH_NAME("cannot insert an instance with a name: %s"),
    INSERT_NEW_TYPE("instance '%s' already has a type. cannot set new type '%s'"),

    DELETE_UNSUPPORTED_PROPERTY("deleting property '%s' is not supported"),

    INVALID_STATMENT("Value [%s] not of type [%s] in data [%s]"),

    //Templating
    TEMPLATE_MISSING_KEY("Key [%s] not present in data: [%s]"),

    //--------------------------------------------- Engine Errors -----------------------------------------------
    ILLEGAL_ARGUMENT_EXCEPTION("Illegal argument exception caused by [%s]"),
    NO_CONCEPT_IN_KEYSPACE("No concept with ID [%s] exists in keyspace [%s]"),
    READ_ONLY_QUERY("Invalid query: [%s]. LoaderClient only accepts queries that mutate the graph."),

    //Server Errors
    ENGINE_ERROR("Exception on Grakn engine"),
    UNAVAILABLE_TASK_CLASS("Could not find task class [%s]"),
    UNAVAILABLE_PROPERTY("Property requested [%s] has not been defined. See configuration file [%s] for configured properties."),
    MISSING_MANDATORY_REQUEST_PARAMETERS("Missing mandatory query parameter [%s]"),
    MISSING_MANDATORY_BODY_REQUEST_PARAMETERS("Missing mandatory parameter in body [%s]"),
    MISSING_REQUEST_BODY("Empty body- it should contain the Graql query to be executed."),
    UNSUPPORTED_CONTENT_TYPE("Unsupported Content-Type [%s] requested"),
    INVALID_CONTENT_TYPE("Invalid combination of query [%s] and content type [%s]"),
    EXPLAIN_ONLY_MATCH("Cannot get explanation for non-match query, given: [%s]"),
    INVALID_QUERY_USAGE("Only %s queries are allowed."),
    MISSING_TASK_ID("Could not retrieve id %s"),
    TASK_STATE_RETRIEVAL_FAILURE("Could not get state from storage %s"),
    ENGINE_UNAVAILABLE("Cannot reach Grakn engine on [%s:%s]"),
    AUTHENTICATION_FAILURE("Authentication parameters are incorrect or invalid"),
    CANNOT_DELETE_KEYSPACE("Could not delete keyspace [%s]"),

    //Post processing Errors
    GRAPH_MUTATION_ERROR("Unexpected error during graph mutation due to [%s]"),
    UNABLE_TO_MUTATE_GRAPH("Unable to mutate graph [%s] due to several repeating errors"),
    BACK_OFF_RETRY("Unexpected failure performing backoff and retry of [%s]S"),

    //Distributed loading Errors
    ERROR_COMMUNICATING_TO_HOST("Exception thrown while trying to communicate with host [%s]"),
    STATE_STORAGE_ERROR("Exception thrown while retrieving state of a task from storage."),

    //--------------------------------------------- Reasoner Errors -----------------------------------------------
    NON_ATOMIC_QUERY("Addressed query is not atomic: [%s]."),
    NO_ATOMS_SELECTED("No atoms were selected from query [%s]"),
    RULE_CREATION_ARITY_ERROR("Arity mismatch when creating rule"),
    UNIFICATION_ATOM_INCOMPATIBILITY("Attempted unification on incompatible atoms"),

    //--------------------------------------------- Analytics Errors -----------------------------------------------
    NO_SOURCE("No valid source id provided"),
    NO_DESTINATION("No valid destination id provided"),
    RESOURCE_TYPE_NOT_SPECIFIED("no resource type provided for compute query."),
    INSTANCE_DOES_NOT_EXIST("Thing does not exist in the subgraph."),
    NO_PATH_EXIST("There is no path between the two instances."),
    MAX_ITERATION_REACHED("Max iteration of [%s] reached.");

    private final String message;

    ErrorMessage(String message) {
        this.message = message;
    }

    @CheckReturnValue
    public String getMessage(Object... args) {
        return String.format(message, args);
    }
}
