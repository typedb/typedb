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
    SUPER_TYPE_LOOP_DETECTED("By setting the super type of concept [%s] to [%s]. You will be creating a loop. This is prohibited"),
    ID_NOT_UNIQUE("Failed to change the Id of Concept [%s] due to another concept already having an id of " +
            "type [%s] with value [%s]"),
    ID_ALREADY_TAKEN("The id [%s] is already taken by concept [%s]"),
    TOO_MANY_CONCEPTS("Too many concepts found for key [%s] and value [%s]"),
    TOO_MANY_CASTINGS("More than one casting found between Role [%s] and Instance [%s]"),
    INVALID_DATATYPE("The value [%s] must be of datatype [%s]"),
    INVALID_RESOURCE_CAST("The value of [%s] cannot be cast to [%s]"),
    INVALID_OBJECT_TYPE("The concept [%s] is not of type [%s]"),
    REGEX_INSTANCE_FAILURE("The regex [%s] cannot be applied because instance [%s] with value [%s] of the " +
            "Resource Type [%s] does not conform to the regular expression"),
    REGEX_NOT_STRING("The Resource Type [%s] is not of type String so it cannot support regular expressions"),
    ROLE_IS_NULL("The provided role cannot be null with roleplayer [%s]"),
    CLOSED_CLEAR("This graph has been closed due to clearing it"),
    TRANSACTIONS_NOT_SUPPORTED("The graph backend [%s] does not actually support transactions. The transaction was not %s. The graph was actually effected directly"),
    IMMUTABLE_VALUE("The value [%s] of concept [%s] cannot be changed to [%s] due to the property [%s] being immutable"),
    NULL_VALUE("The value of [%s] cannot be set to [null]"),
    META_TYPE_IMMUTABLE("The meta type [%s] is immutable"),
    SCHEMA_LOCKED("Schema cannot be modified when using a batch loading graph"),
    HAS_INVALID("The type [%s] is not allowed to have a %s of type [%s]"),
    INVALID_SYSTEM_KEYSPACE("The system keyspace appears to be corrupted: [%s]."),
    ROLE_TYPE_ERROR("The role type [%s] cannot play itself"),
    BACKEND_EXCEPTION("Unknown Backend Exception."),
    GRAPH_CLOSED("The Graph for keyspace [%s] is closed"),
    GRAPH_CLOSED_ON_ACTION("The transaction was %s and closed graph [%s]. Use the session to get a new transaction for the graph."),
    TRANSACTIONS_OPEN("Closed session on graph [%s] with [%s] open transactions"),
    LOCKING_EXCEPTION("Internal locking exception. Please clear the transaction and try again."),
    CANNOT_BE_KEY_AND_RESOURCE("The Type [%s] cannot have the Resource Type [%s] as a key and as a resource"),
    TRANSACTION_ALREADY_OPEN("A transaction is already open on this thread for graph [%s]"),
    TRANSACTION_READ_ONLY("This transaction on graph [%s] is read only"),

    //--------------------------------------------- Validation Errors
    VALIDATION("A structural validation error has occurred. Please correct the [`%s`] errors found. \n"),
    VALIDATION_RELATION_MORE_CASTING_THAN_ROLES("The relation [%s] has [%s] role players but its type [%s] " +
            "only allows [%s] roles \n"),
    VALIDATION_RELATION_CASTING_LOOP_FAIL("The relation [%s] has a role player playing the role [%s] " +
            "which it's type [%s] is not connecting to via a relates connection \n"),

    VALIDATION_CASTING("The type [%s] of role player [%s] is not allowed to play RoleType [%s] \n"),
    VALIDATION_IS_ABSTRACT("The abstract Type [%s] should not have any instances \n"),
    VALIDATION_ROLE_TYPE_MISSING_RELATION_TYPE("RoleType [%s] does not have a relates connection to any Relation Type. \n"),
    VALIDATION_RELATION_TYPE("Relation Type [%s] does not have one or more roles \n"),
    VALIDATION_INSTANCE("Instance [%s] of type [%s] does not play the required role [%s] \n"),

    VALIDATION_RELATION_TYPES_ROLES_SCHEMA("The Role Type [%s] which is connected to Relation Type [%s] " +
            "does not have a %s Role Type which is connected to the %s Relation Type [%s] \n"),

    VALIDATION_RELATION_DUPLICATE("You have created one or more relations with the following roles and role player [%s] \n"),
    VALIDATION_REQUIRED_RELATION("The role player [%s] of type [%s] can only play the role of [%s] once but is currently doing so [%s] times \n"),

    VALIDATION_RULE_MISSING_ELEMENTS("The [%s] of rule [%s] of type [%s] refers to type [%s] which does not exist in the graph \n"),

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
    CONFIG_NOT_FOUND("Grakn Engine located at [%s] did not return the expected response, due to [%s]"),
    COULD_NOT_REACH_ENGINE("Could not reach Grakn engine at [%s]"),

    //--------------------------------------------- Graql Errors -----------------------------------------------
    NO_GRAPH("no graph provided"),

    SYNTAX_ERROR_NO_POINTER("syntax error at line %s:\n%s"),
    SYNTAX_ERROR("syntax error at line %s: \n%s\n%s\n%s"),

    MUST_BE_RESOURCE_TYPE("type '%s' must be a resource-type"),
    LABEL_NOT_FOUND("label '%s' not found"),
    NOT_A_ROLE_TYPE("'%s' is not a role type. perhaps you meant 'isa %s'?"),
    NOT_A_RELATION_TYPE("'%s' is not a relation type. perhaps you forgot to separate your statements with a ';'?"),
    NOT_ROLE_IN_RELATION("'%s' is not a valid role type for relation type '%s'. valid role types are: '%s'"),
    SET_GENERATED_VARIABLE_NAME("cannot set variable name '%s' on a variable without a user-defined name"),
    INSTANCE_OF_ROLE_TYPE("cannot get instances of role type %s"),
    CONFLICTING_PROPERTIES("the following unique properties in '%s' conflict: '%s' and '%s'"),
    NON_POSITIVE_LIMIT("limit %s should be positive"),
    NEGATIVE_OFFSET("offset %s should be non-negative"),

    AGGREGATE_ARGUMENT_NUM("aggregate '%s' takes %s arguments, but got %s"),
    UNKNOWN_AGGREGATE("unknown aggregate '%s'"),

    VARIABLE_NAME_INVALID("the variable name '%s' is not valid. A variable name should comprise upper and lower alphanumeric characters, underscore and dash."),
    VARIABLE_NOT_IN_QUERY("the variable %s is not in the query"),
    SELECT_NONE_SELECTED("no variables have been selected. at least one variable must be selected"),
    NO_PATTERNS("no patterns have been provided. at least one pattern must be provided"),
    MATCH_INVALID("cannot match on property of type [%s]"),
    NO_LABEL_SPECIFIED_FOR_HAS("no label was specified for a resource type in a 'has' property"),
    MULTIPLE_GRAPH("a graph has been specified twice for this query"),
    MULTIPLE_ORDER("an ordering has been specified twice for this query"),

    INSERT_RULE_WITHOUT_LHS("the rule '%s' doesn't have a lhs specified"),
    INSERT_RULE_WITHOUT_RHS("the rule '%s' doesn't have a rhs specified"),
    INSERT_UNSUPPORTED_PROPERTY("the property '%s' can only be inserted on a '%s'"),
    INSERT_WITHOUT_TYPE("'%s' doesn't have an 'isa' or a 'sub'"),
    INSERT_UNDEFINED_VARIABLE("%s doesn't have an 'isa', a 'sub' or an 'id'"),
    INSERT_PREDICATE("cannot insert a concept with a predicate"),
    INSERT_RELATION_WITH_ID("a relation cannot have an id"),
    INSERT_RELATION_WITHOUT_ISA("cannot insert a relation without an isa edge"),
    INSERT_MULTIPLE_VALUES("a concept cannot have multiple values %s and '%s'"),
    INSERT_ISA_AND_SUB("cannot insert %s with an isa and a sub"),
    INSERT_NO_DATATYPE("resource type %s must have a datatype defined"),
    INSERT_NO_RESOURCE_RELATION("type %s cannot have resource type %s"),
    INSERT_METATYPE("'%s' cannot be a subtype of '%s'"),
    INSERT_RECURSIVE("%s should not refer to itself"),
    INSERT_TYPE_WITHOUT_LABEL("attempted to insert a type without a label"),
    INSERT_RELATION_WITHOUT_ROLE_TYPE("attempted to insert a relation without all role types specified"),
    INSERT_RESOURCE_WITHOUT_VALUE("cannot insert a resource without specifying a value"),
    INSERT_INSTANCE_WITH_NAME("cannot insert an instance with a name: %s"),
    INSERT_NON_RESOURCE_WITH_VALUE("cannot set value on an instance of %s because it is not a resource-type"),

    DELETE_VALUE("deleting values is not supported"),
    DELETE_RESOURCE_TYPE_NO_ID("resource type to delete from concept %s has no id specified"),
    DELETE_UNSUPPORTED_PROPERTY("deleting property '%s' is not supported"),

    FAILED_TO_BUILD_TRAVERSAL("failed to build a traversal from the graql query"),

    NO_ANALYTICS_METHOD("No compute method exists with the name [%s]"),

    //--------------------------------------------- Engine Errors -----------------------------------------------
    NO_CONFIG_FILE("Cannot find config file [%s]"),
    NO_LOG_CONFIG_FILE("It was not possible to find the Logback configuration file [%s]. Default configurations for logging will be used."),
    NO_GRAQL_FILE("Cannot find Graql file [%s]"),
    ILLEGAL_ARGUMENT_EXCEPTION("Illegal argument exception caused by [%s]"),
    CONCEPT_ID_NOT_FOUND("ID [%s] not found in the graph."),
    FAILED_TRANSACTION("Could not commit to graph after %s retries"),
    FAILED_VALIDATION("Caught exception during validation: %s"),
    NO_PARAMETER_PROVIDED("Required Parameter [%s] is missing for this [%s] operation"),
    NO_CONCEPT_IN_KEYSPACE("No concept with ID [%s] exists in keyspace [%s]"),

    //Server Errors
    UNAVAILABLE_TASK_CLASS("Could not find task class [%s]"),
    MISSING_MANDATORY_PARAMETERS("Missing mandatory parameter [%s]"),
    UNSUPPORTED_CONTENT_TYPE("Unsupported Content-Type [%s] requested"),
    INVALID_CONTENT_TYPE("Invalid combination of query [%s] and content type [%s]"),
    EXPLAIN_ONLY_MATCH("Cannot get explanation for non-match query, given: [%s]"),

    //Post processing Errors
    CONCEPT_POSTPROCESSING("Concept [%s] of type [%s] does not have any post-processing steps"),
    POSTPROCESSING_ERROR("Unexpected error during post processing on Job [%s] fix due to [%s]"),
    UNABLE_TO_ANALYSE_CONCEPT("Unable to analyse concepts [%s] part of job [%s] due to several repeating errors"),
    BACK_OFF_RETRY("Unexpected failure performing backoff and retry of [%s]S"),
    //Distributed loading Errors
    ERROR_IN_DISTRIBUTED_TRANSACTION("Error while sending transaction to host. Message:[%s] \n Transaction string: [%s] "),
    ERROR_COMMUNICATING_TO_HOST("Exception thrown while trying to communicate with host [%s]"),
    LOADER_WAIT_TIMEOUT("Exception thrown due to timeout being exceeded while waiting for loading to complete"),
    STATE_STORAGE_ERROR("Exception thrown while retrieving state of a task from storage."),

    //--------------------------------------------- Reasoner Errors -----------------------------------------------
    GRAPH_MISSING("Provided query does not have an associated graph"),
    NON_HORN_RULE("The specified rule [%s] is not a Horn rule"),
    HEAD_ROLES_MISSING("The specified rule [%s] is ambiguous - it does not specify all role types in the head."),
    PARENT_MISSING("Attempted operation on atom %s that does not have a parent"),
    PATTERN_NOT_VAR("The pattern [%s] is not a var"),
    MULTIPLE_RESOURCES("Multiple resource types found during data extraction in atom [%s]"),
    MULTIPLE_VALUE_PREDICATES("Multiple value predicates found during data extraction in atom [%s]"),
    NO_VAL_IN_RELATION("Attempted to return value from a relation atom"),
    NO_TYPE_CONSTRAINTS("Attempted to return constraints from non-relation atom"),
    LOOP_CLAUSE("Unable to find suitable starting atom (loop?)"),
    NON_ATOMIC_QUERY("Addressed query is not atomic [%s]"),
    MULTI_VALUE_VAR("Multi value variable found [%s] on query construction"),
    NO_ATOMS_SELECTED("No atoms were selected from query [%s]"),
    MATERIALIZATION_ERROR("Not enough information available to materialize query [%s]"),
    ANSWER_ERROR("Answer method called on an instance with no answers"),
    RULE_CREATION_ARITY_ERROR("Arity mismatch when creating rule"),
    UNIFICATION_ATOM_INCOMPATIBILITY("Attempted unification on incompatible atoms"),
    GRAQL_PROPERTY_NOT_MAPPED("Graql property [%s] could not be mapped"),
    CANNOT_CREATE_IDPREDICATE("Cannot create IdPredicate - type [%s] doesn't exist in the graph"),

    //--------------------------------------------- Analytics Errors -----------------------------------------------
    NO_SOURCE("No valid source id provided"),
    NO_DESTINATION("No valid destination id provided"),
    RESOURCE_TYPE_NOT_SPECIFIED("no resource type provided for compute query."),
    INSTANCE_DOES_NOT_EXIST("Instance does not exist in the subgraph."),
    NO_PATH_EXIST("There is no path between the two instances."),
    ONTOLOGY_MUTATION("The mutations to the ontology have not been successfully committed. Validation Errors: [%s]"),
    BULK_PERSIST("The bulk persist operation on instances of concept type [%s] has failed with validation error: [%s]"),
    CLONE_FAILED("The vertex program could not be cloned, check implementation of the clone method in class: [%s]. Stacktrace: [%s]"),
    MAX_ITERATION_REACHED("Max iteration of [%s] reached.");

    private final String message;

    ErrorMessage(String message) {
        this.message = message;
    }

    public String getMessage(Object... args) {
        return String.format(message, args);
    }
}
