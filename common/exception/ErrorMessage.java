/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.exception;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.JAVA_ERROR;

public abstract class ErrorMessage extends com.vaticle.typedb.common.exception.ErrorMessage {

    private ErrorMessage(String codePrefix, int codeNumber, String messagePrefix, String messageBody) {
        super(codePrefix, codeNumber, messagePrefix, messageBody);
    }

    public static void loadConstants() {
        for (Class<?> innerClass : ErrorMessage.class.getDeclaredClasses()) {
            try {
                Class.forName(innerClass.getName(), true, innerClass.getClassLoader());
            } catch (ClassNotFoundException e) {
                throw TypeDBException.of(JAVA_ERROR, e);
            }
        }
    }

    public static class Server extends ErrorMessage {
        public static final Server FAILED_TO_CREATE_DATA_DIRECTORY =
                new Server(1, "Failed to create the data directory '%s', received exception:\n'%s'");
        public static final Server DATA_DIRECTORY_NOT_WRITABLE =
                new Server(2, "The data directory path '%s' is not writable.");
        public static final Server EXITED_WITH_ERROR =
                new Server(3, "Exited with error.");
        public static final Server UNCAUGHT_ERROR =
                new Server(4, "Uncaught error thrown at thread '%s':\n'%s'");
        public static final Server CONFIG_FILE_NOT_FOUND =
                new Server(5, "Could not find/read the configuration file '%s'.");
        public static final Server UNRECOGNISED_CLI_COMMAND =
                new Server(6, "The command '%s' was not recognised.");
        public static final Server CLI_OPTION_MISSING_PREFIX =
                new Server(7, "All arguments should be prefixed with '%s', which '%s' is missing.");
        public static final Server DUPLICATE_CLI_OPTION =
                new Server(8, "Unexpected duplicate command line arguments '%s'.");
        public static final Server CLI_FLAG_OPTION_HAS_VALUE =
                new Server(9, "Command line option '%s' does not take a value.");
        public static final Server CLI_OPTION_REQUIRES_TYPED_VALUE =
                new Server(10, "Command line option '%s' requires a '%s' value.");
        public static final Server CLI_OPTION_REQUIRES_VALUE =
                new Server(11, "Command line option '%s' requires a value.");
        public static final Server CLI_OPTION_REQUIRED =
                new Server(12, "Missing required command line option: '%s'.");
        public static final Server CLI_OPTION_UNRECOGNISED =
                new Server(13, "Unrecognized command line option: '%s'.");
        public static final Server CONFIG_YAML_MUST_BE_MAP =
                new Server(14, "The configuration file must be a YAML map.");
        public static final Server CONFIG_SECTION_MUST_BE_MAP =
                new Server(15, "The configuration section with key '%s' must be a key-value map.");
        public static final Server CONFIG_KEY_MISSING =
                new Server(16, "Required configuration '%s' is missing.");
        public static final Server CONFIGS_UNRECOGNISED =
                new Server(17, "The provided configuration(s) '%s' are unrecognised.");
        public static final Server CONFIG_VALUE_UNEXPECTED =
                new Server(18, "Configuration '%s' received an unexpected value '%s'. It must be '%s'.");
        public static final Server CONFIG_VALUE_ENUM_UNEXPECTED =
                new Server(19, "Configuration '%s' received an unexpected value '%s'. It must be one of '%s'.");
        public static final Server CONFIG_LOG_OUTPUT_UNRECOGNISED =
                new Server(20, "Configuration output named '%s' was not recognised (check the output definition names).");
        public static final Server CONFIG_REASONER_REQUIRES_DIR_OUTPUT =
                new Server(21, "Reasoner debugger configuration requires a directory output");
        public static final Server FAILED_AT_STOPPING =
                new Server(22, "Exception occurred while attempting to stop the server.");
        public static final Server ENV_VAR_NOT_FOUND =
                new Server(23, "Environment variable '%s' is not defined.");
        public static final Server SERVER_SHUTDOWN =
                new Server(24, "TypeDB server has been shutdown.");
        public static final Server MISSING_FIELD =
                new Server(25, "The request message does not contain the required field '%s'.");
        public static final Server PROTOCOL_VERSION_MISMATCH =
                new Server(26, "A protocol version mismatch was detected. This server supports version '%s' but the driver supports version '%s'. Please use a compatible driver to connect.");
        public static final Server MISSING_CONCEPT =
                new Server(27, "Concept does not exist.");
        public static final Server BAD_VALUE_TYPE =
                new Server(28, "The value type '%s' was not recognised.");
        public static final Server TRANSACTION_EXCEEDED_MAX_SECONDS =
                new Server(29, "Transaction exceeded maximum configured duration of '%s' seconds.");
        public static final Server EMPTY_TRANSACTION_REQUEST =
                new Server(30, "Empty transaction request.");
        public static final Server UNKNOWN_REQUEST_TYPE =
                new Server(31, "The request message was not recognised.");
        public static final Server ITERATION_WITH_UNKNOWN_ID =
                new Server(32, "Iteration was requested for ID '%s', but this ID does not correspond to an existing query iterator.");
        public static final Server DUPLICATE_REQUEST =
                new Server(33, "The request with ID '%s' is a duplicate.");
        public static final Server PORT_IN_USE =
                new Server(34, "Could not bind to the port since it is in use: '%s'.");
        public static final Server INCOMPATIBLE_JAVA_RUNTIME =
                new Server(35, "Incompatible Java runtime version: '%s'. Please use Java 11 or above.");
        public static final Server ERROR_LOGGING_CONNECTIONS =
                new Server(36, "An error occurred while logging server connection info.");
        public static final Server USER_MANAGEMENT_NOT_AVAILABLE =
                new Server(37, "User management is only available in TypeDB Cloud.");

        private static final String codePrefix = "SRV";
        private static final String messagePrefix = "Invalid Server Operation";

        Server(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class Internal extends ErrorMessage {
        public static final Internal ILLEGAL_STATE =
                new Internal(1, "Illegal internal state!");
        public static final Internal ILLEGAL_CAST =
                new Internal(2, "Illegal casting operation from '%s' to '%s'.");
        public static final Internal ILLEGAL_OPERATION =
                new Internal(3, "Illegal internal operation! This method should not have been called.");
        public static final Internal ILLEGAL_ARGUMENT =
                new Internal(4, "Illegal argument provided.");
        public static final Internal UNSUPPORTED_OPERATION =
                new Internal(5, "Operation is not supported.");
        public static final Internal UNRECOGNISED_VALUE =
                new Internal(7, "Unrecognised encoding value!");
        public static final Internal STORAGE_PROPERTY_EXCEPTION =
                new Internal(8, "Failed to get storage property.");
        public static final Internal DIRTY_INITIALISATION =
                new Internal(9, "Invalid Database Initialisation.");
        public static final Internal TYPEDB_CLOSED =
                new Internal(10, "Attempted to open a session on a closed TypeDB.");
        public static final Internal OUT_OF_BOUNDS =
                new Internal(11, "Resource out of bounds.");
        public static final Internal UNEXPECTED_INTERRUPTION =
                new Internal(12, "Unexpected thread interruption!");
        public static final Internal UNEXPECTED_PLANNING_ERROR =
                new Internal(13, "Unexpected error during traversal plan optimisation.");
        public static final Internal UNEXPECTED_OPTIMISER_VALUE =
                new Internal(14, "Unexpected optimiser value.");
        public static final Internal UNIMPLEMENTED =
                new Internal(15, "This functionality is not yet implemented.");
        public static final Internal JAVA_ERROR =
                new Internal(16, "Received Java error:\n%s");
        public static final Internal STORAGE_ERROR =
                new Internal(17, "Received storage error:\n%s");
        public static final Internal UNKNOWN_ERROR =
                new Internal(18, "Received unknown error:\n%s");

        public static final String codePrefix = "INT";
        private static final String messagePrefix = "Invalid Internal State";

        Internal(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class Database extends ErrorMessage {
        public static final Database INVALID_DATABASE_DIRECTORIES =
                new Database(1, "Database '%s' (located at: %s) does not contain required directories '%s'.");
        public static final Database INCOMPATIBLE_ENCODING =
                new Database(2, "Database '%s' (located at: %s) has incompatible data version '%d' - this server supports " +
                        "version '%d'. Please reload or migrate your data.");
        public static final Database DATABASE_MANAGER_CLOSED =
                new Database(3, "Attempted to use database manager when it has been closed.");
        public static final Database DATABASE_EXISTS =
                new Database(4, "The database with the name '%s' already exists.");
        public static final Database DATABASE_NOT_EMPTY =
                new Database(5, "The existing database with the name '%s' is not empty.");
        public static final Database DATABASE_NOT_FOUND =
                new Database(6, "The database with the name '%s' does not exist.");
        public static final Database DATABASE_DELETED =
                new Database(7, "Database with the name '%s' has been deleted.");
        public static final Database DATABASE_CLOSED =
                new Database(8, "Attempted to open a new session from the database '%s' that has been closed.");
        public static final Database DATABASE_NAME_RESERVED =
                new Database(9, "Database name must not start with an underscore.");
        public static final Database ROCKS_LOGGER_SHUTDOWN_TIMEOUT =
                new Database(10, "Background RocksDB properties logger shutdown timed out.");
        public static final Database STATISTICS_CORRECTOR_SHUTDOWN_TIMEOUT =
                new Database(11, "Background statistics corrector shutdown timed out.");

        private static final String codePrefix = "DBS";
        private static final String messagePrefix = "Invalid Database Operation";

        Database(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class Session extends ErrorMessage {
        public static final Session SESSION_NOT_FOUND =
                new Session(1, "Session with UUID '%s' does not exist.");
        public static final Session SESSION_CLOSED =
                new Session(2, "Attempted to open a transaction from closed session.");
        public static final Session SCHEMA_ACQUIRE_LOCK_TIMEOUT =
                new Session(3, "Could not acquire lock for schema session. Another schema session may have been left open.");
        public static final Session SESSION_IDLE_TIMEOUT_NOT_CONFIGURABLE =
                new Session(4, "The session idle timeout is not configurable at the '%s' level.");

        private static final String codePrefix = "SSN";
        private static final String messagePrefix = "Invalid Session Operation";

        Session(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class Transaction extends ErrorMessage {
        public static final Transaction UNSUPPORTED_OPERATION =
                new Transaction(1, "Unsupported operation: calling '%s' for '%s' is not supported.");
        public static final Transaction ILLEGAL_OPERATION =
                new Transaction(2, "Attempted an illegal operation!");
        public static final Transaction TRANSACTION_NOT_OPENED =
                new Transaction(3, "The transaction has not been opened yet, so the only allowed operation is to open it.");
        public static final Transaction TRANSACTION_ALREADY_OPENED =
                new Transaction(4, "Transaction has already been opened.");
        public static final Transaction TRANSACTION_CLOSED =
                new Transaction(5, "The transaction has been closed and no further operation is allowed.");
        public static final Transaction ILLEGAL_COMMIT =
                new Transaction(6, "Only write transactions can be committed.");
        public static final Transaction TRANSACTION_SCHEMA_READ_VIOLATION =
                new Transaction(7, "Attempted schema writes when transaction type does not allow.");
        public static final Transaction TRANSACTION_DATA_READ_VIOLATION =
                new Transaction(8, "Attempted data writes when transaction type does not allow.");
        public static final Transaction TRANSACTION_ISOLATION_MODIFY_DELETE_VIOLATION =
                new Transaction(9, "The transaction modifies a key that is deleted in a concurrent transaction.");
        public static final Transaction TRANSACTION_ISOLATION_DELETE_MODIFY_VIOLATION =
                new Transaction(10, "The transaction deletes a key that is modified in concurrent transaction.");
        public static final Transaction TRANSACTION_ISOLATION_EXCLUSIVE_CREATE_VIOLATION =
                new Transaction(11, "The transaction fails to create a key that is created exclusively in a concurrent transaction.");
        public static final Transaction SESSION_DATA_VIOLATION =
                new Transaction(12, "Attempted schema writes when session type does not allow.");
        public static final Transaction SESSION_SCHEMA_VIOLATION =
                new Transaction(13, "Attempted data writes when session type does not allow.");
        public static final Transaction MISSING_TRANSACTION =
                new Transaction(14, "Transaction can not be null.");
        public static final Transaction BAD_TRANSACTION_TYPE =
                new Transaction(15, "The transaction type '%s' was not recognised.");
        public static final Transaction DATA_ACQUIRE_LOCK_TIMEOUT =
                new Transaction(16, "Could not acquire lock for data transaction. A schema session may have been left open.");
        public static final Transaction RPC_PREFETCH_SIZE_TOO_SMALL =
                new Transaction(17, "RPC answer streaming prefetch size must be at least 1, is set to: %d.");
        public static final Transaction TRANSACTION_TIMEOUT_NOT_CONFIGURABLE =
                new Transaction(18, "Transaction timeout cannot be configured at the '%s' level.");
        public static final Transaction QUERY_ERROR =
                new Transaction(19, "Error executing query: \n%s");
        public static final Transaction CONCEPT_ERROR =
                new Transaction(20, "Error executing concept operation: \n%s");
        public static final Transaction LOGIC_ERROR =
                new Transaction(21, "Error executing logic operation: \n%s");
        public static final Transaction RESOURCE_CLOSED =
                new Transaction(22, "Attempted to utilise a closed resource.");
        public static final Transaction SCHEMA_VALIDATION_EXCEPTIONS =
                new Transaction(23, "Errors during schema validation:\n%s");

        private static final String codePrefix = "TXN";
        private static final String messagePrefix = "Invalid Transaction Operation";

        Transaction(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class Pattern extends ErrorMessage {

        public static final Pattern INVALID_CASTING =
                new Pattern(1, "The class '%s' cannot be casted to '%s'.");
        public static final Pattern ANONYMOUS_CONCEPT_VARIABLE =
                new Pattern(2, "Attempted to refer to a concept using an anonymous variable. Their intended use is for inserting things.");
        public static final Pattern ANONYMOUS_TYPE_VARIABLE =
                new Pattern(3, "Attempted to refer to a type using an anonymous variable. Their intended use is for inserting things.");
        public static final Pattern UNBOUNDED_CONCEPT_VARIABLE =
                new Pattern(4, "Invalid query containing unbounded concept variable '%s'.");
        public static final Pattern UNBOUNDED_NEGATION =
                new Pattern(5, "Invalid query containing unbounded negation pattern.");
        public static final Pattern MISSING_CONSTRAINT_VALUE =
                new Pattern(6, "The value constraint for variable has not been provided with a variable or literal value.");
        public static final Pattern VARIABLE_CONTRADICTION =
                new Pattern(7, "The variable '%s' is both a type and a thing.");
        public static final Pattern MULTIPLE_THING_CONSTRAINT_IID =
                new Pattern(8, "The thing variable '%s' has multiple 'iid' constraints.");
        public static final Pattern MULTIPLE_THING_CONSTRAINT_ISA =
                new Pattern(9, "The thing variable '%s' has multiple 'isa' constraints, '%s' and '%s'.");
        public static final Pattern MULTIPLE_THING_CONSTRAINT_RELATION =
                new Pattern(10, "The relation variable '%s' has multiple 'relation' constraints");
        public static final Pattern ILLEGAL_TYPE_SPECIFIER =
                new Pattern(11, "The variable '%s' is specified with a type '%s', which is not allowed in this part of the query.");
        public static final Pattern MULTIPLE_TYPE_CONSTRAINT_SUB =
                new Pattern(12, "The type variable '%s' has multiple 'sub' constraints.");
        public static final Pattern MULTIPLE_TYPE_CONSTRAINT_LABEL =
                new Pattern(13, "The type variable '%s' has multiple 'label' constraints.");
        public static final Pattern MULTIPLE_TYPE_CONSTRAINT_VALUE_TYPE =
                new Pattern(14, "The type variable '%s' has multiple 'value' constraints.");
        public static final Pattern MULTIPLE_TYPE_CONSTRAINT_REGEX =
                new Pattern(15, "The type variable '%s' has multiple 'regex' constraints.");
        public static final Pattern INFERENCE_INCOHERENT_MATCH_PATTERN =
                new Pattern(16, "Could not infer compatible types for the match pattern:\n'%s'.");
        public static final Pattern INFERENCE_INCOHERENT_MATCH_SUB_PATTERN =
                new Pattern(17, "Could not infer compatible types for the match pattern:\n '%s'\n, due to the included pattern:\n'%s'.");
        public static final Pattern INFERENCE_INCOHERENT_VALUE_TYPES =
                new Pattern(18, "Could not infer compatible types for the pattern:\n'%s'\n, due to contradicting attribute value types for:\n'%s'.");
        public static final Pattern UNRECOGNISED_ANNOTATION =
                new Pattern(19, "The annotation '%s' is not recognised.");
        public static final Pattern VARIABLE_NAME_CONFLICT =
                new Pattern(20, "The variable name '%s' was used both for a concept variable and a value variable.");
        public static final Pattern VALUE_VARIABLE_UNASSIGNED =
                new Pattern(21, "The value variable '%s' is never assigned to.");
        public static final Pattern VALUE_VARIABLE_DUPLICATE_ASSIGMENT =
                new Pattern(22, "The value variable '%s' can only have one assignment in the first scope it is used in.");
        public static final Pattern VALUE_ASSIGNMENT_CYCLE =
                new Pattern(23, "A cyclic assignment between value variables was detected: '%s'.");
        public static final Pattern TYPEQL_ERROR =
                new Pattern(24, "TypeQL error occurred:\n'%s'.");

        private static final String codePrefix = "QRY";
        private static final String messagePrefix = "Invalid Query Pattern";

        Pattern(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class Projection extends ErrorMessage {
        public static final Projection PROJECTION_VARIABLE_UNNAMED =
                new Projection(1, "Projection variable '%s' must be named.");
        public static final Projection PROJECTION_VARIABLE_UNBOUND =
                new Projection(2, "Projection variable '%s' is not bound in the preceding match clause.");
        public static final Projection VARIABLE_PROJECTION_CONCEPT_NOT_READABLE =
                new Projection(3, "The variable projection '%s' cannot be used to fetch entity or relation instances. Entities and relations must be mapped to attributes.");
        public static final Projection ILLEGAL_ATTRIBUTE_PROJECTION_TYPE_VARIABLE =
                new Projection(4, "Illegal attribute projection from type variable '%s'.");
        public static final Projection ILLEGAL_ATTRIBUTE_PROJECTION_ATTRIBUTE_TYPE_INVALID =
                new Projection(5, "Attribute projection from '%s' maps to an invalid attribute type '%s'.");
        public static final Projection ILLEGAL_ATTRIBUTE_PROJECTION_TYPES_NOT_OWNED =
                new Projection(6, "Projection from '%s' to attribute type '%s' is illegal, since '%s' could be of type '%s' which does not own the attribute type or any of its subtypes. Constrain the 'match' clause such that all types can own the attribute type or its subtypes.");
        public static final Projection SUBQUERY_UNBOUNDED =
                new Projection(7, "Subquery labeled '%s' is not bounded by any parent match clause.");


        private static final String codePrefix = "PRO";
        private static final String messagePrefix = "Invalid projection operation";

        Projection(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class Expression extends ErrorMessage {
        public static final Expression AMBIGUOUS_VARIABLE_TYPE =
                new Expression(1, "The variable '%s' has ambiguous value types: '%s'.");
        public static final Expression FUNCTION_NOT_RECOGNISED =
                new Expression(2, "The expression function '%s' is not recognised.");
        public static final Expression OPERATION_NOT_RECOGNISED =
                new Expression(3, "The expression operation '%s' is not recognised.");
        public static final Expression FUNCTION_ARGUMENTS_INCOMPATIBLE =
                new Expression(4, "The expression '%s' has arguments with incompatible value types: '%s : %s' and '%s : %s'.");
        public static final Expression ILLEGAL_CONVERSION =
                new Expression(5, "The expression '%s' with value type '%s' cannot be converted to type '%s'.");
        public static final Expression ILLEGAL_FUNCTION_ARGUMENT_TYPE =
                new Expression(6, "The expression function '%s' cannot accept arguments of value type '%s'.");
        public static final Expression ARGUMENT_COUNT_MISMATCH =
                new Expression(7, "The expression '%s' expects '%s' arguments but received '%s': '%s'.");
        public static final Expression EVALUATION_ERROR =
                new Expression(8, "An error occured while evaluating an expression:\nExpression: '%s = %s'; \nInput: '%s'.\nError: '%s'");
        public static final Expression EVALUATION_ERROR_DIVISION_BY_ZERO =
                new Expression(9, "Illegal division by zero occurred during: '%s / %s'.");

        private static final String codePrefix = "EXP";
        private static final String messagePrefix = "Invalid expression operation";

        Expression(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class ThingRead extends ErrorMessage {
        public static final ThingRead INVALID_THING_IID_CASTING =
                new ThingRead(1, "Invalid Thing IID casting to '%s'.");
        public static final ThingRead INVALID_THING_VERTEX_CASTING =
                new ThingRead(2, "Invalid ThingVertex ('%s') casting to '%s'.");
        public static final ThingRead INVALID_THING_CASTING =
                new ThingRead(3, "Invalid concept conversion from '%s' to '%s'.");
        public static final ThingRead THING_NOT_FOUND =
                new ThingRead(4, "The thing with IID '%s' is not found.");
        public static final ThingRead INVALID_ROLE_TYPE_LABEL =
                new ThingRead(5, "The role type '%s' is not scoped by its relation type.");
        public static final ThingRead CONTRADICTORY_BOUND_VARIABLE =
                new ThingRead(6, "The nested variable '%s' contradicts the type of its bound variable.");
        public static final ThingRead SORT_ATTRIBUTE_NOT_COMPARABLE =
                new ThingRead(7, "The variable '%s' cannot be used to sort, as it may represent incomparable types '%s' and '%s'.");
        public static final ThingRead VALUES_NOT_COMPARABLE =
                new ThingRead(8, "The '%s' value '%s' cannot be compared to the '%s' value '%s'.");
        public static final ThingRead VALUE_TYPES_NOT_COMPARABLE =
                new ThingRead(9, "The value type '%s' cannot be compared to value type '%s'.");
        public static final ThingRead AGGREGATE_ATTRIBUTE_NOT_NUMBER =
                new ThingRead(10, "The variable '%s' cannot be used to calculate aggregate functions, as it contains non-numeric values.");
        public static final ThingRead NUMERIC_IS_NOT_NUMBER =
                new ThingRead(11, "The numeric value is not a valid number (NaN).");

        private static final String codePrefix = "THR";
        private static final String messagePrefix = "Invalid Thing Read";

        ThingRead(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class ThingWrite extends ErrorMessage {
        public static final ThingWrite ILLEGAL_ABSTRACT_WRITE =
                new ThingWrite(1, "Attempted an illegal write of a new '%s' of abstract type '%s'.");
        public static final ThingWrite THING_HAS_BEEN_DELETED =
                new ThingWrite(2, "Instance '%s' of type '%s' has been deleted and cannot be modified any further.");
        public static final ThingWrite THING_CANNOT_OWN_ATTRIBUTE =
                new ThingWrite(3, "Attribute of type '%s' is not defined to be owned by type '%s'.");
        public static final ThingWrite THING_KEY_OVER =
                new ThingWrite(4, "Attempted to assign a key of type '%s' onto a(n) '%s' that already has one.");
        public static final ThingWrite THING_KEY_TAKEN =
                new ThingWrite(5, "Attempted to assign a key '%s' of type '%s' that had been taken by another '%s'.");
        public static final ThingWrite THING_KEY_MISSING =
                new ThingWrite(6, "Attempted to commit a(n) '%s' that is missing key(s) of type(s): %s"); // don't put quotes around the last %s
        public static final ThingWrite THING_UNIQUE_TAKEN =
                new ThingWrite(7, "Attempted to assign a unique attribute '%s' of type '%s' that had been taken by another '%s'.");
        public static final ThingWrite THING_ROLE_UNPLAYED =
                new ThingWrite(8, "The thing type '%s' does not play the role type '%s'.");
        public static final ThingWrite RELATION_ROLE_UNRELATED =
                new ThingWrite(9, "Relation type '%s' does not relate role type '%s'.");
        public static final ThingWrite RELATION_PLAYER_MISSING =
                new ThingWrite(10, "Relation instance of type '%s' does not have any role player");
        public static final ThingWrite ATTRIBUTE_VALUE_UNSATISFIES_REGEX =
                new ThingWrite(11, "Attempted to put an instance of '%s' with value '%s' that does not satisfy the regular expression '%s'.");
        public static final ThingWrite THING_IID_NOT_INSERTABLE =
                new ThingWrite(12, "The variable '%s' tries to insert iid '%s'. IIDs are prohibited in insert clauses. You may want to query the variable using IID in the match clause.");
        public static final ThingWrite THING_INSERT_ISA_NOT_THING_TYPE =
                new ThingWrite(13, "The type '%s' cannot be used for creating an instance as it is not a thing type.");
        public static final ThingWrite THING_ISA_REINSERTION =
                new ThingWrite(14, "Attempted to re-insert pre-existing thing of matched variable '%s' as a new instance (isa) of type '%s'.");
        public static final ThingWrite THING_ISA_MISSING =
                new ThingWrite(15, "The thing variable '%s' cannot be inserted as a new instance without providing its type (isa).");
        public static final ThingWrite ILLEGAL_VALUE_VARIABLE_IN_DELETE =
                new ThingWrite(16, "Illegal value variable '%s' in delete query. You can only delete named variables that were matched.");
        public static final ThingWrite ILLEGAL_ANONYMOUS_VARIABLE_IN_DELETE =
                new ThingWrite(17, "Illegal anonymous delete variable in pattern '%s'.  You can only delete named variables that were matched.");
        public static final ThingWrite INVALID_DELETE_THING =
                new ThingWrite(18, "The thing '%s' cannot be deleted, as the provided type '%s' is not its direct type nor supertype.");
        public static final ThingWrite INVALID_DELETE_THING_DIRECT =
                new ThingWrite(19, "The thing '%s' cannot be deleted, as the provided direct type '%s' is not valid.");
        public static final ThingWrite INVALID_DELETE_HAS =
                new ThingWrite(20, "Invalid attempt to delete attribute ownership. The thing '%s' does not have attribute '%s'.");
        public static final ThingWrite HAS_TYPE_MISMATCH =
                new ThingWrite(21, "The instance of type '%s' cannot own instances of attribute type '%s'.");
        public static final ThingWrite ILLEGAL_IS_CONSTRAINT =
                new ThingWrite(22, "The 'is' constraint, e.g. used in '%s', is not accepted in an insert/delete query.");
        public static final ThingWrite ATTRIBUTE_VALUE_TOO_MANY =
                new ThingWrite(23, "Unable to insert attribute '%s' of type '%s' with more than one value operations.");
        public static final ThingWrite ATTRIBUTE_VALUE_MISSING =
                new ThingWrite(24, "Unable to insert attribute '%s' of type '%s' without a value assigned to the variable.");
        public static final ThingWrite INSERT_RELATION_CONSTRAINT_TOO_MANY =
                new ThingWrite(25, "Unable to insert relation '%s' as it has more than one relation tuple describing the role players.");
        public static final ThingWrite ROLE_TYPE_AMBIGUOUS =
                new ThingWrite(26, "Unable to add role player '%s' to the relation, as there are more than one possible role type it could play.");
        public static final ThingWrite ROLE_TYPE_MISSING =
                new ThingWrite(27, "Unable to add role player '%s' to the relation, as there is no provided or inferrable role type.");
        public static final ThingWrite ROLE_TYPE_MISMATCH =
                new ThingWrite(28, "The type '%s' cannot be used as a role type.");
        public static final ThingWrite PLAYING_TYPE_MISMATCH =
                new ThingWrite(29, "The instance of type '%s' cannot play the role type '%s'.");
        public static final ThingWrite RELATING_TYPE_MISMATCH =
                new ThingWrite(30, "The relation instance of type '%s' cannot relate the role type '%s'.");
        public static final ThingWrite MAX_INSTANCE_REACHED =
                new ThingWrite(31, "The maximum number of instances for type '%s' has been reached: '%s'");
        public static final ThingWrite DELETE_RELATION_CONSTRAINT_TOO_MANY =
                new ThingWrite(32, "Could not perform delete of role players due to multiple relation constraints being present for relation '%s'.");
        public static final ThingWrite DELETE_ROLEPLAYER_NOT_PRESENT =
                new ThingWrite(33, "Could not delete roleplayer '%s' as relation '%s' does not relate it.");
        public static final ThingWrite ILLEGAL_VALUE_CONSTRAINT_IN_INSERT =
                new ThingWrite(34, "Illegal value constraint found in the insert query on variable '%s'. Value variables are only permitted to specify attribute values.");
        public static final ThingWrite ILLEGAL_UNBOUND_TYPE_VAR_IN_INSERT =
                new ThingWrite(35, "Type variable '%s' found in the insert query must retrieved by the match previously.");

        private static final String codePrefix = "THW";
        private static final String messagePrefix = "Invalid Thing Write";

        ThingWrite(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class TypeGraph extends ErrorMessage {
        public static final TypeGraph INVALID_SCHEMA_IID_CASTING =
                new TypeGraph(1, "Invalid Schema IID cast to '%s'.");
        public static final TypeGraph INVALID_SCHEMA_WRITE =
                new TypeGraph(2, "The label '%s' is already in use in the schema graph.");

        private static final String codePrefix = "SCG";
        private static final String messagePrefix = "Invalid Schema Graph Operation";

        TypeGraph(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class TypeRead extends ErrorMessage {
        public static final TypeRead INVALID_TYPE_CASTING =
                new TypeRead(1, "Invalid concept conversion from '%s' to '%s'.");
        public static final TypeRead TYPE_ROOT_MISMATCH =
                new TypeRead(2, "Attempted to retrieve '%s' as '%s', while it is actually a(n) '%s'.");
        public static final TypeRead TYPE_NOT_FOUND =
                new TypeRead(3, "The type '%s' does not exist.");
        public static final TypeRead ROLE_TYPE_NOT_FOUND =
                new TypeRead(4, "There is no role type '%s' in the relation type '%s'");
        public static final TypeRead TYPE_NOT_RESOLVABLE =
                new TypeRead(5, "The type for variable '%s' is not resolvable.");
        public static final TypeRead TYPE_NOT_ATTRIBUTE_TYPE =
                new TypeRead(6, "The type '%s' is not a valid attribute type.");
        public static final TypeRead VALUE_TYPE_MISMATCH =
                new TypeRead(7, "Attempted to retrieve '%s' as AttributeType of ValueType '%s', while it actually has ValueType '%s'.");
        public static final TypeRead OVERRIDDEN_TYPES_IN_TRAVERSAL =
                new TypeRead(8, "Attempted to query for an overridden type through a traversal. Overridden types cannot be queried via TypeQL Match.");
        public static final TypeRead ROLE_TYPE_SCOPE_IS_NOT_RELATION_TYPE =
                new TypeRead(9, "The role type '%s' has scope '%s' that is not a relation type.");

        private static final String codePrefix = "TYR";
        private static final String messagePrefix = "Invalid Type Read";

        TypeRead(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class TypeWrite extends ErrorMessage {
        public static final TypeWrite ROOT_TYPE_MUTATION =
                new TypeWrite(1, "Root types are immutable.");
        public static final TypeWrite TYPE_HAS_SUBTYPES =
                new TypeWrite(2, "The type '%s' has subtypes, and cannot be deleted.");
        public static final TypeWrite TYPE_HAS_INSTANCES_SET_ABSTRACT =
                new TypeWrite(3, "The type '%s' has instances, and cannot be set abstract.");
        public static final TypeWrite TYPE_HAS_INSTANCES_DELETE =
                new TypeWrite(4, "The type '%s' has instances, and cannot be deleted.");
        public static final TypeWrite TYPE_HAS_BEEN_DELETED =
                new TypeWrite(5, "The type '%s' has been deleted and cannot be modified any further.");
        public static final TypeWrite EDGE_HAS_BEEN_DELETED =
                new TypeWrite(6, "The edge '%s' has been deleted and cannot be modified any further.");
        public static final TypeWrite TYPE_REFERENCED_IN_RULES =
                new TypeWrite(7, "The type '%s' is referenced in rules '%s', and cannot be deleted.");
        public static final TypeWrite CYCLIC_TYPE_HIERARCHY =
                new TypeWrite(8, "There is a cyclic type hierarchy, which is not allowed: '%s'.");
        public static final TypeWrite OWNS_ABSTRACT_ATTRIBUTE_TYPE =
                new TypeWrite(9, "The type '%s' is not abstract, and thus cannot own an abstract attribute type '%s'.");
        public static final TypeWrite OVERRIDDEN_OWNED_ATTRIBUTE_TYPE_NOT_SUPERTYPE =
                new TypeWrite(10, "In the type '%s', the owned attribute type '%s' cannot override '%s' as it is not a supertype.");
        public static final TypeWrite OVERRIDDEN_PLAYED_ROLE_TYPE_NOT_SUPERTYPE =
                new TypeWrite(11, "In the type '%s', the played role type '%s' cannot override '%s' as it is not a supertype.");
        public static final TypeWrite OVERRIDDEN_RELATED_ROLE_TYPE_NOT_INHERITED =
                new TypeWrite(12, "In the relation type '%s', the related role type '%s' cannot override '%s' as it is not an inherited role type.");
        public static final TypeWrite ATTRIBUTE_SUPERTYPE_VALUE_TYPE =
                new TypeWrite(14, "The attribute type '%s' has value type '%s', and cannot have supertype '%s' with value type '%s'.");
        public static final TypeWrite ATTRIBUTE_VALUE_TYPE_MISSING =
                new TypeWrite(15, "The attribute type '%s' is missing a value type.");
        public static final TypeWrite ATTRIBUTE_VALUE_TYPE_MODIFIED =
                new TypeWrite(16, "An attribute value type (in this case '%s') can only be set onto an attribute type (in this case '%s') when it is defined for the first time.");
        public static final TypeWrite ATTRIBUTE_VALUE_TYPE_UNDEFINED =
                new TypeWrite(17, "An attribute value type (in this case '%s') cannot be undefined. You can only undefine the attribute type (in this case '%s') itself.");
        public static final TypeWrite ATTRIBUTE_UNSET_ABSTRACT_HAS_SUBTYPES =
                new TypeWrite(18, "The attribute type '%s' cannot be set to non abstract as it has subtypes.");
        public static final TypeWrite ATTRIBUTE_NEW_SUPERTYPE_NOT_ABSTRACT =
                new TypeWrite(19, "The attribute type '%s' cannot be subtyped as it is not abstract.");
        public static final TypeWrite ATTRIBUTE_REGEX_UNSATISFIES_INSTANCES =
                new TypeWrite(20, "The attribute type '%s' cannot have regex '%s' as as it has an instance of value '%s'.");
        public static final TypeWrite ATTRIBUTE_VALUE_TYPE_DEFINED_NOT_ON_ATTRIBUTE_TYPE =
                new TypeWrite(21, "The type '%s' is not an attribute type, so it can not have a value type defined.");
        public static final TypeWrite ROOT_ATTRIBUTE_TYPE_CANNOT_BE_OWNED =
                new TypeWrite(22, "The native root 'attribute' type cannot be owned.");
        public static final TypeWrite ROOT_ROLE_TYPE_CANNOT_BE_PLAYED =
                new TypeWrite(23, "The native root 'role' type cannot be played.");
        public static final TypeWrite OWNS_ATTRIBUTE_WAS_OVERRIDDEN =
                new TypeWrite(24, "Type '%s' cannot own '%s' since it was overridden in a supertype, and cannot be redeclared as an ownership.");
        public static final TypeWrite REDUNDANT_OWNS_DECLARATION =
                new TypeWrite(25, "Type '%s' cannot redeclare inherited ownership '%s' without annotation specialisation.");
        public static final TypeWrite OWNS_VALUE_TYPE_NO_EXACT_EQUALITY =
                new TypeWrite(26, "Type '%s' cannot own '%s' with annotations '%s' since it has has value type '%s', which does not have an exact equality.");
        public static final TypeWrite OWNS_ANNOTATION_DECLARATION_INCOMPATIBLE =
                new TypeWrite(27, "Type '%s' cannot own '%s' with incompatible declared annotations '%s' and '%s'");
        public static final TypeWrite OWNS_OVERRIDE_ANNOTATIONS_REDUNDANT =
                new TypeWrite(28, "Type '%s' cannot declare ownership of '%s' with annotations '%s' since these annotations are inherited from overriding '%s'.");
        public static final TypeWrite OWNS_ANNOTATION_LESS_STRICT_THAN_PARENT =
                new TypeWrite(29, "Type '%s' cannot declare ownership of '%s' with annotations '%s' since this annotation is not stricter than the parent ownership '%s'.");
        public static final TypeWrite OWNS_KEY_PRECONDITION_OWNERSHIP_KEY_TOO_MANY =
                new TypeWrite(30, "Some instances of type '%s' have zero attributes of type '%s' to convert to key.");
        public static final TypeWrite OWNS_KEY_PRECONDITION_OWNERSHIP_KEY_MISSING =
                new TypeWrite(31, "Some instances of type '%s' have more than one attribute of type '%s' to convert to key.");
        public static final TypeWrite OWNS_KEY_PRECONDITION_UNIQUENESS =
                new TypeWrite(32, "The attributes of type '%s' are not uniquely owned by instances of type '%s' to convert to key.");
        public static final TypeWrite OWNS_UNIQUE_PRECONDITION =
                new TypeWrite(33, "The attributes of type '%s' are not uniquely owned by instances of type '%s' to convert to unique.");
        public static final TypeWrite ILLEGAL_ROLE_TYPE_ALIAS =
                new TypeWrite(34, "The role type '%s' cannot be used as an alias for the inherited role type '%s' - use the inherited role type or define a new role type overriding it with a new name.");
        public static final TypeWrite PLAYS_ROLE_NOT_AVAILABLE_OVERRIDDEN =
                new TypeWrite(35, "Type '%s' cannot declare plays role type '%s' as it has been overridden and cannot be redeclared.");
        public static final TypeWrite PLAYS_ABSTRACT_ROLE_TYPE =
                new TypeWrite(36, "The type '%s' is not abstract, and thus cannot play an abstract role type '%s'.");
        public static final TypeWrite RELATION_NO_ROLE =
                new TypeWrite(37, "The relation type '%s' does not relate any role type.");
        public static final TypeWrite RELATION_ABSTRACT_ROLE =
                new TypeWrite(38, "The relation type '%s' is not abstract, and thus cannot relate an abstract role type '%s'.");
        public static final TypeWrite RELATION_RELATES_ROLE_FROM_SUPERTYPE =
                new TypeWrite(39, "The role type '%s' is already declared by a supertype of '%s'.");
        public static final TypeWrite RELATION_RELATES_ROLE_NOT_AVAILABLE =
                new TypeWrite(40, "The role type '%s' cannot override '%s' as it is either directly related or not inherited.");
        public static final TypeWrite ROLE_DEFINED_OUTSIDE_OF_RELATION =
                new TypeWrite(41, "The role type '%s' cannot be defined/undefined outside the scope of its relation type.");
        public static final TypeWrite INVALID_DEFINE_SUB =
                new TypeWrite(42, "The type '%s' cannot be defined, as the provided supertype '%s' is not a valid thing type.");
        public static final TypeWrite INVALID_UNDEFINE_SUB =
                new TypeWrite(43, "The type '%s' cannot be undefined, as the provided supertype '%s' is not a valid supertype.");
        public static final TypeWrite INVALID_UNDEFINE_RELATES_OVERRIDE =
                new TypeWrite(44, "The overridden related role type '%s' cannot be undefined. You should re-define relating '%s' without overriding.");
        public static final TypeWrite INVALID_UNDEFINE_PLAYS_OVERRIDE =
                new TypeWrite(45, "The overridden played role type '%s' cannot be undefined. You should re-define playing '%s' without overriding.");
        public static final TypeWrite INVALID_UNDEFINE_OWNS_OVERRIDE =
                new TypeWrite(46, "The overridden owned attribute type '%s' cannot be undefined. You should re-define owning '%s' without overriding.");
        public static final TypeWrite INVALID_UNDEFINE_ANNOTATIONS =
                new TypeWrite(47, "Annotations cannot be undefined on '%s'. You should re-define the pattern without the annotations.");
        public static final TypeWrite INVALID_UNDEFINE_RELATES_HAS_INSTANCES =
                new TypeWrite(48, "The role type '%s' cannot be undefined because it is currently played by existing instances.");
        public static final TypeWrite INVALID_UNDEFINE_OWNS_HAS_INSTANCES =
                new TypeWrite(49, "The ability of type '%s' to own attribute type '%s' cannot be undefined because there are data instances using this ownership.");
        public static final TypeWrite INVALID_UNDEFINE_PLAYS_HAS_INSTANCES =
                new TypeWrite(50, "The ability of type '%s' to play role type '%s' cannot be undefined because it is currently played by existing instances.");
        public static final TypeWrite INVALID_UNDEFINE_INHERITED_OWNS =
                new TypeWrite(51, "The ability of type '%s' to own attribute type '%s' cannot be undefined because the ability is inherited from a supertype.");
        public static final TypeWrite INVALID_UNDEFINE_INHERITED_PLAYS =
                new TypeWrite(52, "The ability of type '%s' to play role type '%s' cannot be undefined because the ability is inherited from a supertype.");
        public static final TypeWrite INVALID_UNDEFINE_NONEXISTENT_OWNS =
                new TypeWrite(53, "The ability of type '%s' to own attribute type '%s' cannot be undefined because it does not have that ability.");
        public static final TypeWrite INVALID_UNDEFINE_NONEXISTENT_PLAYS =
                new TypeWrite(54, "The ability of type '%s' to play role type '%s' cannot be undefined because it does not have that ability.");
        public static final TypeWrite TYPE_CONSTRAINT_UNACCEPTED =
                new TypeWrite(55, "The type constraint '%s' is not accepted in a define/undefine query.");
        public static final TypeWrite ILLEGAL_SUPERTYPE_ENCODING =
                new TypeWrite(56, "Unable to set type with class '%s' as a supertype.");
        public static final TypeWrite SCHEMA_VALIDATION_INVALID_DEFINE =
                new TypeWrite(57, "Defining '%s' failed because resulting schema would be invalid: %s.");
        public static final TypeWrite SCHEMA_VALIDATION_INVALID_UNDEFINE =
                new TypeWrite(58, "Undefining '%s' failed because the resulting schema would be invalid: %s.");
        public static final TypeWrite SCHEMA_VALIDATION_INVALID_SET_SUPERTYPE =
                new TypeWrite(59, "Setting the supertype of '%s' to '%s' failed because the resulting schema would be invalid: %s.");
        public static final TypeWrite OVERRIDDEN_PLAYED_ROLE_NOT_AVAILABLE =
                new TypeWrite(60, "The type '%s' cannot override playing '%s' with '%s' as it is either directly declared or not inherited.");
        public static final TypeWrite OVERRIDDEN_OWNED_ATTRIBUTE_NOT_AVAILABLE =
                new TypeWrite(61, "The type '%s' cannot override the ownership of '%s' with '%s' as it is either directly declared or not inherited.");
        public static final TypeWrite REDUNDANT_PLAYS_DECLARATION =
                new TypeWrite(62, "Type '%s' cannot declare plays role type '%s' as it is already inherited.");
        public static final TypeWrite MAX_SUBTYPE_REACHED =
                new TypeWrite(63, "The maximum number of '%s' types has been reached: '%s'.");

        private static final String codePrefix = "TYW";
        private static final String messagePrefix = "Invalid Type Write";

        TypeWrite(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class ValueRead extends ErrorMessage {
        public static final ValueRead INVALID_VALUE_CASTING =
                new ValueRead(1, "Invalid value conversion from '%s' to '%s'.");
        public static final ValueRead INVALID_VALUE_IID_CASTING =
                new ValueRead(2, "Invalid value IID casting to '%s'.");

        private static final String codePrefix = "VLR";
        private static final String messagePrefix = "Invalid value read";

        ValueRead(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class RuleRead extends ErrorMessage {
        public static final RuleRead RULE_NOT_FOUND =
                new RuleRead(1, "The rule with label '%s' is not found.");

        private static final String codePrefix = "RUR";
        private static final String messagePrefix = "Invalid Rule Read";

        RuleRead(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class RuleWrite extends ErrorMessage {
        public static final RuleWrite INVALID_UNDEFINE_RULE_BODY =
                new RuleWrite(1, "The rule body of '%s' ('when' or 'then') cannot be undefined. The rule must be undefined entirely by referring to its label.");
        public static final RuleWrite CONTRADICTORY_RULE_CYCLE =
                new RuleWrite(2, "A cycle containing negation(s) that can cause inference contradictions has been detected in rules: %s");
        public static final RuleWrite RULE_CONCLUSION_ILLEGAL_INSERT =
                new RuleWrite(3, "The conclusion of rule '%s' may insert types '%s', which is not allowed in the current schema.");
        public static final RuleWrite RULE_WHEN_INCOHERENT =
                new RuleWrite(4, "The rule '%s' has a when clause '%s' that is illegal in the current schema.");
        public static final RuleWrite RULE_WHEN_UNANSWERABLE =
                new RuleWrite(5, "The rule '%s' has a when clause '%s' that can never have answers in the current schema.");
        public static final RuleWrite RULE_WHEN_UNANSWERABLE_BRANCH =
                new RuleWrite(6, "The rule '%s' has a branch in the when clause '%s' that can never have answers in the current schema.");
        public static final RuleWrite RULE_THEN_INCOHERENT =
                new RuleWrite(7, "The rule '%s' has a then clause '%s' that can never be satisfied in the current schema.");
        public static final RuleWrite RULE_THEN_INSERTS_ABSTRACT_TYPES =
                new RuleWrite(8, "The rule '%s' has a then clause '%s' which tries to insert instances of types '%s', some of which are abstract.");
        public static final RuleWrite RULE_THEN_INVALID_VALUE_ASSIGNMENT =
                new RuleWrite(9, "The rule '%s' has a then clause with an invalid assignment of '%s' into a '%s'.");
        public static final RuleWrite MAX_RULE_REACHED =
                new RuleWrite(10, "The maximum number of rules has been reached: '%s'");

        private static final String codePrefix = "RUW";
        private static final String messagePrefix = "Invalid Rule Write";

        RuleWrite(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class Reasoner extends ErrorMessage {
        public static final Reasoner REASONING_CANNOT_BE_TOGGLED_PER_QUERY =
                new Reasoner(1, "Reasoning cannot be enabled/disabled per query. Try using Transaction options instead.");
        public static final Reasoner REVERSE_UNIFICATION_MISSING_CONCEPT =
                new Reasoner(2, "Reverse unification failed because a concept for identifier '%s' was not found in the provided map '%s'.");
        public static final Reasoner REASONING_TERMINATED_WITH_CAUSE =
                new Reasoner(3, "Reasoning is terminated, caused by '%s'.");
        public static final Reasoner REASONER_TRACING_CANNOT_BE_TOGGLED_PER_QUERY =
                new Reasoner(4, "Reasoner tracing cannot be enabled/disabled per query. Try using Transaction options instead.");
        public static final Reasoner REASONER_TRACING_DIRECTORY_COULD_NOT_BE_FOUND =
                new Reasoner(5, "Reasoner tracing could not find or create the log directory provided.");
        public static final Reasoner REASONER_TRACING_FILE_COULD_NOT_BE_FOUND =
                new Reasoner(6, "Reasoner tracing file could not be found.");
        public static final Reasoner REASONER_TRACING_WRITE_FAILED =
                new Reasoner(7, "Reasoner tracing failed to write to file.");

        private static final String codePrefix = "RSN";
        private static final String messagePrefix = "Reasoner Error";

        Reasoner(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class Migrator extends ErrorMessage {
        public static final Migrator DATABASE_NOT_FOUND =
                new Migrator(1, "The database '%s' was not found.");
        public static final Migrator FILE_NOT_FOUND =
                new Migrator(2, "The specified file path '%s' could not be found.");
        public static final Migrator FILE_READ_ERROR =
                new Migrator(3, "Error reading the file at '%s'.");
        public static final Migrator FILE_WRITE_ERROR =
                new Migrator(4, "Error writing the file at '%s'.");
        public static final Migrator TYPE_NOT_FOUND =
                new Migrator(5, "The type '%s' is not defined in the schema.");
        public static final Migrator ROLE_TYPE_NOT_FOUND =
                new Migrator(6, "The role type '%s'is not defined for relation type '%s. Please confirm schema was migrated correctly.");
        public static final Migrator PLAYER_NOT_FOUND =
                new Migrator(7, "A player for relation type '%s' was expected but not found.");
        public static final Migrator NO_PLAYERS =
                new Migrator(8, "The relation of type '%s' with original ID '%s' has no role players");
        public static final Migrator INVALID_DATA =
                new Migrator(9, "The data being imported is invalid.");
        public static final Migrator MISSING_HEADER =
                new Migrator(10, "The data being imported is invalid - the header is missing.");
        public static final Migrator IMPORT_CHECKSUM_MISMATCH =
                new Migrator(11, "The import has finished but mismatches the required checksums: '%s");

        private static final String codePrefix = "MIG";
        private static final String messagePrefix = "Migrator failure";

        Migrator(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class Encoding extends ErrorMessage {
        public static final Encoding ILLEGAL_STRING_SIZE =
                new Encoding(1, "Attempted to insert a string larger than the maximum possible size: %s bytes.");
        public static final Encoding UNENCODABLE_STRING =
                new Encoding(2, "The string '%s' cannot be encoded to bytes using the encoding '%s'.");
        public static final Encoding UNENCODABLE_DOUBLE =
                new Encoding(3, "The double '%s' cannot be encoded.");

        private static final String codePrefix = "ENC";
        private static final String messagePrefix = "Data encoding error";

        Encoding(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }
}
