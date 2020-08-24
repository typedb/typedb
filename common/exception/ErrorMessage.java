/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.common.exception;

public abstract class ErrorMessage extends grakn.common.exception.ErrorMessage {

    private ErrorMessage(String codePrefix, int codeNumber, String messagePrefix, String messageBody) {
        super(codePrefix, codeNumber, messagePrefix, messageBody);
    }

    public static class Server extends ErrorMessage {
        public static final Server DATABASE_DIRECTORY_NOT_FOUND =
                new Server(1, "The database directory '%s' does not exist.");
        public static final Server EXITED_WITH_ERROR =
                new Server(2, "Exited with error.");
        public static final Server UNCAUGHT_EXCEPTION =
                new Server(3, "Uncaught exception thrown at thread '%s'.");
        public static final Server FAILED_AT_STOPPING =
                new Server(4, "Exception occurred while attempting to stop the server");
        public static final Server PROPERTIES_FILE_NOT_FOUND =
                new Server(5, "Could not find/read default properties file '%s'.");
        public static final Server FAILED_PARSE_PROPERTIES =
                new Server(6, "Failed at parsing properties file.");
        public static final Server ENV_VAR_NOT_FOUND =
                new Server(7, "Environment variable '%s' is not defined.");
        public static final Server SERVER_SHUTDOWN =
                new Server(8, "Grakn Core server has been shutdown.");
        public static final Server MISSING_CONCEPT =
                new Server(9, "Concept does not exist.");
        public static final Server BAD_VALUE_TYPE =
                new Server(10, "A value type was not correctly set.");
        public static final Server UNKNOWN_REQUEST_TYPE =
                new Server(11, "The request message was not recognized.");

        private static final String codePrefix = "SRV";
        private static final String messagePrefix = "Server Error";

        Server(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class Internal extends ErrorMessage {
        public static final Internal ILLEGAL_STATE =
                new Internal(1, "Illegal internal state!");
        public static final Internal UNRECOGNISED_VALUE =
                new Internal(2, "Unrecognised schema value!");
        public static final Internal DIRTY_INITIALISATION =
                new Internal(3, "Invalid Database Initialisation.");
        public static final Internal ILLEGAL_ARGUMENT =
                new Internal(4, "Illegal argument provided.");

        private static final String codePrefix = "INT";
        private static final String messagePrefix = "Invalid Internal State";

        Internal(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class DatabaseManager extends ErrorMessage {
        public static final DatabaseManager DATABASE_EXISTS =
                new DatabaseManager(1, "The database with the name '%s' already exists.");
        public static final DatabaseManager DATABASE_NOT_FOUND =
                new DatabaseManager(2, "The database with the name '%s' does not exist.");
        public static final DatabaseManager DATABASE_DELETED =
                new DatabaseManager(3, "Database with the name '%s' has been deleted.");

        private static final String codePrefix = "DBS";
        private static final String messagePrefix = "Invalid Database Operations";

        DatabaseManager(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class Session extends ErrorMessage {
        public static final Server SESSION_NOT_FOUND =
                new Server(1, "Session with UUID '%s' does not exist.");

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
        public static final Transaction TRANSACTION_ALREADY_OPENED =
                new Transaction(3, "Transaction has already been already opened.");
        public static final Transaction TRANSACTION_CLOSED =
                new Transaction(4, "The transaction has been closed and no further operation is allowed.");
        public static final Transaction ILLEGAL_COMMIT =
                new Transaction(5, "Only write transactions can be committed.");
        public static final Transaction DIRTY_SCHEMA_WRITES =
                new Transaction(6, "Attempted schema writes when session type does not allow.");
        public static final Transaction DIRTY_DATA_WRITES =
                new Transaction(7, "Attempted data writes when session type does not allow.");
        public static final Transaction UNEXPECTED_NULL =
                new Transaction(8, "Unexpected NULL object.");

        private static final String codePrefix = "TXN";
        private static final String messagePrefix = "Invalid Transaction Operation";

        Transaction(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class ThingRead extends ErrorMessage {
        public static final ThingRead INVALID_IID_CASTING =
                new ThingRead(1, "'Invalid IID casting to '%s'.");
        public static final ThingRead INVALID_VERTEX_CASTING =
                new ThingRead(2, "Invalid ThingVertex casting to '%s'.");
        public static final ThingRead INVALID_THING_CASTING =
                new ThingRead(3, "Invalid concept conversion to '%s'.");
        public static final ThingWrite THING_NOT_FOUND =
                new ThingWrite(4, "The thing with IID '%s' is not found.");

        private static final String codePrefix = "THR";
        private static final String messagePrefix = "Invalid Thing Read";

        ThingRead(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class ThingWrite extends ErrorMessage {
        public static final ThingWrite ILLEGAL_ABSTRACT_WRITE =
                new ThingWrite(1, "Attempted an illegal write of a new '%s' of abstract type '%s'.");
        public static final ThingWrite ILLEGAL_STRING_SIZE =
                new ThingWrite(2, "Attempted to insert a string larger than the maximum size.");
        public static final ThingWrite CONCURRENT_ATTRIBUTE_WRITE_DELETE =
                new ThingWrite(3, "Attempted concurrent modification of attributes (writes and deletes).");
        public static final ThingWrite THING_ATTRIBUTE_UNDEFINED =
                new ThingWrite(4, "Attempted to assign an attribute to a(n) '%s' that is not defined to have that attribute type.");
        public static final ThingWrite THING_KEY_OVER =
                new ThingWrite(5, "Attempted to assign a key of type '%s' onto a(n) '%s' that already has one.");
        public static final ThingWrite THING_KEY_TAKEN =
                new ThingWrite(6, "Attempted to assign a key of type '%s' that had been taken by another '%s'.");
        public static final ThingWrite THING_KEY_MISSING =
                new ThingWrite(7, "Attempted to commit a(n) '%s' that is missing key(s) of type(s): %s"); // don't put quotes around the last %s
        public static final ThingWrite RELATION_UNRELATED_ROLE =
                new ThingWrite(8, "Relation type '%s' does not relate role type '%s'.");
        public static final ThingWrite RELATION_NO_PLAYER =
                new ThingWrite(9, "Relation instance of type '%s' does not have any role player");
        public static final ThingWrite ATTRIBUTE_VALUE_UNSATISFIES_REGEX =
                new ThingWrite(10, "Attempted to put an instance of '%s' with value '%s' that does not satisfy the regular expression '%s'.");
        public static final ThingWrite THING_ISA_IID_CONFLICT =
                new ThingWrite(11, "Attempted to refer to a thing with IID '%s' and assert it as a new instance (isa) of type '%s' at the same time.");
        public static final ThingWrite THING_ISA_MISSING =
                new ThingWrite(12, "The thing variable '%s' cannot be asserted as a new instance with providing its type (isa).");
        public static final ThingWrite THING_PROPERTY_TYPE_VARIABLE =
                new ThingWrite(13, "Types can only be referred to by their labels in insert queries, unlike the the type variable '%s'.");
        public static final ThingWrite THING_PROPERTY_UNACCEPTED =
                new ThingWrite(14, "The thing property '%s' is not accepted in an insert query.");
        public static final ThingWrite ATTRIBUTE_VALUE_MISSING =
                new ThingWrite(15, "Unable to insert attribute '%s' of type '%s' without a value assigned to the variable.");
        public static final ThingWrite RELATION_PROPERTY_MISSING =
                new ThingWrite(16, "Unable to insert relation '%s' as it is missing the relation tuple describing the role players.");
        public static final ThingWrite ROLE_TYPE_AMBIGUOUS =
                new ThingWrite(17, "Unable to add role player '%s' to the relation, as there are more than one possible role type it could play.");
        public static final ThingWrite ROLE_TYPE_MISSING =
                new ThingWrite(18, "Unable to add role player '%s' to the relation, as there is no provided or inferrable role type.");
        public static final ThingWrite ATTRIBUTE_TYPE_MISMATCH =
                new ThingWrite(19, "The attribute '%s' with type '%s' cannot be owned as type '%s'.");
        private static final String codePrefix = "THW";
        private static final String messagePrefix = "Invalid Thing Write";

        ThingWrite(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class TypeRead extends ErrorMessage {
        public static final TypeRead INVALID_TYPE_CASTING =
                new TypeRead(1, "Invalid concept conversion to '%s'.");
        public static final TypeRead TYPE_ROOT_MISMATCH =
                new TypeRead(2, "Attempted to retrieve '%s' as '%s', while it is actually a(n) '%s'.");
        public static final TypeRead TYPE_NOT_FOUND =
                new TypeRead(3, "The type '%s' is not found.");
        public static final TypeRead VALUE_TYPE_MISMATCH =
                new TypeRead(4, "Attempted to retrieve '%s' as AttributeType of ValueType '%s', while it actually has ValueType '%s'.");
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
        public static final TypeWrite TYPE_HAS_INSTANCES =
                new TypeWrite(3, "The type '%s' has instances, and cannot be deleted.");
        public static final TypeWrite SUPERTYPE_SELF =
                new TypeWrite(4, "The type '%s' cannot be a subtype of itself.");
        public static final TypeWrite OWNS_ABSTRACT_ATT_TYPE =
                new TypeWrite(5, "The type '%s' is not abstract, and thus cannot own an abstract attribute type '%s'.");
        public static final TypeWrite OVERRIDDEN_NOT_SUPERTYPE =
                new TypeWrite(6, "The type '%s' cannot override '%s' as it is not a supertype.");
        public static final TypeWrite OVERRIDE_NOT_AVAILABLE = // TODO: this can be split between 'has', 'key' and 'plays' once pushed to commit
                new TypeWrite(7, "The type '%s' cannot override '%s' as it is either directly declared or not inherited.");
        public static final TypeWrite ATTRIBUTE_SUPERTYPE_VALUE_TYPE =
                new TypeWrite(8, "The attribute type '%s' has value type '%s', and cannot have supertype '%s' with value type '%s'.");
        public static final TypeWrite ATTRIBUTE_VALUE_TYPE_MISSING =
                new TypeWrite(9, "The attribute type '%s' is missing a value type.");
        public static final TypeWrite ATTRIBUTE_VALUE_TYPE_MODIFIED =
                new TypeWrite(10, "An attribute value type (in this case '%s') can only be set onto an attribute type (in this case '%s') when it was defined for the first time.");
        public static final TypeWrite ATTRIBUTE_VALUE_TYPE_UNDEFINED =
                new TypeWrite(11, "An attribute value type (in this case '%s') cannot be undefine. You can only undefine the attribute type (in this case '%s') itself.");
        public static final TypeWrite ATTRIBUTE_SUPERTYPE_NOT_ABSTRACT =
                new TypeWrite(12, "The attribute type '%s' cannot be a subtyped as it is not abstract.");
        public static final TypeWrite ATTRIBUTE_REGEX_UNSATISFIES_INSTANCES =
                new TypeWrite(13, "The attribute type '%s' cannot have regex '%s' as as it has an instance of value '%s'.");
        public static final TypeWrite OWNS_KEY_VALUE_TYPE =
                new TypeWrite(14, "The attribute type '%s' has value type '%s', and cannot and cannot be used as a type key.");
        public static final TypeWrite OWNS_KEY_NOT_AVAILABLE =
                new TypeWrite(15, "The attribute type '%s' has been inherited or overridden, and cannot be redeclared as a key.");
        public static final TypeWrite OWNS_KEY_PRECONDITION_OWNERSHIP =
                new TypeWrite(16, "The instances of type '%s' does not have exactly one attribute of type '%s' to convert to key.");
        public static final TypeWrite OWNS_ATT_NOT_AVAILABLE =
                new TypeWrite(17, "The attribute type '%s' has been inherited or overridden, and cannot be redeclared as an attribute.");
        public static final TypeWrite OWNS_KEY_PRECONDITION_UNIQUENESS =
                new TypeWrite(18, "The attributes of type '%s' are not uniquely owned by instances of type '%s' to convert to key.");
        public static final TypeWrite PLAYS_ROLE_NOT_AVAILABLE =
                new TypeWrite(19, "The role type '%s' has been inherited or overridden, and cannot be redeclared.");
        public static final TypeWrite PLAYS_ABSTRACT_ROLE_TYPE =
                new TypeWrite(20, "The type '%s' is not abstract, and thus cannot play an abstract role type '%s'.");
        public static final TypeWrite RELATION_NO_ROLE =
                new TypeWrite(21, "The relation type '%s' does not relate any role type.");
        public static final TypeWrite RELATION_ABSTRACT_ROLE =
                new TypeWrite(22, "The relation type '%s' is not abstract, and thus cannot relate an abstract role type '%s'.");
        public static final TypeWrite RELATION_RELATES_ROLE_FROM_SUPERTYPE =
                new TypeWrite(23, "The role type '%s' is already declared by a supertype.");
        public static final TypeWrite RELATION_RELATES_ROLE_NOT_AVAILABLE =
                new TypeWrite(24, "The role type '%s' cannot override '%s' as it is either directly related or not inherited.");
        public static final TypeWrite ROLE_DEFINED_OUTSIDE_OF_RELATION =
                new TypeWrite(25, "The role type '%s' cannot be defined/undefined outside the scope of its relation type.");
        public static final TypeWrite INVALID_DEFINE_SUB =
                new TypeWrite(26, "The type '%s' cannot be defined, as the provided supertype '%s' is not a valid thing type.");
        public static final TypeWrite INVALID_UNDEFINE_SUB =
                new TypeWrite(27, "The type '%s' cannot be undefined, as the provided supertype '%s' is not a valid supertype.");
        public static final TypeWrite INVALID_UNDEFINE_REGEX =
                new TypeWrite(28, "The attribute type '%s' does have regular express '%s' to be undefined.");
        public static final TypeWrite INVALID_UNDEFINE_RELATES_OVERRIDE =
                new TypeWrite(29, "The overridden related role type '%s' cannot be undefined. You should re-define relating '%s' without overriding.");
        public static final TypeWrite INVALID_UNDEFINE_PLAYS_OVERRIDE =
                new TypeWrite(30, "The overridden played role type '%s' cannot be undefined. You should re-define playing '%s' without overriding.");
        public static final TypeWrite INVALID_UNDEFINE_OWNS_OVERRIDE =
                new TypeWrite(31, "The overridden owned attribute type '%s' cannot be undefined. You should re-define owning '%s' without overriding.");
        public static final TypeWrite INVALID_UNDEFINE_OWNS_KEY =
                new TypeWrite(32, "The annotation @key on attribute type '%s' cannot be undefined. You should re-define owning '%s' without @key annotation.");
        private static final String codePrefix = "TYW";
        private static final String messagePrefix = "Invalid Type Write";

        TypeWrite(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }
}
