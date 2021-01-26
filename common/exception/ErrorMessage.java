/*
 * Copyright (C) 2021 Grakn Labs
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

    public static void loadConstants() {
        for (Class<?> innerClass : ErrorMessage.class.getDeclaredClasses()) {
            try {
                Class.forName(innerClass.getName(), true, innerClass.getClassLoader());
            } catch (ClassNotFoundException e) {
                throw GraknException.of(e);
            }
        }
    }

    public static class Server extends ErrorMessage {
        public static final Server DATA_DIRECTORY_NOT_FOUND =
                new Server(1, "The expected data directory '%s' does not exist.");
        public static final Server DATA_DIRECTORY_NOT_WRITABLE =
                new Server(2, "The expected data directory '%s' is not writable.");
        public static final Server EXITED_WITH_ERROR =
                new Server(3, "Exited with error.");
        public static final Server UNCAUGHT_EXCEPTION =
                new Server(4, "Uncaught exception thrown at thread '%s'.");
        public static final Server FAILED_AT_STOPPING =
                new Server(5, "Exception occurred while attempting to stop the server");
        public static final Server PROPERTIES_FILE_NOT_FOUND =
                new Server(6, "Could not find/read default properties file '%s'.");
        public static final Server FAILED_PARSE_PROPERTIES =
                new Server(7, "Failed at parsing properties file.");
        public static final Server ENV_VAR_NOT_FOUND =
                new Server(8, "Environment variable '%s' is not defined.");
        public static final Server SERVER_SHUTDOWN =
                new Server(9, "Grakn Core server has been shutdown.");
        public static final Server MISSING_FIELD =
                new Server(10, "The request message does not contain the required field '%s'.");
        public static final Server MISSING_CONCEPT =
                new Server(11, "Concept does not exist.");
        public static final Server BAD_VALUE_TYPE =
                new Server(12, "The value type '%s' was not recognised.");
        public static final Server UNKNOWN_ANSWER_TYPE =
                new Server(13, "The answer type '%s' was not recognised.");
        public static final Server UNKNOWN_REQUEST_TYPE =
                new Server(14, "The request message was not recognised.");
        public static final Server ITERATION_WITH_UNKNOWN_ID =
                new Server(15, "Iteration was requested for ID '%s', but this ID does not correspond to an existing query iterator.");
        public static final Server DUPLICATE_REQUEST =
                new Server(16, "The request with ID '%s' is a duplicate.");

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
        public static final Internal UNRECOGNISED_VALUE =
                new Internal(5, "Unrecognised encoding value!");
        public static final Internal DIRTY_INITIALISATION =
                new Internal(6, "Invalid Database Initialisation.");
        public static final Internal GRAKN_CLOSED =
                new Internal(7, "Attempted to open a session on a closed Grakn backend.");
        public static final Internal OUT_OF_BOUNDS =
                new Internal(8, "Resource out of bounds.");
        public static final Internal UNEXPECTED_INTERRUPTION =
                new Internal(9, "Unexpected thread interruption!");
        public static final Internal UNEXPECTED_PLANNING_ERROR =
                new Internal(10, "Unexpected error during traversal plan optimisation.");
        public static final Internal UNIMPLEMENTED =
                new Internal(11, "This functionality is not yet implemented.");

        private static final String codePrefix = "INT";
        private static final String messagePrefix = "Invalid Internal State";

        Internal(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class Database extends ErrorMessage {
        public static final Database DATABASE_EXISTS =
                new Database(1, "The database with the name '%s' already exists.");
        public static final Database DATABASE_NOT_FOUND =
                new Database(2, "The database with the name '%s' does not exist.");
        public static final Database DATABASE_DELETED =
                new Database(3, "Database with the name '%s' has been deleted.");
        public static final Database DATABASE_CLOSED =
                new Database(4, "Attempted to open a new session from the database '%s' that has been closed.");

        private static final String codePrefix = "DBS";
        private static final String messagePrefix = "Invalid Database Operations";

        Database(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class Session extends ErrorMessage {
        public static final Session SESSION_NOT_FOUND =
                new Session(1, "Session with UUID '%s' does not exist.");
        public static final Session SESSION_CLOSED =
                new Session(2, "Attempted to open a transaction from closed session.");

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
        public static final Transaction SESSION_DATA_VIOLATION =
                new Transaction(9, "Attempted schema writes when session type does not allow.");
        public static final Transaction SESSION_SCHEMA_VIOLATION =
                new Transaction(10, "Attempted data writes when session type does not allow.");
        public static final Transaction MISSING_TRANSACTION =
                new Transaction(11, "Transaction can not be null.");
        public static final Transaction BAD_TRANSACTION_TYPE =
                new Transaction(12, "The transaction type '%s' was not recognised.");

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
        public static final Pattern ILLEGAL_DERIVED_THING_CONSTRAINT_ISA =
                new Pattern(10, "The thing variable '%s' has a derived 'isa' constraint, in a query that does not allow it.");
        public static final Pattern MULTIPLE_TYPE_CONSTRAINT_SUB =
                new Pattern(11, "The type variable '%s' has multiple 'sub' constraints.");
        public static final Pattern MULTIPLE_TYPE_CONSTRAINT_LABEL =
                new Pattern(12, "The type variable '%s' has multiple 'label' constraints.");
        public static final Pattern MULTIPLE_TYPE_CONSTRAINT_VALUE_TYPE =
                new Pattern(13, "Tye type variable '%s' has multiple 'value' constraints.");
        public static final Pattern MULTIPLE_TYPE_CONSTRAINT_REGEX =
                new Pattern(14, "The type variable '%s' has multiple 'regex' constraints.");
        public static final Pattern UNSATISFIABLE_CONJUNCTION =
                new Pattern(15, "The conjunction %s can never be satisfied within the current schema.");
        public static final Pattern UNSATISFIABLE_CONSTRAINT_VALUE_TYPE =
                new Pattern(16, "The value type in this constraint %s can not be satisfied within the current conjunction.");

        private static final String codePrefix = "QRY";
        private static final String messagePrefix = "Invalid Query Pattern";

        Pattern(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class ThingRead extends ErrorMessage {
        public static final ThingRead INVALID_THING_IID_CASTING =
                new ThingRead(1, "Invalid Thing IID casting to '%s'.");
        public static final ThingRead INVALID_THING_VERTEX_CASTING =
                new ThingRead(2, "Invalid ThingVertex casting to '%s'.");
        public static final ThingRead INVALID_THING_CASTING =
                new ThingRead(3, "Invalid concept conversion from '%s' to '%s'.");
        public static final ThingRead THING_NOT_FOUND =
                new ThingRead(4, "The thing with IID '%s' is not found.");
        public static final ThingRead INVALID_ROLE_TYPE_LABEL =
                new ThingRead(5, "The role type '%s' is not scoped by its relation type.");
        public static final ThingRead CONTRADICTORY_BOUND_VARIABLE =
                new ThingRead(6, "The nested variable '%s' contradicts the type of its bound variable.");
        public static final ThingRead SORT_VARIABLE_NOT_ATTRIBUTE =
                new ThingRead(7, "The variable '%s' cannot be used to sort, as it is not an attribute.");
        public static final ThingRead SORT_ATTRIBUTE_NOT_COMPARABLE =
                new ThingRead(8, "The variable '%s' cannot be used to sort, as it contains attributes that are not comparable.");
        public static final ThingRead AGGREGATE_ATTRIBUTE_NOT_NUMBER =
                new ThingRead(9, "The variable '%s' cannot be used to calculate aggregate functions, as it contains non-numeric values.");
        public static final ThingRead NUMERIC_IS_NOT_NUMBER =
                new ThingRead(10, "The numeric value is not a valid number (NaN).");

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
                new ThingWrite(2, "Attempted to insert a string larger than the maximum possible size: %s bytes.");
        public static final ThingWrite CONCURRENT_ATTRIBUTE_WRITE_DELETE = // TODO: should we remove this?
                new ThingWrite(3, "Attempted concurrent modification of attributes (writes and deletes).");
        public static final ThingWrite THING_CANNOT_OWN_ATTRIBUTE =
                new ThingWrite(4, "Attribute of type '%s' is not defined to be owned by type '%s'.");
        public static final ThingWrite THING_KEY_OVER =
                new ThingWrite(5, "Attempted to assign a key of type '%s' onto a(n) '%s' that already has one.");
        public static final ThingWrite THING_KEY_TAKEN =
                new ThingWrite(6, "Attempted to assign a key of type '%s' that had been taken by another '%s'.");
        public static final ThingWrite THING_KEY_MISSING =
                new ThingWrite(7, "Attempted to commit a(n) '%s' that is missing key(s) of type(s): %s"); // don't put quotes around the last %s
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
        public static final ThingWrite THING_ISA_REINSERTION =
                new ThingWrite(13, "Attempted to re-insert pre-existing thing of matched variable '%s' as a new instance (isa) of type '%s'.");
        public static final ThingWrite THING_ISA_MISSING =
                new ThingWrite(14, "The thing variable '%s' cannot be inserted as a new instance without providing its type (isa).");
        public static final ThingWrite ILLEGAL_TYPE_VARIABLE_IN_INSERT =
                new ThingWrite(15, "Illegal type variable '%s' found in insert query. Types can only be referred to by their labels in insert queries or then clauses.");
        public static final ThingWrite ILLEGAL_TYPE_VARIABLE_IN_DELETE =
                new ThingWrite(16, "Illegal type variable '%s' found in delete query. Types can only be referred to by their labels in delete queries or then clauses.");
        public static final ThingWrite ILLEGAL_ANONYMOUS_RELATION_IN_DELETE =
                new ThingWrite(17, "Illegal anonymous relation in delete query: '%s'.  You must match the relation variable by name, and then delete it.");
        public static final ThingWrite ILLEGAL_ANONYMOUS_VARIABLE_IN_DELETE =
                new ThingWrite(18, "Illegal anonymous variable in delete query: '%s'.  You can only delete named variables that were matched.");
        public static final ThingWrite INVALID_DELETE_THING =
                new ThingWrite(19, "The thing '%s' cannot be deleted, as the provided type '%s' is not a valid type or supertype.");
        public static final ThingWrite INVALID_DELETE_THING_DIRECT =
                new ThingWrite(20, "The thing '%s' cannot be deleted, as the provided direct type '%s' is not valid.");
        public static final ThingWrite INVALID_DELETE_HAS =
                new ThingWrite(21, "Invalid attempt to delete attribute ownership. The thing '%s' does not have attribute '%s'.");
        public static final ThingWrite ILLEGAL_IS_CONSTRAINT =
                new ThingWrite(22, "The 'is' constraint, e.g. used in '%s', is not accepted in an insert/delete query.");
        public static final ThingWrite ATTRIBUTE_VALUE_TOO_MANY =
                new ThingWrite(23, "Unable to insert attribute '%s' of type '%s' with more than one value operations.");
        public static final ThingWrite ATTRIBUTE_VALUE_MISSING =
                new ThingWrite(24, "Unable to insert attribute '%s' of type '%s' without a value assigned to the variable.");
        public static final ThingWrite INSERT_RELATION_CONSTRAINT_TOO_MANY =
                new ThingWrite(25, "Unable to insert relation '%s' as it has more than one relation tuple describing the role players.");
        public static final ThingWrite RELATION_CONSTRAINT_MISSING =
                new ThingWrite(26, "Unable to insert relation '%s' as it is missing the relation tuple describing the role players.");
        public static final ThingWrite ROLE_TYPE_AMBIGUOUS =
                new ThingWrite(27, "Unable to add role player '%s' to the relation, as there are more than one possible role type it could play.");
        public static final ThingWrite ROLE_TYPE_MISSING =
                new ThingWrite(28, "Unable to add role player '%s' to the relation, as there is no provided or inferrable role type.");
        public static final ThingWrite MAX_INSTANCE_REACHED =
                new ThingWrite(29, "The maximum number of instances for type '%s' has been reached: '%s'");
        public static final ThingWrite DELETE_RELATION_CONSTRAINT_TOO_MANY =
                new ThingWrite(30, "Could not perform delete of role players due to multiple relation constraints being present for relation '%s'.");
        public static final ThingWrite DELETE_ROLEPLAYER_NOT_PRESENT =
                new ThingWrite(31, "Could not delete roleplayer '%s' as relation '%s' does not relate it.");

        private static final String codePrefix = "THW";
        private static final String messagePrefix = "Invalid Thing Write";

        ThingWrite(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class SchemaGraph extends ErrorMessage {
        public static final SchemaGraph INVALID_SCHEMA_IID_CASTING =
                new SchemaGraph(1, "Invalid Schema IID cast to '%s'.");
        public static final SchemaGraph INVALID_SCHEMA_WRITE =
                new SchemaGraph(2, "The label '%s' is already in use in the schema graph.");

        private static final String codePrefix = "SCG";
        private static final String messagePrefix = "Invalid Schema Graph Operation";

        SchemaGraph(int number, String message) { super(codePrefix, number, messagePrefix, message); }
    }

    public static class TypeRead extends ErrorMessage {
        public static final TypeRead INVALID_TYPE_CASTING =
                new TypeRead(1, "Invalid concept conversion from '%s' to '%s'.");
        public static final TypeRead TYPE_ROOT_MISMATCH =
                new TypeRead(2, "Attempted to retrieve '%s' as '%s', while it is actually a(n) '%s'.");
        public static final TypeRead TYPE_NOT_FOUND =
                new TypeRead(3, "The type '%s' does not exist.");
        public static final TypeRead ROLE_TYPE_NOT_FOUND =
                new TypeRead(4, "There are no role types of label '%s' in the scope of '%s'");
        public static final TypeRead TYPE_NOT_RESOLVABLE =
                new TypeRead(5, "The type for variable '%s' is not resolvable.");
        public static final TypeRead TYPE_NOT_ATTRIBUTE_TYPE =
                new TypeRead(6, "The type '%s' is not a valid attribute type.");
        public static final TypeRead VALUE_TYPE_MISMATCH =
                new TypeRead(7, "Attempted to retrieve '%s' as AttributeType of ValueType '%s', while it actually has ValueType '%s'.");
        public static final TypeRead OVERRIDDEN_TYPES_IN_TRAVERSAL =
                new TypeRead(8, "Attempted to query for an overridden type through a traversal. Overridden types cannot be queried via Graql Match.");
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
        public static final TypeWrite TYPE_HAS_INSTANCES =
                new TypeWrite(3, "The type '%s' has instances, and cannot be deleted.");
        public static final TypeWrite TYPE_REFERENCED_IN_RULES =
                new TypeWrite(4, "The type '%s' is referenced in rules '%s', and cannot be deleted.");
        public static final TypeWrite CYCLIC_TYPE_HIERARCHY =
                new TypeWrite(5, "There is a cyclic type hierarchy, which is not allowed: '%s'.");
        public static final TypeWrite OWNS_ABSTRACT_ATT_TYPE =
                new TypeWrite(6, "The type '%s' is not abstract, and thus cannot own an abstract attribute type '%s'.");
        public static final TypeWrite OVERRIDDEN_NOT_SUPERTYPE =
                new TypeWrite(7, "The type '%s' cannot override '%s' as it is not a supertype.");
        public static final TypeWrite OVERRIDE_NOT_AVAILABLE = // TODO: this can be split between 'has', 'key' and 'plays' once pushed to commit
                new TypeWrite(8, "The type '%s' cannot override '%s' as it is either directly declared or not inherited.");
        public static final TypeWrite ATTRIBUTE_SUPERTYPE_VALUE_TYPE =
                new TypeWrite(9, "The attribute type '%s' has value type '%s', and cannot have supertype '%s' with value type '%s'.");
        public static final TypeWrite ATTRIBUTE_VALUE_TYPE_MISSING =
                new TypeWrite(10, "The attribute type '%s' is missing a value type.");
        public static final TypeWrite ATTRIBUTE_VALUE_TYPE_MODIFIED =
                new TypeWrite(11, "An attribute value type (in this case '%s') can only be set onto an attribute type (in this case '%s') when it was defined for the first time.");
        public static final TypeWrite ATTRIBUTE_VALUE_TYPE_UNDEFINED =
                new TypeWrite(12, "An attribute value type (in this case '%s') cannot be undefined. You can only undefine the attribute type (in this case '%s') itself.");
        public static final TypeWrite ATTRIBUTE_UNSET_ABSTRACT_HAS_SUBTYPES =
                new TypeWrite(13, "The attribute type '%s' cannot be set to non abstract as it has subtypes.");
        public static final TypeWrite ATTRIBUTE_NEW_SUPERTYPE_NOT_ABSTRACT =
                new TypeWrite(14, "The attribute type '%s' cannot be subtyped as it is not abstract.");
        public static final TypeWrite ATTRIBUTE_REGEX_UNSATISFIES_INSTANCES =
                new TypeWrite(15, "The attribute type '%s' cannot have regex '%s' as as it has an instance of value '%s'.");
        public static final TypeWrite ATTRIBUTE_VALUE_TYPE_DEFINED_NOT_ON_ATTRIBUTE_TYPE =
                new TypeWrite(16, "The type '%s' is not an attribute type, so it can not have a value type defined.");
        public static final TypeWrite OWNS_ATT_NOT_AVAILABLE =
                new TypeWrite(17, "The attribute type '%s' has been inherited or overridden, and cannot be redeclared as an attribute.");
        public static final TypeWrite OWNS_KEY_NOT_AVAILABLE =
                new TypeWrite(18, "The attribute type '%s' has been inherited or overridden, and cannot be redeclared as a key.");
        public static final TypeWrite OWNS_KEY_VALUE_TYPE =
                new TypeWrite(19, "The attribute type '%s' has value type '%s', and cannot and cannot be used as a type key.");
        public static final TypeWrite OWNS_KEY_PRECONDITION_OWNERSHIP =
                new TypeWrite(20, "The instances of type '%s' do not have exactly one attribute of type '%s' to convert to key.");
        public static final TypeWrite OWNS_KEY_PRECONDITION_UNIQUENESS =
                new TypeWrite(21, "The attributes of type '%s' are not uniquely owned by instances of type '%s' to convert to key.");
        public static final TypeWrite OWNS_KEY_PRECONDITION_NO_INSTANCES =
                new TypeWrite(22, "The instances of type '%s' do not have any attribute of type '%s' that could be converted to a key.");
        public static final TypeWrite PLAYS_ROLE_NOT_AVAILABLE =
                new TypeWrite(23, "The role type '%s' has been inherited or overridden, and cannot be redeclared.");
        public static final TypeWrite PLAYS_ABSTRACT_ROLE_TYPE =
                new TypeWrite(24, "The type '%s' is not abstract, and thus cannot play an abstract role type '%s'.");
        public static final TypeWrite RELATION_NO_ROLE =
                new TypeWrite(25, "The relation type '%s' does not relate any role type.");
        public static final TypeWrite RELATION_ABSTRACT_ROLE =
                new TypeWrite(26, "The relation type '%s' is not abstract, and thus cannot relate an abstract role type '%s'.");
        public static final TypeWrite RELATION_RELATES_ROLE_FROM_SUPERTYPE =
                new TypeWrite(27, "The role type '%s' is already declared by a supertype of '%s'.");
        public static final TypeWrite RELATION_RELATES_ROLE_NOT_AVAILABLE =
                new TypeWrite(28, "The role type '%s' cannot override '%s' as it is either directly related or not inherited.");
        public static final TypeWrite ROLE_DEFINED_OUTSIDE_OF_RELATION =
                new TypeWrite(29, "The role type '%s' cannot be defined/undefined outside the scope of its relation type.");
        public static final TypeWrite INVALID_DEFINE_SUB =
                new TypeWrite(30, "The type '%s' cannot be defined, as the provided supertype '%s' is not a valid thing type.");
        public static final TypeWrite INVALID_UNDEFINE_SUB =
                new TypeWrite(31, "The type '%s' cannot be undefined, as the provided supertype '%s' is not a valid supertype.");
        public static final TypeWrite INVALID_UNDEFINE_RELATES_OVERRIDE =
                new TypeWrite(32, "The overridden related role type '%s' cannot be undefined. You should re-define relating '%s' without overriding.");
        public static final TypeWrite INVALID_UNDEFINE_PLAYS_OVERRIDE =
                new TypeWrite(33, "The overridden played role type '%s' cannot be undefined. You should re-define playing '%s' without overriding.");
        public static final TypeWrite INVALID_UNDEFINE_OWNS_OVERRIDE =
                new TypeWrite(34, "The overridden owned attribute type '%s' cannot be undefined. You should re-define owning '%s' without overriding.");
        public static final TypeWrite INVALID_UNDEFINE_OWNS_KEY =
                new TypeWrite(35, "The annotation @key on attribute type '%s' cannot be undefined. You should re-define owning '%s' without @key annotation.");
        public static final TypeWrite INVALID_UNDEFINE_RELATES_HAS_INSTANCES =
                new TypeWrite(36, "The role type '%s' cannot be undefined because it is currently played by existing instances.");
        public static final TypeWrite INVALID_UNDEFINE_OWNS_HAS_INSTANCES =
                new TypeWrite(37, "The ability of type '%s' to own attribute type '%s' cannot be undefined because it is currently owned by existing instances.");
        public static final TypeWrite INVALID_UNDEFINE_PLAYS_HAS_INSTANCES =
                new TypeWrite(38, "The ability of type '%s' to play role type '%s' cannot be undefined because it is currently played by existing instances.");
        public static final TypeWrite INVALID_UNDEFINE_INHERITED_RELATES =
                new TypeWrite(39, "The ability of type '%s' to relate role type '%s' cannot be undefined because the ability is inherited from a supertype.");
        public static final TypeWrite INVALID_UNDEFINE_INHERITED_OWNS =
                new TypeWrite(40, "The ability of type '%s' to own attribute type '%s' cannot be undefined because the ability is inherited from a supertype.");
        public static final TypeWrite INVALID_UNDEFINE_INHERITED_PLAYS =
                new TypeWrite(41, "The ability of type '%s' to play role type '%s' cannot be undefined because the ability is inherited from a supertype.");
        public static final TypeWrite INVALID_UNDEFINE_NONEXISTENT_RELATES =
                new TypeWrite(42, "The ability of type '%s' to relate role type '%s' cannot be undefined because because it does not have that ability.");
        public static final TypeWrite INVALID_UNDEFINE_NONEXISTENT_OWNS =
                new TypeWrite(43, "The ability of type '%s' to own attribute type '%s' cannot be undefined because it does not have that ability.");
        public static final TypeWrite INVALID_UNDEFINE_NONEXISTENT_PLAYS =
                new TypeWrite(44, "The ability of type '%s' to play role type '%s' cannot be undefined because it does not have that ability.");
        public static final TypeWrite TYPE_CONSTRAINT_UNACCEPTED =
                new TypeWrite(45, "The type constraint '%s' is not accepted in a define/undefine query.");
        public static final TypeWrite ILLEGAL_SUPERTYPE_ENCODING =
                new TypeWrite(46, "Unable to set type with class '%s' as a supertype.");
        public static final TypeWrite MAX_SUBTYPE_REACHED =
                new TypeWrite(47, "The maximum number of '%s' types has been reached: '%s'");

        private static final String codePrefix = "TYW";
        private static final String messagePrefix = "Invalid Type Write";

        TypeWrite(int number, String message) {
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
        public static final RuleWrite TYPES_NOT_FOUND =
                new RuleWrite(2, "The rule '%s' uses type(s) %s which are not defined in the schema.");
        public static final RuleWrite RULES_IN_NEGATED_CYCLE_NOT_STRATIFIABLE =
                new RuleWrite(3, "The rules '%s' causes inference cycles with negations");
        public static final RuleWrite MAX_RULE_REACHED =
                new RuleWrite(4, "The maximum number of rules has been reached: '%s'");

        private static final String codePrefix = "RUW";
        private static final String messagePrefix = "Invalid Rule Write";

        RuleWrite(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }

    public static class Reasoner extends ErrorMessage {
        public static final Reasoner REVERSE_UNIFICATION_MISSING_CONCEPT =
                new Reasoner(1, "Reverse unification failed because a concept for identifier '%s' was not found in the provided map '%s'");

        private static final String codePrefix = "RSN";
        private static final String messagePrefix = "Reasoner Error";

        Reasoner(int number, String message) { super(codePrefix, number, messagePrefix, message); }
    }

    public static class Migrator extends ErrorMessage {
        public static final Migrator FILE_NOT_READABLE =
                new Migrator(1, "The specified file '%s' cannot be opened for read.");
        public static final Migrator FILE_NOT_WRITABLE =
                new Migrator(2, "The specified file '%s' cannot be opened for write.");
        public static final Migrator TYPE_NOT_FOUND =
                new Migrator(3, "The type '%s' (originally '%s') is not defined in the schema.");
        public static final Migrator INVALID_DATA =
                new Migrator(4, "The data being imported is invalid.");

        private static final String codePrefix = "MIG";
        private static final String messagePrefix = "Migrator failure";

        Migrator(int number, String message) {
            super(codePrefix, number, messagePrefix, message);
        }
    }
}
