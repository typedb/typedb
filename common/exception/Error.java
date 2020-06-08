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

package hypergraph.common.exception;

import java.util.HashMap;
import java.util.Map;

public abstract class Error<TYPE extends Error<TYPE>> {

    private static Map<String, Map<Integer, Error>> errors = new HashMap<>();
    private static int maxCodeNumber = 0;
    private static int maxCodeDigits = 0;

    private final String codePrefix;
    private final int codeNumber;
    private String code = null;
    private String description;

    Error(String codePrefix, int codeNumber, String descriptionPrefix, String descriptionBody) {
        this.codePrefix = codePrefix;
        this.codeNumber = codeNumber;
        this.description = descriptionPrefix + ": " + descriptionBody;

        assert errors.get(codePrefix) == null || errors.get(codePrefix).get(codeNumber) == null;
        errors.computeIfAbsent(codePrefix, s -> new HashMap<>()).put(codeNumber, this);
        maxCodeNumber = Math.max(codeNumber, maxCodeNumber);
        maxCodeDigits = (int) Math.ceil(Math.log10(maxCodeNumber));
    }

    protected abstract TYPE getThis();

    public String code() {
        if (code != null) return code;

        StringBuilder zeros = new StringBuilder();
        for (int digits = (int) Math.ceil(Math.log10(codeNumber)); digits < maxCodeDigits; digits++) {
            zeros.append("0");
        }

        code = codePrefix + zeros.toString() + codeNumber;
        return code;
    }

    public String description() {
        return description;
    }

    public TYPE format(Object... parameters) {
        description = String.format(description, parameters);
        return getThis();
    }

    @Override
    public String toString() {
        return String.format("[%s] %s", code(), description);
    }

    public static class Storage extends Error<Error.Storage> {

        public static final Storage INVALID_BYTE_INTERPRETATION =
                new Storage(1, "Invalid byte interpretation!");

        private static final String codePrefix = "STR";
        private static final String descriptionPrefix = "Invalid Storage State";

        Storage(int number, String description) {
            super(codePrefix, number, descriptionPrefix, description);
        }

        protected Error.Storage getThis() {
            return this;
        }

    }

    public static class Transaction extends Error<Error.Transaction> {

        public static final Transaction ILLEGAL_OPERATION =
                new Transaction(1, "Attempted an illegal operation!");
        public static final Transaction CLOSED_TRANSACTION =
                new Transaction(2, "The transaction has been closed and cannot perform any operation.");
        public static final Transaction ILLEGAL_COMMIT =
                new Transaction(3, "Only write transactions can be committed.");
        public static final Transaction DIRTY_SCHEMA_WRITES =
                new Transaction(4, "Contains schema writes when session type does not allow.");
        public static final Transaction DIRTY_DATA_WRITES =
                new Transaction(5, "Contains data writes when session type does not allow.");

        private static final String codePrefix = "TXN";
        private static final String descriptionPrefix = "Invalid Transaction Operation";

        Transaction(int number, String description) {
            super(codePrefix, number, descriptionPrefix, description);
        }

        protected Error.Transaction getThis() {
            return this;
        }
    }

    public static class ConceptRead extends Error {

        public static final ConceptRead INVALID_CONCEPT_CASTING =
                new ConceptRead(1, "Invalid concept conversion to '%s'.");
        private static final String codePrefix = "CON";
        private static final String descriptionPrefix = "Invalid Concept Retrieval";

        ConceptRead(int number, String description) {
            super(codePrefix, number, descriptionPrefix, description);
        }

        protected ConceptRead getThis() {
            return this;
        }
    }

    public static class ThingRead extends Error {

        public static final ThingRead INVALID_IID_CASTING =
                new ThingRead(1, "'Invalid IID casting to '%s'.");
        public static final ThingRead INVALID_VERTEX_CASTING =
                new ThingRead(2, "Invalid ThingVertex casting to '%s'.");

        private static final String codePrefix = "THR";
        private static final String descriptionPrefix = "Invalid Thing Read";

        ThingRead(int number, String description) {
            super(codePrefix, number, descriptionPrefix, description);
        }

        protected ThingRead getThis() {
            return this;
        }
    }

    public static class ThingWrite extends Error {

        public static final ThingWrite ILLEGAL_ABSTRACT_WRITE =
                new ThingWrite(1, "Attempted an illegal write of a new '%s' of abstract type '%s'.");
        public static final ThingWrite ILLEGAL_STRING_SIZE =
                new ThingWrite(2, "Attempted to insert a string larger than the maximum size.");
        public static final ThingWrite CONCURRENT_ATTRIBUTE_WRITE_DELETE =
                new ThingWrite(3, "Attempted concurrent modification of attributes (writes and deletes).");
        private static final String codePrefix = "THW";
        private static final String descriptionPrefix = "Invalid Thing Write";

        ThingWrite(int number, String description) {
            super(codePrefix, number, descriptionPrefix, description);
        }

        protected ThingWrite getThis() {
            return this;
        }
    }

    public static class TypeRead extends Error {

        public static final TypeRead TYPE_ROOT_MISMATCH =
                new TypeRead(1, "Attempted to retrieve '%s' as '%s', while it is actually a '%s'.");
        public static final TypeRead VALUE_TYPE_MISMATCH =
                new TypeRead(2, "Attempted to retrieve '%s' as AttributeType of ValueType '%s', while it actually has ValueType '%s'.");
        private static final String codePrefix = "TYR";
        private static final String descriptionPrefix = "Invalid Type Read";

        TypeRead(int number, String description) {
            super(codePrefix, number, descriptionPrefix, description);
        }

        protected TypeRead getThis() {
            return this;
        }
    }

    public static class TypeWrite extends Error {

        public static final TypeWrite INVALID_ROOT_TYPE_MUTATION =
                new TypeWrite(1, "Root types are immutable.");
        public static final TypeWrite UNSUPPORTED_VALUE_TYPE =
                new TypeWrite(2, "The Java class '%s' is not a supported value type.");
        public static final TypeWrite INVALID_ATTRIBUTE_SUPERTYPE_VALUE_TYPE =
                new TypeWrite(3, "AttributeType '%s' has value type '%s', and cannot have supertype '%s' with value type '%s'.");
        public static final TypeWrite INVALID_KEY_ATTRIBUTE =
                new TypeWrite(4, "AttributeType '%s' has value type '%s', and cannot and cannot be used as a type key.");

        private static final String codePrefix = "TYW";
        private static final String descriptionPrefix = "Invalid Type Definition";

        TypeWrite(int number, String description) {
            super(codePrefix, number, descriptionPrefix, description);
        }

        protected TypeWrite getThis() {
            return this;
        }
    }
}
