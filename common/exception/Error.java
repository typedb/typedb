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

        private static final String codePrefix = "TXN";
        private static final String descriptionPrefix = "Invalid Transaction Operation";

        Transaction(int number, String description) {
            super(codePrefix, number, descriptionPrefix, description);
        }

        protected Error.Transaction getThis() {
            return this;
        }
    }

    public static class TypeRetrieval extends Error {

        public static final Error.TypeDefinition INVALID_TYPE_CASTING =
                new Error.TypeDefinition(1, "The type '%s' cannot be converted into '%s'");

        private static final String codePrefix = "TYR";
        private static final String descriptionPrefix = "Invalid Type Retrieval";

        TypeRetrieval(int number, String description) {
            super(codePrefix, number, descriptionPrefix, description);
        }

        protected Error.TypeRetrieval getThis() {
            return this;
        }
    }

    public static class TypeDefinition extends Error {

        public static final Error.TypeDefinition INVALID_ROOT_TYPE_MUTATION =
                new Error.TypeDefinition(1, "Root types are immutable");
        public static final Error.TypeDefinition INVALID_VALUE_CLASS =
                new Error.TypeDefinition(2, "The Java class '%s' is not a supported value class");
        public static final Error.TypeDefinition INVALID_SUPERTYPE_VALUE_CLASS =
                new Error.TypeDefinition(3, "Type '%s' has value class '%s', and cannot have supertype '%s' with value class '%s' ");

        private static final String codePrefix = "TYD";
        private static final String descriptionPrefix = "Invalid Type Definition";

        TypeDefinition(int number, String description) {
            super(codePrefix, number, descriptionPrefix, description);
        }

        protected Error.TypeDefinition getThis() {
            return this;
        }
    }
}
