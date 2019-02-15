/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package graql.lang.exception;


import graql.lang.util.Token;

import java.util.List;
import java.util.Set;

import static graql.lang.exception.ErrorMessage.INVALID_COMPUTE_ARGUMENT;
import static graql.lang.exception.ErrorMessage.INVALID_COMPUTE_CONDITION;
import static graql.lang.exception.ErrorMessage.INVALID_COMPUTE_METHOD;
import static graql.lang.exception.ErrorMessage.INVALID_COMPUTE_METHOD_ALGORITHM;
import static graql.lang.exception.ErrorMessage.MISSING_COMPUTE_CONDITION;

public class GraqlException extends RuntimeException {

    protected GraqlException(String error) {
        super(error);
    }

    protected GraqlException(String error, Exception e) {
        super(error, e);
    }

    public String getName() {
        return this.getClass().getName();
    }

    public static GraqlException create(String error){
        return new GraqlException(error);
    }

    public static GraqlException conflictingProperties(String statement, String property, String other) {
        return new GraqlException(graql.lang.exception.ErrorMessage.CONFLICTING_PROPERTIES.getMessage(statement, property, other));
    }

    public static GraqlException variableOutOfScope(String var) {
        return new GraqlException(graql.lang.exception.ErrorMessage.VARIABLE_OUT_OF_SCOPE.getMessage(var));
    }

    public static GraqlException noPatterns() {
        return new GraqlException(ErrorMessage.NO_PATTERNS.getMessage());
    }

    public static GraqlException invalidComputeQuery_invalidMethod(List<Token.Compute.Method> methods) {
        return new GraqlException(INVALID_COMPUTE_METHOD.getMessage(methods));
    }

    public static GraqlException invalidComputeQuery_invalidCondition(Token.Compute.Method method, Set<Token.Compute.Condition> accepted) {
        return new GraqlException(INVALID_COMPUTE_CONDITION.getMessage(method, accepted));
    }

    public static GraqlException invalidComputeQuery_missingCondition(Token.Compute.Method method, Set<Token.Compute.Condition> required) {
        return new GraqlException(MISSING_COMPUTE_CONDITION.getMessage(method, required));
    }

    public static GraqlException invalidComputeQuery_invalidMethodAlgorithm(Token.Compute.Method method, Set<Token.Compute.Algorithm> accepted) {
        return new GraqlException(INVALID_COMPUTE_METHOD_ALGORITHM.getMessage(method, accepted));
    }

    public static GraqlException invalidComputeQuery_invalidArgument(Token.Compute.Method method, Token.Compute.Algorithm algorithm, Set<Token.Compute.Param> accepted) {
        return new GraqlException(INVALID_COMPUTE_ARGUMENT.getMessage(method, algorithm, accepted));
    }
}