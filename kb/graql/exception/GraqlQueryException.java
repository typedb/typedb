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


package grakn.core.kb.graql.exception;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Thing;
import graql.lang.query.GraqlQuery;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;

/**
 * Runtime exception signalling illegal states of the system encountered during query processing.
 */
public class GraqlQueryException extends GraknException {

    private final String NAME = "GraqlQueryException";

    private GraqlQueryException(String error) { super(error); }

    private GraqlQueryException(String error, Exception e) {
        super(error, e);
    }

    @Override
    public String getName() { return NAME; }

    public static GraqlQueryException maxIterationsReached(Class<?> clazz) {
        return new GraqlQueryException(ErrorMessage.MAX_ITERATION_REACHED.getMessage(clazz.toString()));
    }

    public static GraqlQueryException nonRoleIdAssignedToRoleVariable(Statement var) {
        return new GraqlQueryException(ErrorMessage.ROLE_ID_IS_NOT_ROLE.getMessage(var.toString()));
    }

    @CheckReturnValue
    public static GraqlQueryException unreachableStatement(Exception cause) {
        return unreachableStatement(null, cause);
    }

    @CheckReturnValue
    public static GraqlQueryException unreachableStatement(String message) {
        return unreachableStatement(message, null);
    }

    @CheckReturnValue
    private static GraqlQueryException unreachableStatement(@Nullable String message, Exception cause) {
        return new GraqlQueryException("Statement expected to be unreachable: " + message, cause);
    }

    public static GraqlQueryException cannotDeleteOwnershipOfNonAttributes(Variable var, Concept concept) {
        return new GraqlQueryException(ErrorMessage.DELETE_OWNERSHIP_NOT_AN_ATTRIBUTE.getMessage(var, concept));
    }

    public static GraqlQueryException cannotDeleteOwnershipTypeNotSatisfied(Variable var, Attribute attribute, Label requiredType) {
        return new GraqlQueryException(ErrorMessage.DELETE_OWNERSHIP_TYPE_NOT_SATISFIED.getMessage(var, attribute, requiredType));
    }

    public static GraqlQueryException cannotDeleteRPNoCompatiblePlayer(Variable rolePlayerVar, Thing rolePlayer, Variable relationVar,
                                                                       Relation relation, Label requiredRoleLabel) {
        return new GraqlQueryException(ErrorMessage.DELETE_ROLE_PLAYER_NO_COMPATIBLE_PLAYER.getMessage(rolePlayerVar, rolePlayer, relationVar, relation, requiredRoleLabel));
    }

    public static GraqlQueryException cannotDeleteInstanceIncorrectType(Variable var, Concept concept, Label expectedType) {
        return new GraqlQueryException((ErrorMessage.DELETE_INSTANCE_INCORRECT_TYPE.getMessage(var, concept, expectedType)));
    }

    public static GraqlQueryException notAThingInstance(Variable var, Concept concept) {
        return new GraqlQueryException(ErrorMessage.NOT_A_THING.getMessage(var, concept));
    }

    public static GraqlQueryException notARelationInstance(Variable var, Concept concept) {
        return new GraqlQueryException(ErrorMessage.NOT_A_RELATION_INSTANCE.getMessage(var, concept));
    }
}
