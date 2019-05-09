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


package grakn.core.graql.exception;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.type.Type;
import grakn.core.graql.reasoner.atom.Atomic;
import grakn.core.graql.reasoner.query.ReasonerQuery;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.Set;

/**
 * Runtime exception signalling illegal states of the system encountered during query processing.
 */
public class GraqlQueryException  extends GraknException {

    private final String NAME = "GraqlQueryException";

    private GraqlQueryException(String error) { super(error); }

    @Override
    public String getName() { return NAME; }

    public static GraqlQueryException maxIterationsReached(Class<?> clazz) {
        return new GraqlQueryException(ErrorMessage.MAX_ITERATION_REACHED.getMessage(clazz.toString()));
    }

    public static GraqlQueryException ambiguousType(Variable var, Set<Type> types) {
        return new GraqlQueryException(ErrorMessage.AMBIGUOUS_TYPE.getMessage(var, types));
    }

    public static GraqlQueryException incompleteResolutionPlan(ReasonerQuery reasonerQuery) {
        return new GraqlQueryException(ErrorMessage.INCOMPLETE_RESOLUTION_PLAN.getMessage(reasonerQuery));
    }

    public static GraqlQueryException rolePatternAbsent(Atomic relation) {
        return new GraqlQueryException(ErrorMessage.ROLE_PATTERN_ABSENT.getMessage(relation));
    }

    public static GraqlQueryException nonExistentUnifier() {
        return new GraqlQueryException(ErrorMessage.NON_EXISTENT_UNIFIER.getMessage());
    }

    public static GraqlQueryException illegalAtomConversion(Atomic atom, Class<?> targetType) {
        return new GraqlQueryException(ErrorMessage.ILLEGAL_ATOM_CONVERSION.getMessage(atom, targetType));
    }

    public static GraqlQueryException valuePredicateAtomWithMultiplePredicates() {
        return new GraqlQueryException("Attempting creation of ValuePredicate atom with more than single predicate");
    }

    public static GraqlQueryException getUnifierOfNonAtomicQuery() {
        return new GraqlQueryException("Attempted to obtain unifiers on non-atomic queries.");
    }

    public static GraqlQueryException invalidQueryCacheEntry(ReasonerQuery query, ConceptMap answer) {
        return new GraqlQueryException(ErrorMessage.INVALID_CACHE_ENTRY.getMessage(query.toString(), answer.toString()));
    }

    public static GraqlQueryException nonRoleIdAssignedToRoleVariable(Statement var) {
        return new GraqlQueryException(ErrorMessage.ROLE_ID_IS_NOT_ROLE.getMessage(var.toString()));
    }
}
