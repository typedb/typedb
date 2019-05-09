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

    private GraqlQueryException(String error) { super(error, null, false, false); }

    @Override
    public String getName() { return NAME; }

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
