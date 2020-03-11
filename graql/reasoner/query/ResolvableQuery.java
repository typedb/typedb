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

package grakn.core.graql.reasoner.query;

import com.google.common.annotations.VisibleForTesting;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.graql.executor.TraversalExecutor;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.ResolutionIterator;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.state.AnswerPropagatorState;
import grakn.core.graql.reasoner.state.ResolutionState;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;

import javax.annotation.CheckReturnValue;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

/**
 *
 * Interface for resolvable reasoner queries.
 *
 */
public abstract class ResolvableQuery implements ReasonerQuery {

    private final ReasoningContext ctx;
    protected final TraversalExecutor traversalExecutor;

    ResolvableQuery(TraversalExecutor traversalExecutor, ReasoningContext ctx) {
        this.traversalExecutor = traversalExecutor;
        this.ctx = ctx;
    }

    @CheckReturnValue
    abstract ResolvableQuery copy();

    @CheckReturnValue
    public abstract Stream<Atom> selectAtoms();

    @CheckReturnValue
    public ReasoningContext context(){ return ctx;}

    /**
     * @return this query in the composite form
     */
    @CheckReturnValue
    public abstract CompositeQuery asComposite();

    /**
     * @param sub substitution to be inserted into the query
     * @return corresponding query with additional substitution
     */
    @CheckReturnValue
    public abstract ResolvableQuery withSubstitution(ConceptMap sub);

    /**
     * @return corresponding query with variable predicates removed
     */
    @CheckReturnValue
    abstract ResolvableQuery constantValuePredicateQuery();

    /**
     * @return corresponding reasoner query with inferred types
     */
    @CheckReturnValue
    abstract ResolvableQuery inferTypes();

    /**
     * @param q query to be compared with
     * @return true if two queries are alpha-equivalent
     */
    @CheckReturnValue
    abstract boolean isEquivalent(ResolvableQuery q);

    /**
     * @return true if this query requires atom decomposition
     */
    @CheckReturnValue
    public abstract boolean requiresDecomposition();

    /**
     * reiteration might be required if rule graph contains loops with negative flux
     * or there exists a rule which head satisfies body
     * @return true if because of the rule graph form, the resolution of this query may require reiteration
     */
    @CheckReturnValue
    public abstract boolean requiresReiteration();

    /**
     * @return corresponding Get query
     */
    @CheckReturnValue
    public GraqlGet getQuery() {
        return Graql.match(getPattern()).get();
    }

    /**
     * @return rewritten (decomposed) version of the query
     */
    @CheckReturnValue
    public abstract ResolvableQuery rewrite();

    /**
     * resolves the query
     * @return stream of answers
     */
    @CheckReturnValue
    @VisibleForTesting
    public Stream<ConceptMap> resolve(boolean infer){
        return resolve(new HashSet<>(), infer);
    }

    /**
     *
     * @param subGoals already visited subgoals
     * @return stream of resolved answers
     */
    @CheckReturnValue
    public Stream<ConceptMap> resolve(Set<ReasonerAtomicQuery> subGoals, boolean infer){
        boolean doNotResolve = !infer || getAtoms().isEmpty() || (isPositive() && !isRuleResolvable());
        if (doNotResolve) {
            return traversalExecutor.traverse(getPattern());
        } else {
            return new ResolutionIterator(this, subGoals, ctx.queryCache()).hasStream();
        }
    }

    /**
     * @param sub partial substitution
     * @param u unifier with parent state
     * @param parent parent state
     * @param subGoals set of visited sub goals
     * @return resolution state formed from this query
     */
    @CheckReturnValue
    public abstract ResolutionState resolutionState(ConceptMap sub, Unifier u, AnswerPropagatorState parent, Set<ReasonerAtomicQuery> subGoals);

    /**
     * @param parent parent state
     * @param subGoals set of visited sub goals
     * @return inner query state iterator (db iter + unifier + state iter) for this query
     */
    @CheckReturnValue
    abstract Iterator<ResolutionState> innerStateIterator(AnswerPropagatorState parent, Set<ReasonerAtomicQuery> subGoals);
}
