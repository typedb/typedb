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

import com.google.common.collect.Iterables;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.graql.executor.TraversalExecutor;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.PropertyAtomicFactory;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.executor.ExecutorFactory;
import grakn.core.kb.graql.planning.gremlin.TraversalPlanFactory;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.cache.QueryCache;
import grakn.core.kb.graql.reasoner.cache.RuleCache;
import grakn.core.kb.keyspace.KeyspaceStatistics;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;

import java.util.List;
import java.util.Set;

public class ReasonerQueryFactory {

    private ReasoningContext ctx;
    private ExecutorFactory executorFactory;
    private PropertyAtomicFactory propertyAtomicFactory;
    private TraversalPlanFactory traversalPlanFactory;
    private TraversalExecutor traversalExecutor;

    public ReasonerQueryFactory(ConceptManager conceptManager, QueryCache queryCache, RuleCache ruleCache, KeyspaceStatistics statistics,
                                ExecutorFactory executorFactory, PropertyAtomicFactory propertyAtomicFactory, TraversalPlanFactory traversalPlanFactory,
                                TraversalExecutor traversalExecutor) {
        this.executorFactory = executorFactory;
        this.propertyAtomicFactory = propertyAtomicFactory;
        this.traversalPlanFactory = traversalPlanFactory;
        this.traversalExecutor = traversalExecutor;
        this.ctx = new ReasoningContext(this, conceptManager, queryCache, ruleCache, statistics);
    }

    /**
     * @param pattern conjunctive pattern defining the query
     * @return a composite reasoner query constructed from provided conjunctive pattern
     */
    public CompositeQuery composite(Conjunction<Pattern> pattern) {
        return new CompositeQuery(pattern, traversalExecutor, ctx).inferTypes();
    }

    /**
     * @param conj conjunctive query corresponding to the +ve part of the composite query
     * @param comp set of queries corresponding to the -ve part of the composite query
     * @return corresponding composite query
     */
    public CompositeQuery composite(ReasonerQueryImpl conj, Set<ResolvableQuery> comp) {
        return new CompositeQuery(conj, comp, traversalExecutor, ctx).inferTypes();
    }

    /**
     * @param pattern conjunctive pattern defining the query
     * @return a resolvable reasoner query constructed from provided conjunctive pattern
     */
    public ResolvableQuery resolvable(Conjunction<Pattern> pattern) {
        CompositeQuery query = new CompositeQuery(pattern,  traversalExecutor, ctx).inferTypes();
        return query.isAtomic() ?
                new ReasonerAtomicQuery(query.getAtoms(), traversalPlanFactory, traversalExecutor, ctx) :
                query.isPositive() ?
                        query.getConjunctiveQuery() : query;
    }

    /**
     * @param q   base query for substitution to be attached
     * @param sub (partial) substitution
     * @return resolvable query with the substitution contained in the query
     */
    public ResolvableQuery resolvable(ResolvableQuery q, ConceptMap sub) {
        return q.withSubstitution(sub).inferTypes();
    }

    /**
     * Base factory method to instantiate ReasonerQuery
     * All ReasonerQueryImpl instances should be created from here to make sure the temporal
     * coupling that is necessary (for now) is respected and not broken by accident
     * TODO fix the temporal coupling between Atom and ReasonerQuery
     *
     * @param pattern
     * @return
     */
    public ReasonerQueryImpl withoutRoleInference(Conjunction<Statement> pattern) {
        return new ReasonerQueryImpl(pattern, propertyAtomicFactory, traversalPlanFactory, traversalExecutor, ctx);
    }

    /**
     * create a reasoner query from a conjunctive pattern with types inferred
     *
     * @param pattern conjunctive pattern defining the query
     * @return reasoner query constructed from provided conjunctive pattern
     */
    public ReasonerQueryImpl create(Conjunction<Statement> pattern) {
        ReasonerQueryImpl query = withoutRoleInference(pattern).inferTypes();
        return query.isAtomic() ?
                new ReasonerAtomicQuery(query.getAtoms(), traversalPlanFactory, traversalExecutor, ctx) :
                query;
    }

    /**
     * create a reasoner query from provided set of atomics
     *
     * @param as set of atomics that define the query
     * @return reasoner query defined by the provided set of atomics
     */
    public ReasonerQueryImpl create(Set<Atomic> as) {
        boolean isAtomic = as.stream().filter(Atomic::isSelectable).count() == 1;
        ReasonerQueryImpl reasonerQuery;
        if (isAtomic) {
            reasonerQuery = new ReasonerAtomicQuery(as, traversalPlanFactory, traversalExecutor, ctx).inferTypes();
        } else {
            reasonerQuery = new ReasonerQueryImpl(as, traversalPlanFactory, traversalExecutor, ctx).inferTypes();
        }
        return reasonerQuery;
    }

    /**
     * create a reasoner query from provided list of atoms
     * NB: atom constraints (types and predicates, if any) will be included in the query
     *
     * @param as list of atoms that define the query
     * @return reasoner query defined by the provided list of atoms together with their constraints (types and predicates, if any)
     */
    public ReasonerQueryImpl create(List<Atom> as) {
        boolean isAtomic = as.size() == 1;
        return isAtomic ?
                new ReasonerAtomicQuery(Iterables.getOnlyElement(as), traversalPlanFactory, traversalExecutor, ctx).inferTypes() :
                new ReasonerQueryImpl(as, traversalPlanFactory, traversalExecutor, ctx).inferTypes();
    }

    /**
     * create a reasoner query by combining an existing query and a substitution
     *
     * @param q   base query for substitution to be attached
     * @param sub (partial) substitution
     * @return reasoner query with the substitution contained in the query
     */
    public ReasonerQueryImpl create(ReasonerQueryImpl q, ConceptMap sub) {
        return q.withSubstitution(sub).inferTypes();
    }

    /**
     * @param pattern conjunctive pattern defining the query
     * @return atomic query defined by the provided pattern with inferred types
     */
    public ReasonerAtomicQuery atomic(Conjunction<Statement> pattern) {
        ReasonerAtomicQuery reasonerAtomicQuery = new ReasonerAtomicQuery(pattern, propertyAtomicFactory, traversalPlanFactory, traversalExecutor, ctx);
        return reasonerAtomicQuery.inferTypes();
    }

    /**
     * create an atomic query from the provided atom
     * NB: atom constraints (types and predicates, if any) will be included in the query
     *
     * @param atom defining the query
     * @return atomic query defined by the provided atom together with its constraints (types and predicates, if any)
     */
    public ReasonerAtomicQuery atomic(Atom atom) {
        return new ReasonerAtomicQuery(atom, traversalPlanFactory, traversalExecutor, ctx).inferTypes();
    }

    /**
     * create a reasoner atomic query from provided set of atomics
     *
     * @param as set of atomics that define the query
     * @return reasoner query defined by the provided set of atomics
     */
    public ReasonerAtomicQuery atomic(Set<Atomic> as) {
        return new ReasonerAtomicQuery(as, traversalPlanFactory, traversalExecutor, ctx).inferTypes();
    }

    /**
     * create an atomic query by combining an existing atomic query and a substitution
     *
     * @param q   base query for substitution to be attached
     * @param sub (partial) substitution
     * @return atomic query with the substitution contained in the query
     */
    public ReasonerAtomicQuery atomic(ReasonerAtomicQuery q, ConceptMap sub) {
        return q.withSubstitution(sub).inferTypes();
    }
}
