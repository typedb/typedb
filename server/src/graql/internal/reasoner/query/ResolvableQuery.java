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

package grakn.core.graql.internal.reasoner.query;

import grakn.core.graql.internal.reasoner.unifier.Unifier;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.internal.reasoner.atom.Atom;
import grakn.core.graql.internal.reasoner.cache.MultilevelSemanticCache;
import grakn.core.graql.internal.reasoner.state.QueryStateBase;
import grakn.core.graql.internal.reasoner.state.ResolutionState;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.CheckReturnValue;

/**
 *
 * Interface for resolvable reasoner queries.
 *
 */
public interface ResolvableQuery extends ReasonerQuery {

    @CheckReturnValue
    ResolvableQuery copy();

    @CheckReturnValue
    Stream<Atom> selectAtoms();

    /**
     * @return this query in the composite form
     */
    @CheckReturnValue
    CompositeQuery asComposite();

    /**
     * @param sub substitution to be inserted into the query
     * @return corresponding query with additional substitution
     */
    @CheckReturnValue
    ResolvableQuery withSubstitution(ConceptMap sub);

    /**
     * @return corresponding positive query (with neq predicates removed)
     */
    @CheckReturnValue
    ResolvableQuery positive();

    /**
     * @return corresponding reasoner query with inferred types
     */
    @CheckReturnValue
    ResolvableQuery inferTypes();

    /**
     * @return true if this query requires atom decomposition
     */
    @CheckReturnValue
    boolean requiresDecomposition();

    /**
     * reiteration might be required if rule graph contains loops with negative flux
     * or there exists a rule which head satisfies body
     * @return true if because of the rule graph form, the resolution of this query may require reiteration
     */
    @CheckReturnValue
    boolean requiresReiteration();

    /**
     * @return rewritten (decomposed) version of the query
     */
    @CheckReturnValue
    ResolvableQuery rewrite();

    /**
     * resolves the query
     * @return stream of answers
     */
    @CheckReturnValue
    Stream<ConceptMap> resolve();

    /**
     *
     * @param subGoals already visited subgoals
     * @param cache query cache
     * @param reiterate true if reiteration should be performed
     * @return stream of resolved answers
     */
    @CheckReturnValue
    Stream<ConceptMap> resolve(Set<ReasonerAtomicQuery> subGoals, MultilevelSemanticCache cache, boolean reiterate);

    /**
     * @param sub partial substitution
     * @param u unifier with parent state
     * @param parent parent state
     * @param subGoals set of visited sub goals
     * @param cache query cache
     * @return resolution subGoal formed from this query
     */
    @CheckReturnValue
    ResolutionState subGoal(ConceptMap sub, Unifier u, QueryStateBase parent, Set<ReasonerAtomicQuery> subGoals, MultilevelSemanticCache cache);
}
