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

import grakn.core.graql.admin.Unifier;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.internal.reasoner.ResolutionIterator;
import grakn.core.graql.internal.reasoner.cache.MultilevelSemanticCache;
import grakn.core.graql.internal.reasoner.state.ConjunctiveState;
import grakn.core.graql.internal.reasoner.state.CompositeState;
import grakn.core.graql.internal.reasoner.state.QueryStateBase;
import grakn.core.graql.internal.reasoner.state.ResolutionState;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.Negation;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.server.session.TransactionOLTP;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 */
public class CompositeQuery{

    final private ReasonerQueryImpl conjunctiveQuery;
    final private CompositeQuery complementQuery;

    CompositeQuery(Conjunction<Pattern> pattern, TransactionOLTP tx) {
        Conjunction<Statement> positiveConj = Graql.and(
                pattern.getPatterns().stream()
                        .filter(Pattern::isPositive)
                        .flatMap(p -> p.statements().stream())
                        .collect(Collectors.toSet())
        );
        //conjunction of negation patterns
        Conjunction<Pattern> complementPattern = complementPattern(pattern);
        this.conjunctiveQuery = new ReasonerQueryImpl(positiveConj, tx);
        this.complementQuery = !complementPattern.getPatterns().isEmpty()?
                new CompositeQuery(complementPattern, tx) : null;
    }

    private Conjunction<Pattern> complementPattern(Conjunction<Pattern> pattern){
        Set<Pattern> complements = pattern.getPatterns().stream()
                .filter(p -> !p.isPositive())
                .map(p -> (Negation) p)
                .map((Function<Negation, Pattern>) Negation::getPattern)
                .collect(Collectors.toSet());
        return Graql.and(complements).getNegationDNF().getPatterns().iterator().next();
    }

    CompositeQuery(ReasonerQueryImpl conj, CompositeQuery neg) {
        this.conjunctiveQuery = conj;
        this.complementQuery = neg;
    }

    /**
     * @param sub substitution to be inserted into the query
     * @return corresponding query with additional substitution
     */
    CompositeQuery withSubstitution(ConceptMap sub){
        return new CompositeQuery(
                getConjunctiveQuery().withSubstitution(sub),
                getComplementQuery() != null? getComplementQuery().withSubstitution(sub) : null
        );
    }

    public ReasonerQueryImpl getConjunctiveQuery() {
        return conjunctiveQuery;
    }

    public CompositeQuery getComplementQuery() {
        return complementQuery;
    }

    public boolean isPositive(){
        return complementQuery == null;
    }

    public Stream<ConceptMap> resolve() {
        return resolve(new MultilevelSemanticCache());
    }

    public Stream<ConceptMap> resolve(MultilevelSemanticCache cache){
        return new ResolutionIterator(this, cache).hasStream();
    }

    /**
     * @param sub partial substitution
     * @param u unifier with parent state
     * @param parent parent state
     * @param subGoals set of visited sub goals
     * @param cache query cache
     * @return resolution subGoal formed from this query
     */
    public ResolutionState subGoal(ConceptMap sub, Unifier u, QueryStateBase parent, Set<ReasonerAtomicQuery> subGoals, MultilevelSemanticCache cache){
        return isPositive()?
                new ConjunctiveState(conjunctiveQuery, sub, u, parent, subGoals, cache) :
                new CompositeState(this, sub, u, parent, subGoals, cache);
    }
}
