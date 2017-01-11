/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.query;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.reasoner.atom.Atom;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.join;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.nonEqualsFilterFunction;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.varFilterFunction;

/**
 *
 * <p>
 * Reasoner query providing resolution streaming facilities for conjunctive queries.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class ReasonerMatchQuery extends Query{

    final private QueryAnswers answers;

    public ReasonerMatchQuery(MatchQuery query, GraknGraph graph){
        super(query, graph);
        answers = new QueryAnswers();
    }

    public ReasonerMatchQuery(MatchQuery query, GraknGraph graph, QueryAnswers ans){
        super(query, graph);
        answers = new QueryAnswers(ans);
    }

    @Override
    public Stream<Map<VarName, Concept>> resolve(boolean materialise) {
        if (!this.isRuleResolvable())
            return this.getMatchQuery().admin().streamWithVarNames();
        Iterator<Atom> atIt = this.selectAtoms().iterator();
        AtomicQuery atomicQuery = new AtomicMatchQuery(atIt.next(), this.getSelectedNames());
        Stream<Map<VarName, Concept>> answerStream = atomicQuery.resolve(materialise);
        while (atIt.hasNext()) {
            atomicQuery = new AtomicMatchQuery(atIt.next(), this.getSelectedNames());
            Stream<Map<VarName, Concept>> subAnswerStream = atomicQuery.resolve(materialise);
            answerStream = join(answerStream, subAnswerStream);
        }
        return answerStream
                .flatMap(a -> nonEqualsFilterFunction.apply(a, this.getFilters()))
                .flatMap(a -> varFilterFunction.apply(a, this.getSelectedNames()));
    }
}
