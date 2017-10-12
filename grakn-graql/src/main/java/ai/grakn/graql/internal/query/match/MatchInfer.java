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

package ai.grakn.graql.internal.query.match;

import ai.grakn.GraknTx;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Match;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.rule.RuleUtils;

import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

import static ai.grakn.util.CommonUtil.optionalOr;

/**
 * Modifier that specifies the graph to execute the {@link Match} with.
 */
class MatchInfer extends MatchModifier {

    private final boolean materialise;

    MatchInfer(AbstractMatch inner, boolean materialise) {
        super(inner);
        this.materialise = materialise;
    }

    @Override
    public Stream<Answer> stream(Optional<GraknTx> optionalGraph) {
        GraknTx graph = optionalOr(optionalGraph, inner.tx()).orElseThrow(GraqlQueryException::noTx);

        if (!RuleUtils.hasRules(graph)) return inner.stream(optionalGraph);

        Iterator<Conjunction<VarPatternAdmin>> conjIt = getPattern().getDisjunctiveNormalForm().getPatterns().iterator();
        Conjunction<VarPatternAdmin> conj = conjIt.next();
        ReasonerQuery conjQuery = ReasonerQueries.create(conj, graph);
        Stream<Answer> answerStream = conjQuery.isRuleResolvable()? conjQuery.resolve(materialise) : graph.graql().match(conj).stream();
        while(conjIt.hasNext()) {
            conj = conjIt.next();
            conjQuery = ReasonerQueries.create(conj, graph);
            Stream<Answer> localStream = conjQuery.isRuleResolvable()? conjQuery.resolve(materialise) : graph.graql().match(conj).stream();
            answerStream = Stream.concat(answerStream, localStream);
        }
        return answerStream.map(result -> result.project(getSelectedNames()));
    }

    @Override
    protected String modifierString() {
        return "";
    }
}
