/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.query.match;

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Match;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.rule.RuleUtils;
import ai.grakn.kb.internal.EmbeddedGraknTx;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Modifier that specifies the graph to execute the {@link Match} with.
 *
 * @author Grakn Warriors
 */
class MatchInfer extends MatchModifier {

    MatchInfer(AbstractMatch inner) {
        super(inner);
    }

    @Override
    public Stream<ConceptMap> stream(EmbeddedGraknTx<?> tx) {
        // If the tx is not embedded, treat it like there is no transaction
        // TODO: this is dodgy - when queries don't contain transactions this can be fixed

        EmbeddedGraknTx<?> embeddedTx;

        if (tx != null) {
            embeddedTx = tx;
        }
        else if (inner.tx() instanceof EmbeddedGraknTx) {
            embeddedTx = (EmbeddedGraknTx) inner.tx();
        }
        else {
            throw GraqlQueryException.noTx();
        }

        if (!RuleUtils.hasRules(embeddedTx)) return inner.stream(embeddedTx);

        validatePattern(embeddedTx);


        try {
            Iterator<Conjunction<VarPatternAdmin>> conjIt = getPattern().getDisjunctiveNormalForm().getPatterns().iterator();
            Conjunction<VarPatternAdmin> conj = conjIt.next();
            ReasonerQuery conjQuery = ReasonerQueries.create(conj, embeddedTx);
            conjQuery.checkValid();
            Stream<ConceptMap> answerStream = conjQuery.isRuleResolvable() ? conjQuery.resolve() : embeddedTx.graql().infer(false).match(conj).stream();
            while (conjIt.hasNext()) {
                conj = conjIt.next();
                conjQuery = ReasonerQueries.create(conj, embeddedTx);
                Stream<ConceptMap> localStream = conjQuery.isRuleResolvable() ? conjQuery.resolve() : embeddedTx.graql().infer(false).match(conj).stream();
                answerStream = Stream.concat(answerStream, localStream);
            }
            return answerStream.map(result -> result.project(getSelectedNames()));
        } catch (GraqlQueryException e) {
            System.err.println(e.getMessage());
            return Stream.empty();
        }
    }

    @Override
    public final Boolean inferring() {
        return true;
    }

    @Override
    protected String modifierString() {
        return "";
    }
}
