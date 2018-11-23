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

package grakn.core.graql.internal.match;

import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.query.Match;
import grakn.core.graql.admin.Conjunction;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.admin.VarPatternAdmin;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import grakn.core.graql.internal.reasoner.rule.RuleUtils;
import grakn.core.server.session.TransactionImpl;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Modifier that specifies the graph to execute the {@link Match} with.
 *
 */
class MatchInfer extends MatchModifier {

    MatchInfer(AbstractMatch inner) {
        super(inner);
    }

    private Stream<ConceptMap> resolveConjunction(Conjunction<VarPatternAdmin> conj, TransactionImpl<?> tx){
        ReasonerQuery conjQuery = ReasonerQueries.create(conj, tx).rewrite();
        conjQuery.checkValid();
        return conjQuery.resolve();
    }

    @Override
    public Stream<ConceptMap> stream(TransactionImpl<?> tx) {
        // If the tx is not embedded, treat it like there is no transaction
        // TODO: this is dodgy - when queries don't contain transactions this can be fixed

        TransactionImpl<?> embeddedTx;

        if (tx != null) {
            embeddedTx = tx;
        } else if (inner.tx() instanceof TransactionImpl) {
            embeddedTx = (TransactionImpl) inner.tx();
        } else {
            throw GraqlQueryException.noTx();
        }

        validatePattern(embeddedTx);

        try {
            Iterator<Conjunction<VarPatternAdmin>> conjIt = getPattern().getDisjunctiveNormalForm().getPatterns().iterator();
            Stream<ConceptMap> answerStream = Stream.empty();
            while (conjIt.hasNext()) {
                answerStream = Stream.concat(answerStream, resolveConjunction(conjIt.next(), embeddedTx));
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
