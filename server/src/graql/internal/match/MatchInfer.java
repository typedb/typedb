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
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.query.Match;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import grakn.core.graql.internal.reasoner.rule.RuleUtils;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.server.Transaction;
import grakn.core.server.session.TransactionImpl;

import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Modifier that specifies the graph to execute the {@link Match} with.
 */
class MatchInfer extends AbstractMatch {

    final AbstractMatch inner;

    MatchInfer(AbstractMatch inner) {
        this.inner = inner;
    }

    @Override
    public Stream<ConceptMap> stream() {
        if (inner.tx() == null || !(inner.tx() instanceof TransactionImpl)) {
            throw GraqlQueryException.noTx();
        }

        TransactionImpl<?> tx = (TransactionImpl) inner.tx();

        if (!RuleUtils.hasRules(tx)) return inner.stream();
        validateStatements(tx);

        try {
            Iterator<Conjunction<Statement>> conjIt = getPatterns().getDisjunctiveNormalForm().getPatterns().iterator();
            Conjunction<Statement> conj = conjIt.next();

            ReasonerQuery conjQuery = ReasonerQueries.create(conj, tx).rewrite();
            conjQuery.checkValid();
            Stream<ConceptMap> answerStream = conjQuery.isRuleResolvable() ? conjQuery.resolve() : tx.graql().infer(false).match(conj).stream();
            while (conjIt.hasNext()) {
                conj = conjIt.next();
                conjQuery = ReasonerQueries.create(conj, tx).rewrite();
                Stream<ConceptMap> localStream = conjQuery.isRuleResolvable() ? conjQuery.resolve() : tx.graql().infer(false).match(conj).stream();
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

    protected String modifierString() {
        return "";
    }

    @Override
    public final Set<SchemaConcept> getSchemaConcepts(Transaction tx) {
        return inner.getSchemaConcepts(tx);
    }

    @Override
    public final Conjunction<Pattern> getPatterns() {
        return inner.getPatterns();
    }

    @Override
    public Transaction tx() {
        return inner.tx();
    }

    @Override
    public Set<SchemaConcept> getSchemaConcepts() {
        return inner.getSchemaConcepts();
    }

    @Override
    public final Set<Variable> getSelectedNames() {
        return inner.getSelectedNames();
    }

    @Override
    public final String toString() {
        return inner.toString() + modifierString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MatchInfer maps = (MatchInfer) o;

        return inner.equals(maps.inner);
    }

    @Override
    public int hashCode() {
        return inner.hashCode();
    }
}
