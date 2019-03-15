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


package grakn.core.graql.reasoner;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.query.ReasonerQueries;
import grakn.core.graql.reasoner.query.ResolvableQuery;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.query.MatchClause;
import java.util.HashSet;
import java.util.Iterator;

public class DisjunctionIterator extends ReasonerQueryIterator {

    final private Iterator<Conjunction<Pattern>> conjIterator;
    private Iterator<ConceptMap> answerIterator;
    private final TransactionOLTP tx;

    public DisjunctionIterator(MatchClause matchClause, TransactionOLTP tx){
        this.tx = tx;

        this.conjIterator = matchClause.getPatterns().getNegationDNF().getPatterns().stream().iterator();
        answerIterator = conjunctionIterator(conjIterator.next(), tx);
    }

    private Iterator<ConceptMap> conjunctionIterator(Conjunction<Pattern> conj, TransactionOLTP tx){
        ResolvableQuery query = ReasonerQueries.resolvable(conj, tx).rewrite();
        query.checkValid();

        boolean doNotResolve = query.getAtoms().isEmpty()
                || (query.isPositive() && !query.isRuleResolvable());
        return doNotResolve?
                tx.stream(Graql.match(conj), false).iterator() :
                new ResolutionIterator(query, new HashSet<>(), tx.queryCache(), query.requiresReiteration());
    }

    @Override
    public ConceptMap next(){
        return answerIterator.next();
    }

    /**
     * check whether answers available, if answers not fully computed compute more answers
     * @return true if answers available
     */
    @Override
    public boolean hasNext() {
        if (answerIterator.hasNext()) return true;
        if (conjIterator.hasNext()) answerIterator = conjunctionIterator(conjIterator.next(), tx);
        return answerIterator.hasNext();
    }
}
