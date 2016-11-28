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

import ai.grakn.concept.Concept;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class AtomicMatchQuery extends AtomicQuery{

    final private QueryAnswers answers;

    public AtomicMatchQuery(Atom atom, Set<String> vars){
        super(atom, vars);
        answers = new QueryAnswers();
    }

    public AtomicMatchQuery(AtomicQuery query, QueryAnswers ans){
        super(query);
        answers = new QueryAnswers(ans);
    }

    @Override
    public Stream<Map<String, Concept>> stream() {return answers.stream();}

    @Override
    public QueryAnswers getAnswers(){ return answers;}

    @Override
    public void DBlookup() {
        answers.addAll(execute());
    }

    @Override
    public void memoryLookup(Map<AtomicQuery, AtomicQuery> matAnswers) {
        AtomicQuery equivalentQuery = matAnswers.get(this);
        if(equivalentQuery != null)
            answers.addAll(QueryAnswers.getUnifiedAnswers(this, equivalentQuery, equivalentQuery.getAnswers()));
    }

    @Override
    public void propagateAnswers(Map<AtomicQuery, AtomicQuery> matAnswers) {
        getChildren().forEach(childQuery -> {
            QueryAnswers ans = QueryAnswers.getUnifiedAnswers(childQuery, this, matAnswers.get(this).getAnswers());
            childQuery.getAnswers().addAll(ans);
            childQuery.propagateAnswers(matAnswers);
        });
    }

    @Override
    public QueryAnswers materialise(){
        QueryAnswers fullAnswers = new QueryAnswers();
        answers.forEach(answer -> {
            Set<IdPredicate> subs = new HashSet<>();
            answer.forEach((var, con) -> {
                IdPredicate sub = new IdPredicate(var, con);
                if (!containsAtom(sub))
                    subs.add(sub);
            });
            fullAnswers.addAll(materialise(subs));
        });
        return fullAnswers;
    }
}
