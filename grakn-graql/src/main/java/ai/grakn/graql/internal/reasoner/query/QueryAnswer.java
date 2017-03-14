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
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.AnswerExplanation;
import ai.grakn.graql.internal.reasoner.explanation.Explanation;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * Wrapper for query result class.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class QueryAnswer implements Answer {

    private Map<VarName, Concept> map = new HashMap<>();
    private AnswerExplanation explanation;

    public QueryAnswer(){}

    public QueryAnswer(Answer a){
        map.putAll(a.map());
        explanation = a.getExplanation();
    }

    public QueryAnswer(Map<VarName, Concept> m){
        map.putAll(m);
    }

    @Override
    public Answer copy(){ return new QueryAnswer(this);}

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || !(obj instanceof Answer)) return false;
        QueryAnswer a2 = (QueryAnswer) obj;
        return map.equals(a2.map);
    }

    @Override
    public int hashCode(){ return map.hashCode();}

    @Override
    public Set<VarName> keySet(){ return map.keySet();}

    @Override
    public Collection<Concept> values(){ return map.values();}

    @Override
    public Set<Concept> concepts(){ return map.values().stream().collect(Collectors.toSet());}

    @Override
    public Set<Map.Entry<VarName, Concept>> entrySet(){ return map.entrySet();}

    @Override
    public Concept get(VarName var){ return map.get(var);}

    @Override
    public Concept put(VarName var, Concept con){ return map.put(var, con);}

    @Override
    public Concept remove(VarName var){ return map.remove(var);}

    @Override
    public Map<VarName, Concept> map(){ return map;}

    @Override
    public void putAll(Answer a){ map.putAll(a.map());}

    @Override
    public void putAll(Map<VarName, Concept> m2){ map.putAll(m2);}

    @Override
    public boolean containsKey(VarName var){ return map.containsKey(var);}

    @Override
    public boolean isEmpty(){ return map.isEmpty();}

    @Override
    public int size(){ return map.size();}

    @Override
    public Answer merge(Answer a2){
        QueryAnswer merged = new QueryAnswer(a2);
        merged.putAll(this);

        merged.setExplanation(new Explanation());
        if (this.getExplanation() != null
                && (!this.getExplanation().isLookupExplanation() && !this.getExplanation().isRuleExplanation())){
            this.getExplanation().getAnswers().forEach(merged.getExplanation()::addAnswer);
        } else {
            merged.getExplanation().addAnswer(this);
        }
        if (a2.getExplanation() != null
                && (!a2.getExplanation().isLookupExplanation() && !a2.getExplanation().isRuleExplanation())){
            a2.getExplanation().getAnswers().forEach(merged.getExplanation()::addAnswer);
        } else {
            merged.getExplanation().addAnswer(a2);
        }

        return merged;
    }

    @Override
    public QueryAnswer explain(AnswerExplanation exp){
        Set<Answer> answers = explanation != null? explanation.getAnswers() : new HashSet<>();
        explanation = exp;
        answers.forEach(explanation::addAnswer);
        return this;
    }

    @Override
    public QueryAnswer filterVars(Set<VarName> vars) {
        QueryAnswer filteredAnswer = new QueryAnswer(this);
        Set<VarName> varsToRemove = Sets.difference(this.keySet(), vars);
        varsToRemove.forEach(filteredAnswer::remove);

        filteredAnswer.setExplanation(this.getExplanation());
        return filteredAnswer;
    }

    @Override
    public QueryAnswer unify(Map<VarName, VarName> unifiers){
        if (unifiers.isEmpty()) return this;
        QueryAnswer unified = new QueryAnswer(
                this.entrySet().stream()
                        .collect(Collectors.toMap(e -> unifiers.containsKey(e.getKey())?  unifiers.get(e.getKey()) : e.getKey(), Map.Entry::getValue))
        );
        return unified.setExplanation(this.getExplanation());
    }

    @Override
    public AnswerExplanation getExplanation(){ return explanation;}

    @Override
    public QueryAnswer setExplanation(AnswerExplanation e){
        this.explanation = e;
        return this;
    }
}
