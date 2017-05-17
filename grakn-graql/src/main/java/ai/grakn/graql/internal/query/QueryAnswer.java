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
 *
 */

package ai.grakn.graql.internal.query;

import ai.grakn.concept.Concept;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.AnswerExplanation;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.explanation.Explanation;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Wrapper for a query result class {@link Answer}.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class QueryAnswer implements Answer {

    private final Map<VarName, Concept> map = new HashMap<>();
    private AnswerExplanation explanation = new Explanation();

    public QueryAnswer(){}

    public QueryAnswer(Answer a){
        map.putAll(a.map());
        explanation = a.getExplanation();
    }

    public QueryAnswer(Map<VarName, Concept> m){
        map.putAll(m);
    }

    @Override
    public String toString(){
        return map.entrySet().stream().map(e -> "[" + e.getKey() + "/" + e.getValue().getId() + "]").collect(Collectors.joining());
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
    public Concept get(String var) {
        return map.get(VarName.of(var));
    }

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
    public void forEach(BiConsumer<? super VarName, ? super Concept> consumer) {
        map.forEach(consumer);
    }

    @Override
    public Answer merge(Answer a2, boolean mergeExplanation){
        if(a2.isEmpty()) return this;
        AnswerExplanation exp = this.getExplanation();
        QueryAnswer merged = new QueryAnswer(a2);
        merged.putAll(this);

        if(mergeExplanation) {
            exp = exp.merge(a2.getExplanation());
            if(!this.getExplanation().isJoinExplanation()) exp.addAnswer(this);
            if(!a2.getExplanation().isJoinExplanation()) exp.addAnswer(a2);
        }
        return merged.setExplanation(exp);
    }

    @Override
    public Answer merge(Answer a2){ return this.merge(a2, false);}

    @Override
    public Answer explain(AnswerExplanation exp){
        Set<Answer> answers = explanation.getAnswers();
        explanation = exp;
        answers.forEach(explanation::addAnswer);
        return this;
    }

    @Override
    public Answer filterVars(Set<VarName> vars) {
        QueryAnswer filteredAnswer = new QueryAnswer(this);
        Set<VarName> varsToRemove = Sets.difference(this.keySet(), vars);
        varsToRemove.forEach(filteredAnswer::remove);

        return filteredAnswer.setExplanation(this.getExplanation());
    }

    @Override
    public Answer unify(Unifier unifier){
        if (unifier.isEmpty()) return this;
        Answer unified = new QueryAnswer();
        Multimap<VarName, Concept> answerMultimap = HashMultimap.create();

        this.entrySet()
                .forEach(e -> {
                    VarName var = e.getKey();
                    Collection<VarName> uvars = unifier.get(var);
                    if (uvars.isEmpty()) {
                        answerMultimap.put(var, e.getValue());
                    } else {
                        uvars.forEach(uv -> answerMultimap.put(uv, e.getValue()));
                    }
                });
        //non-ambiguous mapping
        if ( answerMultimap.keySet().size() == answerMultimap.values().size()) {
            answerMultimap.entries().forEach(e -> unified.put(e.getKey(), e.getValue()));
        }

        return unified.setExplanation(this.getExplanation());
    }

    @Override
    public Stream<Answer> permute(Set<Unifier> unifierSet){
        if (unifierSet.isEmpty()) return Stream.of(this);
        return unifierSet.stream().map(this::unify);
    }

    @Override
    public AnswerExplanation getExplanation(){ return explanation;}

    @Override
    public QueryAnswer setExplanation(AnswerExplanation e){
        this.explanation = e;
        return this;
    }

    @Override
    public Set<Answer> getExplicitPath(){
        return getAnswers().stream().filter(ans -> ans.getExplanation().isLookupExplanation()).collect(Collectors.toSet());
    }

    @Override
    public Set<Answer> getAnswers(){
        Set<Answer> answers = Sets.newHashSet(this);
        this.getExplanation().getAnswers().forEach(ans -> ans.getAnswers().forEach(answers::add));
        return answers;
    }

    @Override
    public Set<AnswerExplanation> getExplanations(){
        Set<AnswerExplanation> explanations = Sets.newHashSet(this.getExplanation());
        this.getExplanation().getAnswers().forEach(ans -> ans.getExplanations().forEach(explanations::add));
        return explanations;
    }
}
