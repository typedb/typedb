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

package ai.grakn.graql.admin;

import ai.grakn.concept.Concept;
import ai.grakn.graql.VarName;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Interface for query result class.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class Answer{

    private Map<VarName, Concept> map = new HashMap<>();
    private Explanation explanation;

    public Answer(){}
    public Answer(Answer a){
        map.putAll(a.map);
        explanation = a.getExplanation();
    }
    public Answer(Map<VarName, Concept> m){
        map.putAll(m);
    }

    public Answer copy(){ return new Answer(this);}

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || !(obj instanceof Answer)) return false;
        Answer a2 = (Answer) obj;
        return map.equals(a2.map);
    }

    @Override
    public int hashCode(){ return map.hashCode();}

    
    public Set<VarName> keySet(){ return map.keySet();}
    
    public Collection<Concept> values(){ return map.values();}
    
    public Set<Concept> concepts(){ return map.values().stream().collect(Collectors.toSet());}
    
    public Set<Map.Entry<VarName, Concept>> entrySet(){ return map.entrySet();}

    
    public Concept get(VarName var){ return map.get(var);}
    
    public Concept put(VarName var, Concept con){ return map.put(var, con);}

    public Concept remove(VarName var){ return map.remove(var);}
    
    public Map<VarName, Concept> map(){ return map;}

    public void putAll(Answer a2){ map.putAll(a2.map);}
    public void putAll(Map<VarName, Concept> m2){ map.putAll(m2);}

    
    public boolean containsKey(VarName var){ return map.containsKey(var);}
    
    public boolean isEmpty(){ return map.isEmpty();}

    
    public int size(){ return map.size();}

    
    public Answer merge(Answer a2){
        Answer merged = new Answer(a2);
        merged.putAll(this);

        merged.setExplanation(new Explanation());
        Stream.of(this.copy(), a2.copy()).forEach(merged.getExplanation()::addAnswer);

        return merged;
    }

    public Answer explain(Explanation exp){
        Set<Answer> answers = explanation != null? explanation.getAnswers() : new HashSet<>();
        explanation = exp;
        answers.forEach(explanation::addAnswer);
        return this;
    }

    public Answer filterVars(Set<VarName> vars) {
        Answer filteredAnswer = new Answer(this);
        Set<VarName> varsToRemove = Sets.difference(this.keySet(), vars);
        varsToRemove.forEach(filteredAnswer::remove);

        filteredAnswer.setExplanation(this.getExplanation());
        return filteredAnswer;
    }

    
    public Answer unify(Map<VarName, VarName> unifiers){
        if (unifiers.isEmpty()) return this;
        Answer unified = new Answer(
                this.entrySet().stream()
                        .collect(Collectors.toMap(e -> unifiers.containsKey(e.getKey())?  unifiers.get(e.getKey()) : e.getKey(), Map.Entry::getValue))
        );

        Explanation exp = this.getExplanation().copy();
        unified.setExplanation(exp);
        //if (unified.getExplanation().isLookupExplanation()){
        //    unified.getExplanation().getQuery().unify(unifiers);
        //}
        return unified;
    }

    public Explanation getExplanation(){ return explanation;}
    public void setExplanation(Explanation e){ this.explanation = e;}
}

