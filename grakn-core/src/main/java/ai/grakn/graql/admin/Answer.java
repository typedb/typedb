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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Wrapper class for query answer.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class Answer {

    private Map<VarName, Concept> map = new HashMap<>();
    private List<Concept> concepts = new ArrayList<>();

    public Answer(){}
    public Answer(Answer a){
        map.putAll(a.map);
        concepts.addAll(a.concepts);
    }
    public Answer(Map<VarName, Concept> m){ map.putAll(m);}

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

    public Set<Map.Entry<VarName, Concept>> entrySet(){ return map.entrySet();}

    public Concept get(VarName var){ return map.get(var);}
    public Concept put(VarName var, Concept con){ return map.put(var, con);}
    public Map<VarName, Concept> map(){ return map;}

    public void putAll(Answer a2){ map.putAll(a2.map);}
    public void putAll(Map<VarName, Concept> m2){ map.putAll(m2);}

    public boolean containsKey(VarName var){ return map.containsKey(var);}
    public boolean isEmpty(){ return map.isEmpty();}

    public int size(){ return map.size();}

    public Answer merge(Answer a2){
        Answer merged = new Answer(a2);
        merged.putAll(this);
        Stream.concat(this.values().stream(), a2.values().stream())
                .filter(c -> !c.isType())
                .forEach(merged.concepts::add);
        return merged;
    }

    public Answer filterVars(Set<VarName> vars) {
        Answer filteredAnswer = new Answer();
        vars.stream()
                .filter(this::containsKey)
                .forEach(var -> filteredAnswer.put(var, this.get(var)));
        filteredAnswer.concepts().addAll(this.concepts());
        return filteredAnswer;
    }

    public Answer unify(Map<VarName, VarName> unifiers){
        if (unifiers.isEmpty()) return this;
        Answer unified = new Answer(
                            this.entrySet().stream()
                            .collect(Collectors.toMap(e -> unifiers.containsKey(e.getKey())?  unifiers.get(e.getKey()) : e.getKey(), Map.Entry::getValue))
        );

        unified.concepts().addAll(this.concepts());
        return unified;
    }

    public List<Concept> concepts(){ return concepts;}
}
