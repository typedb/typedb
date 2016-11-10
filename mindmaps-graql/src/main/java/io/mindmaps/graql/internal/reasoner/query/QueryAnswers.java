/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.reasoner.query;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.Type;
import io.mindmaps.graql.internal.reasoner.atom.Atom;
import io.mindmaps.graql.internal.reasoner.atom.Atomic;

import io.mindmaps.graql.internal.reasoner.atom.Binary;
import io.mindmaps.graql.internal.reasoner.atom.Predicate;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.mindmaps.graql.internal.reasoner.Utility.subtractSets;

public class QueryAnswers extends HashSet<Map<String, Concept>> {

    public QueryAnswers(){super();}
    public QueryAnswers(Collection<? extends Map<String, Concept>> ans){ super(ans);}

    public QueryAnswers filterVars(Set<String> vars) {
        QueryAnswers results = new QueryAnswers();
        if (this.isEmpty()) return results;
        
        this.forEach(answer -> {
            Map<String, Concept> map = new HashMap<>();
            answer.forEach((var, concept) -> {
                if (vars.contains(var))
                    map.put(var, concept);
            });
            if (!map.isEmpty()) results.add(map);
        });
        return new QueryAnswers(results.stream().distinct().collect(Collectors.toSet()));
    }

    public QueryAnswers filterInComplete(Set<String> vars) {
        return new QueryAnswers(this.stream()
                .filter(answer -> answer.size() == vars.size())
                .collect(Collectors.toSet()));
    }

    public QueryAnswers filterByTypes(Set<String> vars, Map<String, Type> varTypeMap){
        QueryAnswers results = new QueryAnswers();
        if(this.isEmpty()) return results;
        Map<String, Type> filteredMap = new HashMap<>();
        varTypeMap.forEach( (v, t) -> {
            if(vars.contains(v)) filteredMap.put(v, t);
        });
        if (filteredMap.isEmpty()) return this;


        this.forEach(answer -> {
            boolean isCompatible = true;
            //Iterator<Map.Entry<String, Concept>> it = answer.entrySet().iterator();
            Iterator<Map.Entry<String, Type>> it = filteredMap.entrySet().iterator();
            while( it.hasNext() && isCompatible){
                Map.Entry<String, Type> entry = it.next();
                isCompatible = answer.get(entry.getKey()).type().equals(entry.getValue());
                //Map.Entry<String, Concept> entry = it.next();
                //isCompatible = filteredMap.get(entry.getKey()).equals(entry.getValue().type());
            }
            if (isCompatible) results.add(answer);
        });
        return results;
    }

    public QueryAnswers join(QueryAnswers localTuples) {
        if (this.isEmpty() || localTuples.isEmpty())
            return new QueryAnswers();

        QueryAnswers join = new QueryAnswers();
        for( Map<String, Concept> lanswer : localTuples){
            for (Map<String, Concept> answer : this){
                boolean isCompatible = true;
                Iterator<Map.Entry<String, Concept>> it = lanswer.entrySet().iterator();
                while(it.hasNext() && isCompatible) {
                    Map.Entry<String, Concept> entry = it.next();
                    String var = entry.getKey();
                    Concept concept = entry.getValue();
                    if(answer.containsKey(var) && !concept.equals(answer.get(var)))
                        isCompatible = false;
                }

                if (isCompatible) {
                    Map<String, Concept> merged = new HashMap<>();
                    merged.putAll(lanswer);
                    merged.putAll(answer);
                    join.add(merged);
                }
            }
        }
        return join;
    }

    public QueryAnswers unify(Map<String, String> unifiers){
        return unify(unifiers, new HashMap<>(), new HashMap<>(), new HashMap<>());
    }

    private QueryAnswers unify(Map<String, String> unifiers, Map<String, Concept> subVars,
                               Map<String, Concept> valueConstraints, Map<String, String> typeConstraints){
        if (unifiers.isEmpty()) return new QueryAnswers(this);
        QueryAnswers unifiedAnswers = new QueryAnswers();
        this.forEach(entry -> {
            Map<String, Concept> answer = new HashMap<>(subVars);
            boolean isCompatible = true;
            Iterator<String> it = entry.keySet().iterator();
            while (it.hasNext() && isCompatible) {
                String var = it.next();
                Concept con = entry.get(var);
                if (unifiers.containsKey(var)) var = unifiers.get(var);
                if ( ( valueConstraints.containsKey(var) && !valueConstraints.get(var).equals(con) ) ||
                        ( typeConstraints.containsKey(var) && !typeConstraints.get(var).equals(con.getId()) ) )
                    isCompatible = false;
                else
                    answer.put(var, con);
            }
            if (isCompatible && !answer.isEmpty())
                unifiedAnswers.add(answer);
        });

        return unifiedAnswers;
    }

    /**
     * unify answers of childQuery with parentQuery
     * @param parentQuery parent atomic query containing target variables
     * @return unified answers
     */
    public static QueryAnswers getUnifiedAnswers(AtomicQuery parentQuery, AtomicQuery childQuery, QueryAnswers answers){
        if (parentQuery == childQuery) return new QueryAnswers(answers);
        MindmapsGraph graph = childQuery.getGraph().orElse(null);
        Atomic childAtom = childQuery.getAtom();
        Atomic parentAtom = parentQuery.getAtom();

        Map<String, String> unifiers = childAtom.getUnifiers(parentAtom);

        //identify extra subs contribute to/constraining answers
        Map<String, Concept> subVars = new HashMap<>();
        Map<String, Concept> valueConstraints = new HashMap<>();
        Map<String, String> typeConstraints = new HashMap<>();

        //find extra type constraints
        //TODO revisit bo bez sensu
        //TODO look only for instance types as we are looking at specific concepts

        Set<Atom> extraTypes =  parentQuery.getTypeConstraints();
        extraTypes.removeAll(childQuery.getTypeConstraints());
        extraTypes.stream().map(t -> (Binary) t).forEach(type -> {
            Predicate predicate = parentQuery.getIdPredicate(type.getValueVariable());
            if (predicate != null) typeConstraints.put(type.getVarName(), predicate.getPredicateValue());
        });


        //find extra subs
        if (parentQuery.getSelectedNames().size() != childQuery.getSelectedNames().size()){
            //get |child - parent| set difference
            Set<Predicate> extraSubs = subtractSets(parentQuery.getIdPredicates(), childQuery.getIdPredicates());
            extraSubs.forEach( sub -> {
                String var = sub.getVarName();
                Concept con = graph.getConcept(sub.getPredicateValue());
                if (unifiers.containsKey(var)) var = unifiers.get(var);
                if (childQuery.getSelectedNames().size() > parentQuery.getSelectedNames().size())
                    valueConstraints.put(var, con);
                else
                    subVars.put(var, con);
            });
        }

        QueryAnswers unifiedAnswers = answers.unify(unifiers, subVars, valueConstraints, typeConstraints);
        return unifiedAnswers.filterVars(parentQuery.getSelectedNames());
    }
}
