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
import ai.grakn.concept.Type;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.internal.reasoner.atom.NotEquals;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * Wrapper class for a set of answers providing higher level filtering facilities
 * as well as unification and join operations.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class QueryAnswers extends HashSet<Map<VarName, Concept>> {

    public QueryAnswers(){super();}
    public QueryAnswers(Collection<? extends Map<VarName, Concept>> ans){ super(ans);}

    private Set<VarName> getVars(){
        Optional<Map<VarName, Concept>> map = this.stream().findFirst();
        return map.isPresent()? map.get().keySet() : new HashSet<>();
    }


    /**
     * permute answer based on specified sets of permutations defined by unifiers
     * @param unifierSet set of unifier mappings to perform the permutation on
     * @param subs substitutions that need to met
     * @param types type constraints that need to be met
     * @return permuted answers
     */
    public QueryAnswers permute(Set<Map<VarName, VarName>> unifierSet, Set<IdPredicate> subs, Set<TypeAtom> types){
        if (unifierSet.isEmpty()) return this;
        QueryAnswers permutedAnswers = new QueryAnswers();
        unifierSet.forEach(unifiers -> permutedAnswers.addAll(this.unify(unifiers)));
        return permutedAnswers
                .filterBySubstitutions(subs)
                .filterByEntityTypes(types);
    }

    /**
     * filter answers by constraining the variable set to the provided one
     * @param vars set of variable names
     * @return filtered answers
     */
    public QueryAnswers filterVars(Set<VarName> vars) {
        return new QueryAnswers(this.stream().map(result -> Maps.filterKeys(result, vars::contains)).collect(Collectors.toSet()));
    }

    /**
     * filter answers by discarding the already known ones
     * @param known set of known answers
     * @return filtered answers
     */
    public QueryAnswers filterKnown(QueryAnswers known){
        if (this.getVars().equals(known.getVars()) || known.isEmpty() ){
            QueryAnswers results = new QueryAnswers(this);
            results.removeAll(known);
            return results;
        }
        QueryAnswers results = new QueryAnswers();
        this.forEach(answer ->{
            boolean isKnown = false;
            Iterator<Map<VarName, Concept>> it = known.iterator();
            while(it.hasNext() && !isKnown) {
                Map<VarName, Concept> knownAnswer = it.next();
                isKnown = knownAnswer.entrySet().containsAll(answer.entrySet());
            }
            if (!isKnown) results.add(answer);
        });
        return results;
    }

    /**
     * filter answers by applying NonEquals filters
     * @param filters non equal atoms to
     * @return filtered answers
     */
    public QueryAnswers filterNonEquals(Set<NotEquals> filters){
        if(filters.isEmpty()) return this;
        QueryAnswers results = new QueryAnswers(this);
        for (NotEquals filter : filters) results = filter.filter(results);
        return results;
    }

    private QueryAnswers filterBySubstitutions(Set<IdPredicate> subs){
        if (subs.isEmpty()) return this;
        QueryAnswers results = new QueryAnswers(this);
        subs.forEach( sub -> this.stream()
                .filter(answer -> !answer.get(sub.getVarName()).getId().equals(sub.getPredicate()))
                .forEach(results::remove));
        return results;
    }

    private QueryAnswers filterByEntityTypes(Set<TypeAtom> types){
        if (types.isEmpty()) return this;
        QueryAnswers results = new QueryAnswers();
        this.forEach(answer -> {
            boolean isCompatible = true;
            Iterator<TypeAtom> it = types.iterator();
            while( it.hasNext() && isCompatible){
                TypeAtom type = it.next();
                VarName var = type.getVarName();
                Type t = type.getType();
                isCompatible = answer.get(var).asInstance().type().equals(t);
            }
            if (isCompatible) results.add(answer);
        });
        return results;
    }

    /**
     * perform a join operation between this and provided answers
     * @param localTuples right operand of join operation
     * @return joined answers
     */
    public QueryAnswers join(QueryAnswers localTuples) {
        if (this.isEmpty() || localTuples.isEmpty()) {
            return new QueryAnswers();
        }
        QueryAnswers join = new QueryAnswers();
        Set<VarName> joinVars = Sets.intersection(this.getVars(), localTuples.getVars());
        for( Map<VarName, Concept> lanswer : localTuples){
            for (Map<VarName, Concept> answer : this){
                boolean isCompatible = true;
                Iterator<VarName> vit = joinVars.iterator();
                while(vit.hasNext() && isCompatible) {
                    VarName var = vit.next();
                    isCompatible = answer.get(var).equals(lanswer.get(var));
                }

                if (isCompatible) {
                    Map<VarName, Concept> merged = new HashMap<>(lanswer);
                    merged.putAll(answer);
                    join.add(merged);
                }
            }
        }
        return join;
    }

    /**
     * unify the answers by applying unifiers to variable set
     * @param unifiers map of [key: from/value: to] unifiers
     * @return unified query answers
     */
    public QueryAnswers unify(Map<VarName, VarName> unifiers){
        if (unifiers.isEmpty()) return new QueryAnswers(this);
        QueryAnswers unifiedAnswers = new QueryAnswers();
        this.forEach(answer -> {
            Map<VarName, Concept> unifiedAnswer = answer.entrySet().stream()
                    .collect(Collectors.toMap(e -> unifiers.containsKey(e.getKey())? unifiers.get(e.getKey()) : e.getKey(), Map.Entry::getValue));
            unifiedAnswers.add(unifiedAnswer);
        });

        return unifiedAnswers;
    }

    /**
     * unify answers of childQuery with parentQuery
     * @param parentQuery parent atomic query containing target variables
     * @return unified answers
     */
    public static QueryAnswers getUnifiedAnswers(ReasonerAtomicQuery parentQuery, ReasonerAtomicQuery childQuery){
        QueryAnswers answers = childQuery.getAnswers();
        if (parentQuery == childQuery) return new QueryAnswers(answers);
        Atomic childAtom = childQuery.getAtom();
        Atomic parentAtom = parentQuery.getAtom();

        Map<VarName, VarName> unifiers = childAtom.getUnifiers(parentAtom);
        return answers.unify(unifiers).filterVars(parentQuery.getVarNames());
    }
}
