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
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.NotEquals;
import ai.grakn.graql.internal.reasoner.atom.binary.Relation;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;

import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.reasoner.Utility.getListPermutations;
import static ai.grakn.graql.internal.reasoner.Utility.getUnifiersFromPermutations;

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
public class QueryAnswers extends HashSet<Map<String, Concept>> {

    public QueryAnswers(){super();}
    public QueryAnswers(Collection<? extends Map<String, Concept>> ans){ super(ans);}

    private Set<String> getVars(){
        Optional<Map<String, Concept>> map = this.stream().findFirst();
        return map.isPresent()? map.get().keySet() : new HashSet<>();
    }

    /**
     *
     * @param atom atom which roles are to be permuted
     * @param headAtom rule head atom which answers we are permuting, only needed when atom is a match all atom
     * @return permuted answers
     */
    public QueryAnswers permute(Atom atom, Atom headAtom){
        if (!(atom.isRelation() && headAtom.isRelation())) return this;
        List<String> permuteVars = new ArrayList<>();
        //if atom is match all atom, add type from rule head and find unmapped roles
        Relation relAtom = atom.getValueVariable().isEmpty()?
                ((Relation) AtomicFactory.create(atom, atom.getParentQuery())).addType(headAtom.getType()) :
                (Relation) atom;
        relAtom.getUnmappedRolePlayers().forEach(permuteVars::add);

        List<List<String>> varPermutations = getListPermutations(new ArrayList<>(permuteVars));
        Set<Map<String, String>> unifierSet = getUnifiersFromPermutations(permuteVars, varPermutations);
        QueryAnswers permutedAnswers = new QueryAnswers();
        unifierSet.forEach(unifiers -> permutedAnswers.addAll(this.unify(unifiers)));

        return permutedAnswers
                .filterBySubstitutions(atom)
                .filterByEntityTypes(atom);
    }

    /**
     * filter answers by constraining the variable set to the provided one
     * @param vars set of variable names
     * @return filtered answers
     */
    public QueryAnswers filterVars(Set<String> vars) {
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
            Iterator<Map<String, Concept>> it = known.iterator();
            while(it.hasNext() && !isKnown) {
                Map<String, Concept> knownAnswer = it.next();
                isKnown = knownAnswer.entrySet().containsAll(answer.entrySet());
            }
            if (!isKnown) results.add(answer);
        });
        return results;
    }

    /**
     * filter answers by applying NonEquals filters
     * @param query query containing filters
     * @return filtered answers
     */
    public QueryAnswers filterNonEquals(ReasonerQueryImpl query){
        Set<NotEquals> filters = query.getAtoms().stream()
                .filter(at -> at.getClass() == NotEquals.class)
                .map(at -> (NotEquals) at)
                .collect(Collectors.toSet());
        if(filters.isEmpty()) return this;
        QueryAnswers results = new QueryAnswers(this);
        for (NotEquals filter : filters) results = filter.filter(results);
        return results;
    }

    public QueryAnswers filterBySubstitutions(Atom parent){
        if(!parent.isRelation()) return this;
        Relation atom = (Relation) parent;
        Set<String> unmappedVars = atom.getUnmappedRolePlayers();
        //filter by checking substitutions
        Set<IdPredicate> subs = atom.getIdPredicates().stream()
                .filter(pred -> unmappedVars.contains(pred.getVarName()))
                .collect(Collectors.toSet());
        if (subs.isEmpty()) return this;

        QueryAnswers results = new QueryAnswers(this);
        subs.forEach( sub -> this.stream()
                .filter(answer -> !answer.get(sub.getVarName()).getId().equals(sub.getPredicate()))
                .forEach(results::remove));
        return results;
    }

    public QueryAnswers filterByEntityTypes(Atom parent){
        if(!parent.isRelation()) return this;
        Relation atom = (Relation) parent;
        Set<String> unmappedVars = atom.getUnmappedRolePlayers();
        Map<String, Type> varTypeMap = atom.getParentQuery().getVarTypeMap();
        Map<String, Type> filterMap = unmappedVars.stream()
                .filter(varTypeMap::containsKey)
                .filter(v -> Objects.nonNull(varTypeMap.get(v)))
                .collect(Collectors.toMap(v -> v, varTypeMap::get));
        if (filterMap.isEmpty()) return this;

        QueryAnswers results = new QueryAnswers();
        this.forEach(answer -> {
            boolean isCompatible = true;
            Iterator<Map.Entry<String, Type>> it = filterMap.entrySet().iterator();
            while( it.hasNext() && isCompatible){
                Map.Entry<String, Type> entry = it.next();
                isCompatible = answer.get(entry.getKey()).asInstance().type().equals(entry.getValue());
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
        Set<String> joinVars = new HashSet<>(this.getVars());
        joinVars.retainAll(localTuples.getVars());

        for( Map<String, Concept> lanswer : localTuples){
            for (Map<String, Concept> answer : this){
                boolean isCompatible = true;
                Iterator<String> vit = joinVars.iterator();
                while(vit.hasNext() && isCompatible) {
                    String var = vit.next();
                    isCompatible = answer.get(var).equals(lanswer.get(var));
                }

                if (isCompatible) {
                    Map<String, Concept> merged = new HashMap<>(lanswer);
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
    public QueryAnswers unify(Map<String, String> unifiers){
        if (unifiers.isEmpty()) return new QueryAnswers(this);
        QueryAnswers unifiedAnswers = new QueryAnswers();
        this.forEach(answer -> {
            Map<String, Concept> unifiedAnswer = answer.entrySet().stream()
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

        Map<String, String> unifiers = childAtom.getUnifiers(parentAtom);
        return answers.unify(unifiers).filterVars(parentQuery.getVarNames());
    }
}
