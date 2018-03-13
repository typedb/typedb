/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.graql.internal.query;

import ai.grakn.concept.Concept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.AnswerExplanation;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.explanation.Explanation;
import ai.grakn.graql.internal.reasoner.explanation.JoinExplanation;
import ai.grakn.graql.internal.reasoner.utils.Pair;
import ai.grakn.graql.internal.reasoner.utils.ReasonerUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

    private final ImmutableMap<Var, Concept> map;
    private final AnswerExplanation explanation;

    public QueryAnswer(){
        this.map = ImmutableMap.of();
        this.explanation = new Explanation();
    }

    public QueryAnswer(Answer a){
        this.map = ImmutableMap.<Var, Concept>builder().putAll(a.entrySet()).build();
        this.explanation = a.getExplanation();
    }

    public QueryAnswer(Collection<Map.Entry<Var, Concept>> mappings, AnswerExplanation exp){
        this.map = ImmutableMap.<Var, Concept>builder().putAll(mappings).build();
        this.explanation = exp;
    }

    public QueryAnswer(Map<Var, Concept> m, AnswerExplanation exp){
        this.map = ImmutableMap.copyOf(m);
        this.explanation = exp;
    }

    public QueryAnswer(Map<Var, Concept> m){
        this(m, new Explanation());
    }

    @Override
    public String toString(){
        return entrySet().stream()
                .sorted(Comparator.comparing(e -> e.getKey().getValue()))
                .map(e -> "[" + e.getKey() + "/" + e.getValue().getId() + "]").collect(Collectors.joining());
    }

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
    public ImmutableMap<Var, Concept> map() { return map;}

    @Override
    public Set<Var> vars(){ return map.keySet();}

    @Override
    public Collection<Concept> concepts(){ return map.values(); }

    @Override
    public Set<Map.Entry<Var, Concept>> entrySet(){ return map.entrySet();}

    @Override
    public Concept get(String var) {
        return get(Graql.var(var));
    }

    @Override
    public Concept get(Var var) {
        Concept concept = map.get(var);
        if (concept == null) throw GraqlQueryException.varNotInQuery(var);
        return concept;
    }

    @Override
    public boolean containsVar(Var var){ return map.containsKey(var);}

    @Override
    public boolean containsAll(Answer ans){ return map.entrySet().containsAll(ans.entrySet());}

    @Override
    public boolean isEmpty(){ return map.isEmpty();}

    @Override
    public int size(){ return map.size();}

    @Override
    public void forEach(BiConsumer<? super Var, ? super Concept> consumer) {
        map.forEach(consumer);
    }

    @Override
    public Answer merge(Answer a2, boolean mergeExplanation){
        if(a2.isEmpty()) return this;
        if(this.isEmpty()) return a2;

        Sets.SetView<Var> varUnion = Sets.union(this.vars(), a2.vars());
        Set<Var> varIntersection = Sets.intersection(this.vars(), a2.vars());
        Map<Var, Concept> entryMap = Sets.union(
                this.entrySet(),
                a2.entrySet()
        )
                .stream()
                .filter(e -> !varIntersection.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        varIntersection
                .forEach(var -> {
                    Concept concept = this.get(var);
                    Concept otherConcept = a2.get(var);
                    if (concept.equals(otherConcept)) entryMap.put(var, concept);
                    else {
                        if (concept.isSchemaConcept()
                                && otherConcept.isSchemaConcept()
                                && !ReasonerUtils.areDisjointTypes(concept.asSchemaConcept(), otherConcept.asSchemaConcept())) {
                            entryMap.put(
                                    var,
                                    Iterables.getOnlyElement(ReasonerUtils.topOrMeta(
                                            Sets.newHashSet(
                                                    concept.asSchemaConcept(),
                                                    otherConcept.asSchemaConcept())
                                            )
                                    )
                            );
                        }
                    }
                });
        if (!entryMap.keySet().equals(varUnion)) return new QueryAnswer();

        return new QueryAnswer(
                entryMap,
                mergeExplanation? this.mergeExplanation(a2) : this.getExplanation()
        );
    }

    public AnswerExplanation mergeExplanation(Answer toMerge) {
        Set<Answer> partialAnswers = new HashSet<>();
        if (this.getExplanation().isJoinExplanation()) this.getExplanation().getAnswers().forEach(partialAnswers::add);
        else partialAnswers.add(this);
        if (toMerge.getExplanation().isJoinExplanation()) toMerge.getExplanation().getAnswers().forEach(partialAnswers::add);
        else partialAnswers.add(toMerge);
        return new JoinExplanation(partialAnswers);
    }

    @Override
    public Answer merge(Answer a2){ return this.merge(a2, false);}

    @Override
    public Answer explain(AnswerExplanation exp){
        return new QueryAnswer(this.entrySet(), exp.childOf(this));
    }

    @Override
    public Answer project(Set<Var> vars) {
        return new QueryAnswer(
                this.entrySet().stream()
                        .filter(e -> vars.contains(e.getKey()))
                        .collect(Collectors.toSet()),
                this.getExplanation()
        );
    }

    @Override
    public Answer unify(Unifier unifier){
        if (unifier.isEmpty()) return this;
        Map<Var, Concept> unified = new HashMap<>();

        for(Map.Entry<Var, Concept> e : this.entrySet()){
            Var var = e.getKey();
            Concept con = e.getValue();
            Collection<Var> uvars = unifier.get(var);
            if (uvars.isEmpty() && !unifier.values().contains(var)) {
                Concept put = unified.put(var, con);
                if (put != null && !put.equals(con)) return new QueryAnswer();
            } else {
                for(Var uv : uvars){
                    Concept put = unified.put(uv, con);
                    if (put != null && !put.equals(con)) return new QueryAnswer();
                }
            }
        }
        return new QueryAnswer(unified, this.getExplanation());
    }

    @Override
    public Stream<Answer> unify(MultiUnifier multiUnifier) {
        return multiUnifier.stream().map(this::unify);
    }

    @Override
    public Stream<Answer> expandHierarchies(Set<Var> toExpand) {
        if (toExpand.isEmpty()) return Stream.of(this);
        List<Set<Pair<Var, Concept>>> entryOptions = entrySet().stream()
                .map(e -> {
                    Var var = e.getKey();
                    if (toExpand.contains(var)) {
                        Concept c = get(var);
                        if (c.isSchemaConcept()) {
                            return ReasonerUtils.upstreamHierarchy(c.asSchemaConcept()).stream()
                                    .map(r -> new Pair<Var, Concept>(var, r))
                                    .collect(Collectors.toSet());
                        }
                    }
                    return Collections.singleton(new Pair<>(var, get(var)));
                }).collect(Collectors.toList());

        return Sets.cartesianProduct(entryOptions).stream()
                .map(mappingList -> new QueryAnswer(mappingList.stream().collect(Collectors.toMap(Pair::getKey, Pair::getValue)), this.getExplanation()))
                .map(ans -> ans.explain(getExplanation()));
    }

    @Override
    public AnswerExplanation getExplanation(){ return explanation;}

    @Override
    public Set<Answer> getExplicitPath(){
        return getPartialAnswers().stream().filter(ans -> ans.getExplanation().isLookupExplanation()).collect(Collectors.toSet());
    }

    @Override
    public Set<Answer> getPartialAnswers(){
        Set<Answer> answers = Sets.newHashSet(this);
        this.getExplanation().getAnswers().forEach(ans -> ans.getPartialAnswers().forEach(answers::add));
        return answers;
    }

    @Override
    public Set<AnswerExplanation> getExplanations(){
        Set<AnswerExplanation> explanations = Sets.newHashSet(this.getExplanation());
        this.getExplanation().getAnswers().forEach(ans -> ans.getExplanations().forEach(explanations::add));
        return explanations;
    }

    @Override
    public Set<Atomic> toPredicates(ReasonerQuery parent) {
        Set<Var> varNames = parent.getVarNames();
        return entrySet().stream()
                .filter(e -> varNames.contains(e.getKey()))
                .map(e -> IdPredicate.create(e.getKey(), e.getValue(), parent))
                .collect(Collectors.toSet());
    }
}
