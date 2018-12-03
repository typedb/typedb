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

package grakn.core.graql.answer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.Explanation;
import grakn.core.graql.admin.MultiUnifier;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.admin.Unifier;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.Role;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.internal.reasoner.cache.SemanticDifference;
import grakn.core.graql.internal.reasoner.explanation.JoinExplanation;
import grakn.core.graql.internal.reasoner.explanation.QueryExplanation;
import grakn.core.graql.internal.reasoner.utils.Pair;
import grakn.core.graql.internal.reasoner.utils.ReasonerUtils;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Variable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.CheckReturnValue;


/**
 * <p>
 * Wrapper for a query result class {@link ConceptMap}.
 * </p>
 */
public class ConceptMap implements Answer<ConceptMap> {

    private final ImmutableMap<Variable, Concept> map;
    private final Explanation explanation;

    public ConceptMap() {
        this.map = ImmutableMap.of();
        this.explanation = new QueryExplanation();
    }

    public ConceptMap(ConceptMap map) {
        this(map.map().entrySet(), map.explanation());
    }

    public ConceptMap(Collection<Map.Entry<Variable, Concept>> mappings, Explanation exp) {
        this.map = ImmutableMap.<Variable, Concept>builder().putAll(mappings).build();
        this.explanation = exp;
    }

    public ConceptMap(Map<Variable, Concept> m, Explanation exp) {
        this(m.entrySet(), exp);
    }

    public ConceptMap(Map<Variable, Concept> m) {
        this(m, new QueryExplanation());
    }

    @Override
    public ConceptMap asConceptMap() {
        return this;
    }

    @Override
    public Explanation explanation() {
        return explanation;
    }

    @CheckReturnValue
    public ImmutableMap<Variable, Concept> map() {
        return map;
    }

    @CheckReturnValue
    public Set<Variable> vars() { return map.keySet();}

    @CheckReturnValue
    public Collection<Concept> concepts() { return map.values(); }

    /**
     * Return the {@link Concept} bound to the given variable name.
     *
     * @throws GraqlQueryException if the {@link Variable} is not in this {@link ConceptMap}
     */
    @CheckReturnValue
    public Concept get(String var) {
        return get(Pattern.var(var));
    }

    /**
     * Return the {@link Concept} bound to the given {@link Variable}.
     *
     * @throws GraqlQueryException if the {@link Variable} is not in this {@link ConceptMap}
     */
    @CheckReturnValue
    public Concept get(Variable var) {
        Concept concept = map.get(var);
        if (concept == null) throw GraqlQueryException.varNotInQuery(var);
        return concept;
    }

    @CheckReturnValue
    public boolean containsVar(Variable var) { return map.containsKey(var);}

    @CheckReturnValue
    public boolean containsAll(ConceptMap map) { return this.map.entrySet().containsAll(map.map().entrySet());}

    @CheckReturnValue
    public boolean isEmpty() { return map.isEmpty();}

    @CheckReturnValue
    public int size() { return map.size();}

    @Override
    public String toString() {
        return map.entrySet().stream()
                .sorted(Comparator.comparing(e -> e.getKey().name()))
                .map(e -> "[" + e.getKey() + "/" + e.getValue().id() + "]").collect(Collectors.joining());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ConceptMap a2 = (ConceptMap) obj;
        return map.equals(a2.map);
    }

    @Override
    public int hashCode() { return map.hashCode();}

    public void forEach(BiConsumer<Variable, Concept> consumer) {
        map.forEach(consumer);
    }

    /**
     * perform an answer merge with optional explanation
     * NB:assumes answers are compatible (concept corresponding to join vars if any are the same)
     *
     * @param map          answer to be merged with
     * @param mergeExplanation flag for providing explanation
     * @return merged answer
     */
    @CheckReturnValue
    public ConceptMap merge(ConceptMap map, boolean mergeExplanation) {
        if (map.isEmpty()) return this;
        if (this.isEmpty()) return map;

        Sets.SetView<Variable> varUnion = Sets.union(this.vars(), map.vars());
        Set<Variable> varIntersection = Sets.intersection(this.vars(), map.vars());
        Map<Variable, Concept> entryMap = Sets.union(
                this.map.entrySet(),
                map.map().entrySet()
        )
                .stream()
                .filter(e -> !varIntersection.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        varIntersection
                .forEach(var -> {
                    Concept concept = this.get(var);
                    Concept otherConcept = map.get(var);
                    if (concept.equals(otherConcept)) entryMap.put(var, concept);
                    else {
                        if (concept.isSchemaConcept()
                                && otherConcept.isSchemaConcept()
                                && !ReasonerUtils.areDisjointTypes(concept.asSchemaConcept(), otherConcept.asSchemaConcept(), false)) {
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
        if (!entryMap.keySet().equals(varUnion)) return new ConceptMap();

        return new ConceptMap(
                entryMap,
                mergeExplanation ? this.mergeExplanation(map) : this.explanation()
        );
    }

    private Explanation mergeExplanation(ConceptMap toMerge) {
        List<ConceptMap> partialAnswers = new ArrayList<>();
        if (this.explanation().isJoinExplanation()) partialAnswers.addAll(this.explanation().getAnswers());
        else partialAnswers.add(this);
        if (toMerge.explanation().isJoinExplanation()) partialAnswers.addAll(toMerge.explanation().getAnswers());
        else partialAnswers.add(toMerge);
        return new JoinExplanation(partialAnswers);
    }

    /**
     * perform an answer merge without explanation
     * NB:assumes answers are compatible (concept corresponding to join vars if any are the same)
     *
     * @param a2 answer to be merged with
     * @return merged answer
     */
    @CheckReturnValue
    public ConceptMap merge(ConceptMap a2) { return this.merge(a2, false);}

    /**
     * explain this answer by providing explanation with preserving the structure of dependent answers
     *
     * @param exp explanation for this answer
     * @return explained answer
     */
    public ConceptMap explain(Explanation exp) {
        return new ConceptMap(this.map.entrySet(), exp.childOf(this));
    }

    /**
     * @param vars variables defining the projection
     * @return project the answer retaining the requested variables
     */
    @CheckReturnValue
    public ConceptMap project(Set<Variable> vars) {
        return new ConceptMap(
                this.map.entrySet().stream()
                        .filter(e -> vars.contains(e.getKey()))
                        .collect(Collectors.toSet()),
                this.explanation()
        );
    }

    /**
     * @param partialSub partial child substitution that needs to be incorporated
     * @param vars child vars
     * @param unifier parent-child unifier
     * @param diff parent-child semantic difference
     * @return projected answer (empty if semantic difference not satisfied)
     */
    @CheckReturnValue
    public ConceptMap projectToChild(ConceptMap partialSub, Set<Variable> vars, Unifier unifier, SemanticDifference diff) {
        ConceptMap unified = this.unify(unifier);
        if (unified.isEmpty()) return unified;
        Set<Variable> varsToRetain = Sets.difference(unified.vars(), partialSub.vars());
        return diff.satisfiedBy(unified)?
                unified
                        .project(varsToRetain)
                        .merge(partialSub)
                        .project(vars) :
                new ConceptMap();
    }

    /**
     * @param unifier set of mappings between variables
     * @return unified answer
     */
    @CheckReturnValue
    public ConceptMap unify(Unifier unifier) {
        if (unifier.isEmpty()) return this;
        Map<Variable, Concept> unified = new HashMap<>();

        for (Map.Entry<Variable, Concept> e : this.map.entrySet()) {
            Variable var = e.getKey();
            Concept con = e.getValue();
            Collection<Variable> uvars = unifier.get(var);
            if (uvars.isEmpty() && !unifier.values().contains(var)) {
                Concept put = unified.put(var, con);
                if (put != null && !put.equals(con)) return new ConceptMap();
            } else {
                for (Variable uv : uvars) {
                    Concept put = unified.put(uv, con);
                    if (put != null && !put.equals(con)) return new ConceptMap();
                }
            }
        }
        return new ConceptMap(unified, this.explanation());
    }

    /**
     * @param multiUnifier set of unifiers defining variable mappings
     * @return stream of unified answers
     */
    @CheckReturnValue
    public Stream<ConceptMap> unify(MultiUnifier multiUnifier) {
        return multiUnifier.stream().map(this::unify);
    }

    /**
     * @param toExpand set of variables for which {@link Role} hierarchy should be expanded
     * @return stream of answers with expanded role hierarchy
     */
    @CheckReturnValue
    public Stream<ConceptMap> expandHierarchies(Set<Variable> toExpand) {
        if (toExpand.isEmpty()) return Stream.of(this);
        List<Set<Pair<Variable, Concept>>> entryOptions = map.entrySet().stream()
                .map(e -> {
                    Variable var = e.getKey();
                    if (toExpand.contains(var)) {
                        Concept c = get(var);
                        if (c.isSchemaConcept()) {
                            return ReasonerUtils.upstreamHierarchy(c.asSchemaConcept()).stream()
                                    .map(r -> new Pair<Variable, Concept>(var, r))
                                    .collect(Collectors.toSet());
                        }
                    }
                    return Collections.singleton(new Pair<>(var, get(var)));
                }).collect(Collectors.toList());

        return Sets.cartesianProduct(entryOptions).stream()
                .map(mappingList -> new ConceptMap(mappingList.stream().collect(Collectors.toMap(Pair::getKey, Pair::getValue)), this.explanation()))
                .map(ans -> ans.explain(explanation()));
    }

    /**
     * @param parent query context
     * @return (partial) set of predicates corresponding to this answer
     */
    @CheckReturnValue
    public Set<Atomic> toPredicates(ReasonerQuery parent) {
        Set<Variable> varNames = parent.getVarNames();
        return map.entrySet().stream()
                .filter(e -> varNames.contains(e.getKey()))
                .map(e -> IdPredicate.create(e.getKey(), e.getValue().id(), parent))
                .collect(Collectors.toSet());
    }
}
