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

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.ConceptUtils;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.reasoner.unifier.MultiUnifier;
import grakn.core.graql.internal.reasoner.unifier.Unifier;
import graql.lang.exception.GraqlException;
import graql.lang.statement.Variable;
import java.util.AbstractMap;
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
 * A type of Answer object that contains a Map of Concepts.
 */
public class ConceptMap extends Answer {

    private final Map<Variable, Concept> map;
    private final Explanation explanation;

    public ConceptMap() {
        this.map = Collections.emptyMap();
        this.explanation = new Explanation();
    }

    public ConceptMap(ConceptMap map) {
        this(map.map(), map.explanation());
    }

    public ConceptMap(Map<Variable, Concept> map, Explanation exp) {
        this.map = Collections.unmodifiableMap(map);
        this.explanation = exp;
    }

    public ConceptMap(Map<Variable, Concept> m) {
        this(m, new Explanation());
    }

    @Override
    public Explanation explanation() {
        return explanation;
    }

    @CheckReturnValue
    public Map<Variable, Concept> map() {
        return map;
    }

    @CheckReturnValue
    public Set<Variable> vars() { return map.keySet();}

    @CheckReturnValue
    public Collection<Concept> concepts() { return map.values(); }

    /**
     * Return the Concept bound to the given variable name.
     *
     * @throws GraqlQueryException if the Variable is not in this ConceptMap
     */
    @CheckReturnValue
    public Concept get(String var) {
        return get(new Variable(var));
    }

    /**
     * Return the Concept bound to the given Variable.
     *
     * @throws GraqlQueryException if the Variable is not in this ConceptMap
     */
    @CheckReturnValue
    public Concept get(Variable var) {
        Concept concept = map.get(var);
        if (concept == null) throw GraqlException.variableOutOfScope(var.toString());
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
     * explain this answer by providing explanation with preserving the structure of dependent answers
     *
     * @param exp explanation for this answer
     * @return explained answer
     */
    public ConceptMap explain(Explanation exp) {
        return new ConceptMap(this.map, exp.childOf(this));
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
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
                this.explanation()
        );
    }

    /**
     * perform an answer merge with optional explanation
     * NB:assumes answers are compatible (concept corresponding to join vars if any are the same)
     *
     * @param map              answer to be merged with
     * @return merged answer
     */
    @CheckReturnValue
    public ConceptMap merge(ConceptMap map) {
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
                                && !ConceptUtils.areDisjointTypes(concept.asSchemaConcept(), otherConcept.asSchemaConcept(), false)) {
                            entryMap.put(
                                    var,
                                    Iterables.getOnlyElement(ConceptUtils.topOrMeta(
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
        return new ConceptMap(entryMap, this.explanation());
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
     * @param toExpand set of variables for which Role hierarchy should be expanded
     * @return stream of answers with expanded role hierarchy
     */
    @CheckReturnValue
    public Stream<ConceptMap> expandHierarchies(Set<Variable> toExpand) {
        if (toExpand.isEmpty()) return Stream.of(this);
        List<Set<AbstractMap.SimpleImmutableEntry<Variable, Concept>>> entryOptions = map.entrySet().stream()
                .map(e -> {
                    Variable var = e.getKey();
                    Concept concept = get(var);
                    if (toExpand.contains(var)) {
                        if (concept.isSchemaConcept()) {
                            return concept.asSchemaConcept().sups()
                                    .map(sup -> new AbstractMap.SimpleImmutableEntry<>(var, (Concept) sup))
                                    .collect(Collectors.toSet());
                        }
                    }
                    return Collections.singleton(new AbstractMap.SimpleImmutableEntry<>(var, concept));
                }).collect(Collectors.toList());

        return Sets.cartesianProduct(entryOptions).stream()
                .map(mappingList -> new ConceptMap(
                        mappingList.stream().collect(Collectors.toMap(AbstractMap.SimpleImmutableEntry::getKey, AbstractMap.SimpleImmutableEntry::getValue)), this.explanation()))
                .map(ans -> ans.explain(explanation()));
    }
}
