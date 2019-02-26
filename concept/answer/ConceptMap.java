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

package grakn.core.concept.answer;

import grakn.core.concept.Concept;
import grakn.core.concept.exception.GraknConceptException;
import graql.lang.statement.Variable;

import javax.annotation.CheckReturnValue;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;


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
        this(map.map, map.explanation);
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

    @CheckReturnValue
    public Concept get(String var) {
        return get(new Variable(var));
    }

    @CheckReturnValue
    public Concept get(Variable var) {
        Concept concept = map.get(var);
        if (concept == null) throw GraknConceptException.variableDoesNotExist(var.toString());
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
}
