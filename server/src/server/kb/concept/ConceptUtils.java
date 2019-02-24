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

package grakn.core.server.kb.concept;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import grakn.core.concept.Concept;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.server.kb.Schema;
import graql.lang.statement.Variable;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

public class ConceptUtils {
    /**
     * @param schemaConcepts entry {@link SchemaConcept} set
     * @return top non-meta {@link SchemaConcept}s from within the provided set
     */
    public static <T extends SchemaConcept> Set<T> top(Set<T> schemaConcepts) {
        return schemaConcepts.stream()
                .filter(t -> Sets.intersection(nonMetaSups(t), schemaConcepts).isEmpty())
                .collect(toSet());
    }

    /**
     * @param schemaConcepts entry {@link SchemaConcept} set
     * @return bottom non-meta {@link SchemaConcept}s from within the provided set
     */
    public static <T extends SchemaConcept> Set<T> bottom(Set<T> schemaConcepts) {
        return schemaConcepts.stream()
                .filter(t -> Sets.intersection(t.subs().filter(t2 -> !t.equals(t2)).collect(toSet()), schemaConcepts).isEmpty())
                .collect(toSet());
    }

    /**
     * @param schemaConcepts entry {@link SchemaConcept} set
     * @return top {@link SchemaConcept}s from within the provided set or meta concept if it exists
     */
    public static <T extends SchemaConcept> Set<T> topOrMeta(Set<T> schemaConcepts) {
        Set<T> concepts = top(schemaConcepts);
        T meta = concepts.stream()
                .filter(c -> Schema.MetaSchema.isMetaLabel(c.label()))
                .findFirst().orElse(null);
        return meta != null ? Collections.singleton(meta) : concepts;
    }

    /**
     * @param schemaConcept input type
     * @return set of all non-meta super types of the role
     */
    public static Set<? extends SchemaConcept> nonMetaSups(SchemaConcept schemaConcept){
        Set<SchemaConcept> superTypes = new HashSet<>();
        SchemaConcept superType = schemaConcept.sup();
        while(superType != null && !Schema.MetaSchema.isMetaLabel(superType.label())) {
            superTypes.add(superType);
            superType = superType.sup();
        }
        return superTypes;
    }

    /**
     * @param parent type
     * @param child type
     * @param direct flag indicating whether only direct types should be considered
     * @return true if child is a subtype of parent
     */
    public static boolean typesCompatible(SchemaConcept parent, SchemaConcept child, boolean direct) {
        if (parent == null ) return true;
        if (child == null) return false;
        if (direct) return parent.equals(child);
        if (Schema.MetaSchema.isMetaLabel(parent.label())) return true;
        SchemaConcept superType = child;
        while(superType != null && !Schema.MetaSchema.isMetaLabel(superType.label())){
            if (superType.equals(parent)) return true;
            superType = superType.sup();
        }
        return false;
    }

    /** determines disjointness of parent-child types, parent defines the bound on the child
     * @param parent {@link SchemaConcept}
     * @param child {@link SchemaConcept}
     * @return true if types do not belong to the same type hierarchy, also true if parent is null and false if parent non-null and child null
     */
    public static boolean areDisjointTypes(SchemaConcept parent, SchemaConcept child, boolean direct) {
        return parent != null && child == null || !typesCompatible(parent, child, direct) && !typesCompatible(child, parent, direct);
    }

    /**
     * perform an answer merge with optional explanation
     * NB:assumes answers are compatible (concept corresponding to join vars if any are the same)
     *
     * @return merged answer
     */
    public static ConceptMap mergeAnswers(ConceptMap answerA, ConceptMap answerB) {
        if (answerB.isEmpty()) return answerA;
        if (answerA.isEmpty()) return answerB;

        Sets.SetView<Variable> varUnion = Sets.union(answerA.vars(), answerB.vars());
        Set<Variable> varIntersection = Sets.intersection(answerA.vars(), answerB.vars());
        Map<Variable, Concept> entryMap = Sets.union(
                answerA.map().entrySet(),
                answerB.map().entrySet()
        )
                .stream()
                .filter(e -> !varIntersection.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        varIntersection
                .forEach(var -> {
                    Concept concept = answerA.get(var);
                    Concept otherConcept = answerB.get(var);
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
        return new ConceptMap(entryMap, answerA.explanation());
    }
}
