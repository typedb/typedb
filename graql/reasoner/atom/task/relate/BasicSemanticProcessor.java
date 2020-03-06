/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.graql.reasoner.atom.task.relate;

import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.reasoner.cache.SemanticDifference;
import grakn.core.graql.reasoner.cache.VariableDefinition;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.unifier.MultiUnifier;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.statement.Variable;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

public class BasicSemanticProcessor implements SemanticProcessor<Atom>{

    @Override
    public Unifier getUnifier(Atom childAtom, Atom parentAtom, UnifierType unifierType, ReasoningContext ctx){
        throw new IllegalArgumentException();
    }

    @Override
    public MultiUnifier getMultiUnifier(Atom childAtom, Atom parentAtom, UnifierType unifierType, ReasoningContext ctx) {
        throw new IllegalArgumentException();
    }

    /**
     * Calculates the semantic difference between the this (parent) and child atom,
     * that needs to be applied on A(P) to find the subset belonging to A(C).
     *
     * @param childAtom child atom
     * @param unifier    parent->child unifier
     * @return semantic difference between this and child defined in terms of this variables
     */
    @Override
    public SemanticDifference computeSemanticDifference(Atom parentAtom, Atom childAtom, Unifier unifier, ReasoningContext ctx){
        Set<VariableDefinition> diff = new HashSet<>();
        Unifier unifierInverse = unifier.inverse();

        unifier.mappings().forEach(m -> {
            Variable parentVar = m.getKey();
            Variable childVar = m.getValue();

            Type parentType = parentAtom.getParentQuery().getUnambiguousType(parentVar, false);
            Type childType = childAtom.getParentQuery().getUnambiguousType(childVar, false);
            Type requiredType = childType != null ?
                    parentType != null ?
                            (!parentType.equals(childType) ? childType : null) :
                            childType
                    : null;

            Set<ValuePredicate> predicatesToSatisfy = childAtom.getPredicates(childVar, ValuePredicate.class)
                    .flatMap(vp -> vp.unify(unifierInverse).stream()).collect(toSet());
            parentAtom.getPredicates(parentVar, ValuePredicate.class).forEach(predicatesToSatisfy::remove);

            diff.add(new VariableDefinition(parentVar, requiredType, null, new HashSet<>(), predicatesToSatisfy));
        });
        return new SemanticDifference(diff);
    }

    /**
     *
     * @param parentAtom atom wrt which we check the compatibility
     * @param unifier mappings between this (child) and parent variables
     * @param unifierType unifier type in question
     * @return true if predicates between this (child) and parent are compatible based on the mappings provided by unifier
     */
    boolean isPredicateCompatible(Atom childAtom, Atom parentAtom, Unifier unifier, UnifierType unifierType, ConceptManager conceptManager){
        //check value predicates compatibility
        return unifier.mappings().stream().allMatch(mapping -> {
            Variable childVar = mapping.getKey();
            Variable parentVar = mapping.getValue();
            Set<Atomic> parentIdPredicates = parentAtom.getPredicates(parentVar, IdPredicate.class).collect(Collectors.toSet());
            Set<Atomic> childIdPredicates = childAtom.getPredicates(childVar, IdPredicate.class).collect(Collectors.toSet());
            Set<Atomic> parentValuePredicates = parentAtom.getAllPredicates(parentVar, ValuePredicate.class).collect(Collectors.toSet());
            Set<Atomic> childValuePredicates = childAtom.getAllPredicates(childVar, ValuePredicate.class).collect(Collectors.toSet());

            if (unifierType.inferValues()) {
                parentAtom.getParentQuery().getAtoms(IdPredicate.class)
                        .filter(id -> id.getVarName().equals(parentVar))
                        .map(id -> id.toValuePredicate(conceptManager))
                        .filter(Objects::nonNull)
                        .forEach(parentValuePredicates::add);
                childAtom.getParentQuery().getAtoms(IdPredicate.class)
                        .filter(id -> id.getVarName().equals(childVar))
                        .map(id -> id.toValuePredicate(conceptManager))
                        .filter(Objects::nonNull)
                        .forEach(childValuePredicates::add);
            }

            return unifierType.idCompatibility(parentIdPredicates, childIdPredicates)
                    && unifierType.valueCompatibility(parentValuePredicates, childValuePredicates);
        });
    }
}
