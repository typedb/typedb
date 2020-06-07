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

package grakn.core.graql.reasoner.atom;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.statement.Variable;

import java.util.Objects;
import javax.annotation.CheckReturnValue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Helpers for handling and converting to and from Atoms
 */
public class AtomicUtil {
    /**
     * @param parent query context
     * @return (partial) set of predicates corresponding to this answer
     */
    @CheckReturnValue
    public static Set<Atomic> answerToPredicates(ConceptMap answer, ReasonerQuery parent) {
        Set<Variable> varNames = parent.getVarNames();
        return answer.map().entrySet().stream()
                .filter(e -> varNames.contains(e.getKey()))
                .map(e -> IdPredicate.create(e.getKey(), e.getValue().id(), parent))
                .collect(Collectors.toSet());
    }

    /**
     *
     * @param parentAtom atom wrt which we check the compatibility
     * @param unifier mappings between this (child) and parent variables
     * @param unifierType unifier type in question
     * @return true if predicates between this (child) and parent are compatible based on the mappings provided by unifier
     */
    @CheckReturnValue
    public static boolean isPredicateCompatible(Atom childAtom, Atom parentAtom, Unifier unifier, UnifierType unifierType, ConceptManager conceptManager){
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
