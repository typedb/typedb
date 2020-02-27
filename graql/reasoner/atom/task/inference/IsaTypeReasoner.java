/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graql.reasoner.atom.task.inference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.util.ConceptUtils;
import grakn.core.graql.reasoner.atom.binary.Binary;
import grakn.core.graql.reasoner.atom.binary.IsaAtom;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptManager;
import graql.lang.statement.Variable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class IsaTypeReasoner implements TypeReasoner<IsaAtom> {

    private final ConceptManager conceptManager;

    public IsaTypeReasoner(ConceptManager conceptManager){
        this.conceptManager = conceptManager;
    }

    @Override
    public IsaAtom inferTypes(IsaAtom atom, ConceptMap sub) {
        if (atom.getTypePredicate() != null) return atom;
        if (sub.containsVar(atom.getPredicateVariable())) return atom.addType(sub.get(atom.getPredicateVariable()).asType());
        return atom;
    }

    @Override
    public ImmutableList<Type> inferPossibleTypes(IsaAtom atom, ConceptMap sub){
        if (atom.getSchemaConcept() != null) return ImmutableList.of(atom.getSchemaConcept().asType());
        if (sub.containsVar(atom.getPredicateVariable())) return ImmutableList.of(sub.get(atom.getPredicateVariable()).asType());

        Variable varName = atom.getVarName();
        //determine compatible types from played roles
        Set<Type> typesFromRoles = atom.getParentQuery().getAtoms(RelationAtom.class)
                .filter(r -> r.getVarNames().contains(varName))
                .flatMap(r -> r.getRoleVarMap().entries().stream()
                        .filter(e -> e.getValue().equals(varName))
                        .map(Map.Entry::getKey))
                .map(role -> role.players().collect(Collectors.toSet()))
                .reduce(Sets::intersection)
                .orElse(Sets.newHashSet());

        Set<Type> typesFromTypes = atom.getParentQuery().getAtoms(IsaAtom.class)
                .filter(at -> at.getVarNames().contains(varName))
                .filter(at -> at != atom)
                .map(Binary::getSchemaConcept)
                .filter(Objects::nonNull)
                .filter(Concept::isType)
                .map(Concept::asType)
                .collect(Collectors.toSet());

        Set<Type> types = typesFromTypes.isEmpty()?
                typesFromRoles :
                typesFromRoles.isEmpty()? typesFromTypes: Sets.intersection(typesFromRoles, typesFromTypes);

        return !types.isEmpty()?
                ImmutableList.copyOf(ConceptUtils.top(types)) :
                conceptManager.getMetaConcept().subs().collect(ImmutableList.toImmutableList());
    }
}
