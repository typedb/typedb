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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.binary.Binary;
import grakn.core.graql.reasoner.cache.SemanticDifference;
import grakn.core.graql.reasoner.unifier.UnifierImpl;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.reasoner.unifier.MultiUnifier;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.statement.Variable;

import java.util.Collections;
import java.util.Set;

public class BinarySemanticProcessor implements SemanticProcessor<Binary> {

    private final BasicSemanticProcessor basicSemanticProcessor = new BasicSemanticProcessor();

    @Override
    public Unifier getUnifier(Binary childAtom, Atom parentAtom, UnifierType unifierType, ReasoningContext ctx) {
        boolean inferTypes = unifierType.inferTypes();
        Variable childVarName = childAtom.getVarName();
        Variable parentVarName = parentAtom.getVarName();
        Variable childPredicateVarName = childAtom.getPredicateVariable();
        Variable parentPredicateVarName = parentAtom.getPredicateVariable();
        Set<Type> parentTypes = parentAtom.getParentQuery().getVarTypeMap(inferTypes).get(parentAtom.getVarName());
        Set<Type> childTypes = childAtom.getParentQuery().getVarTypeMap(inferTypes).get(childAtom.getVarName());

        ConceptManager conceptManager = ctx.conceptManager();
        SchemaConcept parentType = parentAtom.getSchemaConcept();
        SchemaConcept childType = childAtom.getSchemaConcept();

        //check for incompatibilities
        if( !unifierType.typeCompatibility(
                parentType != null? Collections.singleton(parentType) : Collections.emptySet(),
                childType != null? Collections.singleton(childType) : Collections.emptySet())
                || !unifierType.typeCompatibility(parentTypes, childTypes)
                || !unifierType.typePlayabilityWithInsertSemantics(childAtom, childAtom.getVarName(), parentTypes)
                || !unifierType.typeDirectednessCompatibility(parentAtom, childAtom)){
            return UnifierImpl.nonExistent();
        }

        Multimap<Variable, Variable> varMappings = HashMultimap.create();

        if (parentVarName.isReturned()) {
            varMappings.put(childVarName, parentVarName);
        }
        if (parentPredicateVarName.isReturned()) {
            varMappings.put(childPredicateVarName, parentPredicateVarName);
        }

        UnifierImpl unifier = new UnifierImpl(varMappings);
        return basicSemanticProcessor.isPredicateCompatible(childAtom, parentAtom, unifier, unifierType, conceptManager)?
                unifier : UnifierImpl.nonExistent();
    }

    @Override
    public MultiUnifier getMultiUnifier(Binary childAtom, Atom parentAtom, UnifierType unifierType, ReasoningContext ctx) {
        return basicSemanticProcessor.getMultiUnifier(childAtom, parentAtom, unifierType, ctx);
    }

    @Override
    public SemanticDifference computeSemanticDifference(Binary parentAtom, Atom childAtom, Unifier unifier, ReasoningContext ctx) {
        return basicSemanticProcessor.computeSemanticDifference(parentAtom, childAtom, unifier, ctx);
    }
}
