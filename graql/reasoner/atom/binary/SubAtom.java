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

package grakn.core.graql.reasoner.atom.binary;

import grakn.core.graql.reasoner.atom.predicate.Predicate;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.cache.RuleCache;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import graql.lang.property.SubProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import javax.annotation.Nullable;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * TypeAtom corresponding to graql a SubProperty property.
 * </p>
 *
 *
 */
public class SubAtom extends OntologicalAtom {

    SubAtom(ConceptManager conceptManager,
            RuleCache ruleCache,
            Variable varName,
            Statement pattern,
            ReasonerQuery parentQuery,
            @Nullable ConceptId typeId,
            Variable predicateVariable) {
        super(conceptManager, ruleCache, varName, pattern, parentQuery, typeId, predicateVariable);
    }


    public static SubAtom create(ConceptManager conceptManager, RuleCache ruleCache, Variable var, Variable pVar, ConceptId predicateId, ReasonerQuery parent) {
        Variable varName = var.asReturnedVar();
        Variable predicateVar = pVar.asReturnedVar();
        return new SubAtom(conceptManager, ruleCache, varName, new Statement(varName).sub(new Statement(predicateVar)), parent, predicateId, predicateVar);
    }
    /**
     * copy constructor
     */
    private static SubAtom create(SubAtom a, ReasonerQuery parent) {
        return create(a.conceptManager, a.ruleCache, a.getVarName(), a.getPredicateVariable(), a.getTypeId(), parent);
    }

    @Override
    OntologicalAtom createSelf(Variable var, Variable predicateVar, ConceptId predicateId, ReasonerQuery parent) {
        return create(conceptManager, ruleCache, var, predicateVar, predicateId, parent);
    }


    @Override
    public Atomic copy(ReasonerQuery parent){
        return create(this, parent);
    }

    @Override
    public Class<? extends VarProperty> getVarPropertyClass() {return SubProperty.class;}

    @Override
    public String toString(){
        String typeString = "sub"+ "(" + getVarName() + ", " + getPredicateVariable() +")";
        return typeString + getPredicates().map(Predicate::toString).collect(Collectors.joining(""));
    }
}
