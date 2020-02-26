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

import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.cache.QueryCache;
import grakn.core.kb.graql.reasoner.cache.RuleCache;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import graql.lang.Graql;
import graql.lang.property.HasAttributeTypeProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import javax.annotation.Nullable;

/**
 * TypeAtom corresponding to graql a HasAttributeTypeProperty property.
 */
public class HasAtom extends OntologicalAtom {

    private HasAtom(Variable varName,
                    Statement pattern,
                    ReasonerQuery parentQuery,
                    @Nullable ConceptId typeId,
                    Variable predicateVariable,
                    ReasonerQueryFactory queryFactory, ConceptManager conceptManager, QueryCache queryCache, RuleCache ruleCache) {
        super(varName, pattern, parentQuery, typeId, predicateVariable, queryFactory, conceptManager, queryCache, ruleCache);
    }

    public static HasAtom create(Variable var, Variable pVar, ConceptId predicateId, ReasonerQuery parent,
                                 ReasonerQueryFactory queryFactory, ConceptManager conceptManager, QueryCache queryCache, RuleCache ruleCache) {
        Variable varName = var.asReturnedVar();
        Variable predicateVar = pVar.asReturnedVar();
        Label label = conceptManager.getConcept(predicateId).asType().label();
        return new HasAtom(varName, new Statement(varName).has(Graql.type(label.getValue())), parent, predicateId, predicateVar,
                queryFactory, conceptManager, queryCache, ruleCache);
    }

    private static HasAtom create(HasAtom a, ReasonerQuery parent) {
        return create(a.getVarName(), a.getPredicateVariable(), a.getTypeId(), parent,
                a.queryFactory, a.conceptManager, a.queryCache, a.ruleCache);
    }

    @Override
    OntologicalAtom createSelf(Variable var, Variable predicateVar, ConceptId predicateId, ReasonerQuery parent) {
        return HasAtom.create(var, predicateVar, predicateId, parent, queryFactory, conceptManager, queryCache, ruleCache);
    }

    @Override
    public Atomic copy(ReasonerQuery parent){ return create(this, parent); }

    @Override
    public Class<? extends VarProperty> getVarPropertyClass() { return HasAttributeTypeProperty.class;}
}
