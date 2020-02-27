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

import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import graql.lang.property.RelatesProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;


/**
 * TypeAtom corresponding to a graql RelatesProperty property.
 */
public class RelatesAtom extends OntologicalAtom {

    private RelatesAtom(Variable varName, Statement pattern, ReasonerQuery reasonerQuery, ConceptId typeId,
                        Variable predicateVariable, ReasoningContext ctx) {
        super(varName, pattern, reasonerQuery, typeId, predicateVariable, ctx);
    }

    public static RelatesAtom create(Variable var, Variable pVar, ConceptId predicateId, ReasonerQuery parent, ReasoningContext ctx) {
        Variable varName = var.asReturnedVar();
        Variable predicateVar = pVar.asReturnedVar();
        return new RelatesAtom(varName, new Statement(varName).relates(new Statement(predicateVar)), parent, predicateId, predicateVar, ctx);
    }

    private static RelatesAtom create(RelatesAtom a, ReasonerQuery parent) {
        return create(a.getVarName(), a.getPredicateVariable(), a.getTypeId(), parent, a.context());
    }

    @Override
    OntologicalAtom createSelf(Variable var, Variable predicateVar, ConceptId predicateId, ReasonerQuery parent) {
        return RelatesAtom.create(var, predicateVar, predicateId, parent, context());
    }

    @Override
    public Atomic copy(ReasonerQuery parent) {
        return create(this, parent);
    }

    @Override
    public Class<? extends VarProperty> getVarPropertyClass() {
        return RelatesProperty.class;
    }
}
