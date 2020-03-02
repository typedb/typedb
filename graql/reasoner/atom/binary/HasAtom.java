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

package grakn.core.graql.reasoner.atom.binary;

import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.graql.reasoner.atom.Atomic;
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
                    @Nullable Label label,
                    Variable predicateVariable,
                    ReasoningContext ctx) {
        super(varName, pattern, parentQuery, label, predicateVariable, ctx);
    }

    public static HasAtom create(Variable var, Variable pVar, @Nullable Label label, ReasonerQuery parent, ReasoningContext ctx) {
        Variable varName = var.asReturnedVar();
        Variable predicateVar = pVar.asReturnedVar();
        return new HasAtom(varName, new Statement(varName).has(Graql.type(label.getValue())), parent, label, predicateVar, ctx);
    }

    private static HasAtom create(TypeAtom a, ReasonerQuery parent) {
        return create(a.getVarName(), a.getPredicateVariable(), a.getTypeLabel(), parent, a.context());
    }

    @Override
    OntologicalAtom createSelf(Variable var, Variable predicateVar, @Nullable Label label, ReasonerQuery parent) {
        return HasAtom.create(var, predicateVar, label, parent, context());
    }

    @Override
    public Atomic copy(ReasonerQuery parent){ return create(this, parent); }

    @Override
    public Class<? extends VarProperty> getVarPropertyClass() { return HasAttributeTypeProperty.class;}
}
