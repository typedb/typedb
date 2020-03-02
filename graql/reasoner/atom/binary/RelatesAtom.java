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
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import graql.lang.property.RelatesProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import javax.annotation.Nullable;


/**
 * TypeAtom corresponding to a graql RelatesProperty property.
 */
public class RelatesAtom extends OntologicalAtom {

    private RelatesAtom(Variable varName, Statement pattern, ReasonerQuery reasonerQuery, @Nullable Label label,
                        Variable predicateVariable, ReasoningContext ctx) {
        super(varName, pattern, reasonerQuery, label, predicateVariable, ctx);
    }

    public static RelatesAtom create(Variable var, Variable pVar, @Nullable Label label, ReasonerQuery parent, ReasoningContext ctx) {
        Variable varName = var.asReturnedVar();
        Variable predicateVar = pVar.asReturnedVar();
        return new RelatesAtom(varName, new Statement(varName).relates(new Statement(predicateVar)), parent, label, predicateVar, ctx);
    }

    private static RelatesAtom create(RelatesAtom a, ReasonerQuery parent) {
        return create(a.getVarName(), a.getPredicateVariable(), a.getTypeLabel(), parent, a.context());
    }

    @Override
    OntologicalAtom createSelf(Variable var, Variable predicateVar, @Nullable Label label, ReasonerQuery parent) {
        return RelatesAtom.create(var, predicateVar, label, parent, context());
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
