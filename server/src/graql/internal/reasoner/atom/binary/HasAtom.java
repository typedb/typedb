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

package grakn.core.graql.internal.reasoner.atom.binary;

import com.google.auto.value.AutoValue;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Label;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.pattern.statement.Statement;
import grakn.core.graql.query.pattern.statement.Variable;
import grakn.core.graql.query.pattern.property.HasAttributeTypeProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

/**
 * TypeAtom corresponding to graql a {@link HasAttributeTypeProperty} property.
 */
@AutoValue
public abstract class HasAtom extends OntologicalAtom {

    @Override public abstract Variable getPredicateVariable();
    @Override public abstract Statement getPattern();
    @Override public abstract ReasonerQuery getParentQuery();

    public static HasAtom create(Statement pattern, Variable predicateVar, ConceptId predicateId, ReasonerQuery parent) {
        return new AutoValue_HasAtom(pattern.var(), predicateId, predicateVar, pattern, parent);
    }

    public static HasAtom create(Variable var, Variable predicateVar, ConceptId predicateId, ReasonerQuery parent) {
        Label label = parent.tx().getConcept(predicateId).asType().label();
        return create(new Statement(var).has(Graql.type(label.getValue())), predicateVar, predicateId, parent);
    }

    private static HasAtom create(TypeAtom a, ReasonerQuery parent) {
        return create(a.getVarName(), a.getPredicateVariable(), a.getTypeId(), parent);
    }

    @Override
    OntologicalAtom createSelf(Variable var, Variable predicateVar, ConceptId predicateId, ReasonerQuery parent) {
        return HasAtom.create(var, predicateVar, predicateId, parent);
    }

    @Override
    public Atomic copy(ReasonerQuery parent){ return create(this, parent); }

    @Override
    public Class<? extends VarProperty> getVarPropertyClass() { return HasAttributeTypeProperty.class;}
}
