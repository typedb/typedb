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
import grakn.core.graql.internal.reasoner.atom.Atomic;
import grakn.core.graql.internal.reasoner.query.ReasonerQuery;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.query.pattern.statement.Statement;
import grakn.core.graql.query.pattern.statement.Variable;
import grakn.core.graql.query.pattern.property.PlaysProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

/**
 * TypeAtom corresponding to graql a {@link PlaysProperty} property.
 */
@AutoValue
public abstract class PlaysAtom extends OntologicalAtom {

    @Override public abstract Variable getPredicateVariable();
    @Override public abstract Statement getPattern();
    @Override public abstract ReasonerQuery getParentQuery();

    public static PlaysAtom create(Variable var, Variable predicateVar, ConceptId predicateId, ReasonerQuery parent) {
        return new AutoValue_PlaysAtom(var, predicateId, predicateVar, new Statement(var).plays(new Statement(predicateVar)), parent);
    }

    private static PlaysAtom create(PlaysAtom a, ReasonerQuery parent) {
        return create(a.getVarName(), a.getPredicateVariable(), a.getTypeId(), parent);
    }

    @Override
    OntologicalAtom createSelf(Variable var, Variable predicateVar, ConceptId predicateId, ReasonerQuery parent) {
        return PlaysAtom.create(var, predicateVar, predicateId, parent);
    }

    @Override
    public Atomic copy(ReasonerQuery parent){
        return create(this, parent);
    }

    @Override
    public Class<? extends VarProperty> getVarPropertyClass() {return PlaysProperty.class;}
}
