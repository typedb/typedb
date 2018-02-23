/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.atom.binary;

import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.pattern.property.PlaysProperty;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import com.google.auto.value.AutoValue;

/**
 *
 * <p>
 * TypeAtom corresponding to graql a {@link ai.grakn.graql.internal.pattern.property.PlaysProperty} property.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
@AutoValue
public abstract class PlaysAtom extends OntologicalAtom {

    public static PlaysAtom create(Var var, Var predicateVar, IdPredicate predicate, ReasonerQuery parent) {
        PlaysAtom atom = new AutoValue_PlaysAtom(var, var.plays(predicateVar), predicateVar, predicate);
        atom.parent = parent;
        return atom;
    }

    private static PlaysAtom create(PlaysAtom a, ReasonerQuery parent) {
        return create(a.getVarName(), a.getPredicateVariable(), a.getTypePredicate(), parent);
    }

    @Override
    public Atomic copy(ReasonerQuery parent){
        return create(this, parent);
    }

    @Override
    public Class<? extends VarProperty> getVarPropertyClass() {return PlaysProperty.class;}
}
