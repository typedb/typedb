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

package ai.grakn.graql.internal.reasoner.atom.binary;

import ai.grakn.concept.ConceptId;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.pattern.property.RelatesProperty;
import com.google.auto.value.AutoValue;


/**
 *
 * <p>
 * TypeAtom corresponding to a graql {@link ai.grakn.graql.internal.pattern.property.RelatesProperty} property.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
@AutoValue
public abstract class RelatesAtom extends OntologicalAtom {

    @Override public abstract Var getPredicateVariable();
    @Override public abstract VarPattern getPattern();
    @Override public abstract ReasonerQuery getParentQuery();

    public static RelatesAtom create(Var var, Var predicateVar, ConceptId predicateId, ReasonerQuery parent) {
        return new AutoValue_RelatesAtom(var, predicateId, predicateVar, var.relates(predicateVar), parent);
    }

    private static RelatesAtom create(RelatesAtom a, ReasonerQuery parent) {
        return create(a.getVarName(), a.getPredicateVariable(), a.getTypeId(), parent);
    }

    @Override
    OntologicalAtom createSelf(Var var, Var predicateVar, ConceptId predicateId, ReasonerQuery parent) {
        return RelatesAtom.create(var, predicateVar, predicateId, parent);
    }

    @Override
    public Atomic copy(ReasonerQuery parent){
        return create(this, parent);
    }

    @Override
    public Class<? extends VarProperty> getVarPropertyClass() { return RelatesProperty.class;}
}
