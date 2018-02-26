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

import ai.grakn.concept.ConceptId;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.pattern.property.SubProperty;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.utils.autovalue.IgnoreHashEquals;
import com.google.auto.value.AutoValue;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * TypeAtom corresponding to graql a {@link ai.grakn.graql.internal.pattern.property.SubProperty} property.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
@AutoValue
public abstract class SubAtom extends OntologicalAtom {

    @Override @IgnoreHashEquals public abstract Var getPredicateVariable();
    @Override @IgnoreHashEquals public abstract VarPattern getPattern();
    @Override @IgnoreHashEquals public abstract ReasonerQuery getParentQuery();

    public static SubAtom create(Var var, Var predicateVar, ConceptId predicateId, ReasonerQuery parent) {
        return new AutoValue_SubAtom(var, predicateId, predicateVar, var.sub(predicateVar), parent);
    }

    private static SubAtom create(SubAtom a, ReasonerQuery parent) {
        return create(a.getVarName(), a.getPredicateVariable(), a.getTypeId(), parent);
    }

    @Override
    OntologicalAtom createSelf(Var var, Var predicateVar, ConceptId predicateId, ReasonerQuery parent) {
        return SubAtom.create(var, predicateVar, predicateId, parent);
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
