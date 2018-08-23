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

package ai.grakn.graql.internal.reasoner.atom.predicate;

import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.property.NeqProperty;

import ai.grakn.graql.internal.reasoner.utils.IgnoreHashEquals;
import com.google.auto.value.AutoValue;

/**
 *
 * <p>
 * Predicate implementation specialising it to be an inequality predicate. Corresponds to graql {@link NeqProperty}.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
@AutoValue
public abstract class NeqIdPredicate extends NeqPredicate {

    @Override @IgnoreHashEquals public abstract VarPattern getPattern();
    @Override @IgnoreHashEquals public abstract ReasonerQuery getParentQuery();
    //need to have it explicitly here cause autovalue gets confused with the generic
    public abstract Var getPredicate();

    public static NeqPredicate create(VarPattern pattern, ReasonerQuery parent) {
        return new AutoValue_NeqIdPredicate(pattern.admin().var(), pattern, parent, extractPredicate(pattern));
    }
    public static NeqPredicate create(Var varName, NeqProperty prop, ReasonerQuery parent) {
        VarPatternAdmin pattern = varName.neq(prop.var().var()).admin();
        return create(pattern, parent);
    }
    public static NeqPredicate create(NeqPredicate a, ReasonerQuery parent) {
        return create(a.getPattern(), parent);
    }

    private static Var extractPredicate(VarPattern pattern) {
        return pattern.admin().getProperties(NeqProperty.class).iterator().next().var().var();
    }

    @Override
    public Atomic copy(ReasonerQuery parent) { return create(this, parent);}

    @Override
    public String toString(){
        IdPredicate idPredicate = this.getIdPredicate(this.getVarName());
        IdPredicate refIdPredicate = this.getIdPredicate(this.getPredicate());
        return "[" + getVarName() + "!=" + getPredicate() + "]" +
                (idPredicate != null? idPredicate : "" ) +
                (refIdPredicate != null? refIdPredicate : "");
    }

    @Override
    public boolean isSatisfied(ConceptMap sub) {
        return sub.containsVar(getVarName())
                && sub.containsVar(getPredicate())
                && sub.get(getVarName()).equals(sub.get(getPredicate()));
    }
}
