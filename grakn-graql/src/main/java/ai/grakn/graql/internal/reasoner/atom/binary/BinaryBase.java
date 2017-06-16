/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.UnifierImpl;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 *
 * <p>
 * Base implementation for binary atoms of the type ($varName, $valueVariable), where value variable
 * references predicates.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class BinaryBase extends Atom {
    private Var valueVariable;

    BinaryBase(VarPatternAdmin pattern, ReasonerQuery par) {
        super(pattern, par);
        this.valueVariable = extractValueVariableName(pattern);
    }

    BinaryBase(BinaryBase a) {
        super(a);
        this.valueVariable = a.getValueVariable();
    }

    protected abstract Var extractValueVariableName(VarPatternAdmin var);
    protected abstract boolean hasEquivalentPredicatesWith(BinaryBase atom);

    public Var getValueVariable() {
        return valueVariable;
    }
    void setValueVariable(Var var) { valueVariable = var;}

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + (this.getTypeId() != null? this.getTypeId().hashCode() : 0);
        hashCode = hashCode * 37 + this.getVarName().hashCode();
        hashCode = hashCode * 37 + this.valueVariable.hashCode();
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        BinaryBase a2 = (BinaryBase) obj;
        return Objects.equals(this.getTypeId(), a2.getTypeId())
                && this.getVarName().equals(a2.getVarName())
                && this.valueVariable.equals(a2.getValueVariable());
    }

    @Override
    public boolean isEquivalent(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        BinaryBase a2 = (BinaryBase) obj;
        return Objects.equals(this.getTypeId(), a2.getTypeId())
                && hasEquivalentPredicatesWith(a2);
    }

    @Override
    public Set<Var> getVarNames() {
        Set<Var> vars = new HashSet<>();
        if (isUserDefinedName()) vars.add(getVarName());
        if (!valueVariable.getValue().isEmpty()) vars.add(valueVariable);
        return vars;
    }

    @Override
    public Unifier getUnifier(Atom parentAtom) {
        if (!(parentAtom instanceof BinaryBase)) {
            throw GraqlQueryException.unificationAtomIncompatibility();
        }

        Unifier unifier = new UnifierImpl();
        Var childValVarName = this.getValueVariable();
        Var parentValVarName = parentAtom.getValueVariable();

        if (parentAtom.isUserDefinedName()){
            Var childVarName = this.getVarName();
            Var parentVarName = parentAtom.getVarName();
            unifier.addMapping(childVarName, parentVarName);
        }
        if (!childValVarName.getValue().isEmpty()
                && !parentValVarName.getValue().isEmpty()){
            unifier.addMapping(childValVarName, parentValVarName);
        }
        return unifier;
    }

}
