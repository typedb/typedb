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

import ai.grakn.concept.ConceptId;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.UnifierImpl;
import ai.grakn.util.ErrorMessage;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static ai.grakn.graql.internal.reasoner.ReasonerUtils.capture;

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

    protected BinaryBase(VarPatternAdmin pattern, ReasonerQuery par) {
        super(pattern, par);
        this.valueVariable = extractValueVariableName(pattern);
    }

    protected BinaryBase(BinaryBase a) {
        super(a);
        this.valueVariable = a.getValueVariable();
        this.typeId = a.getTypeId() != null? ConceptId.of(a.getTypeId().getValue()) : null;
    }

    protected abstract Var extractValueVariableName(VarPatternAdmin var);
    protected abstract boolean hasEquivalentPredicatesWith(BinaryBase atom);

    public Var getValueVariable() {
        return valueVariable;
    }
    protected void setValueVariable(Var var) {
        valueVariable = var;
    }

    @Override
    public boolean isBinary() { return true;}

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + (this.typeId != null? this.typeId.hashCode() : 0);
        hashCode = hashCode * 37 + this.varName.hashCode();
        hashCode = hashCode * 37 + this.valueVariable.hashCode();
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        BinaryBase a2 = (BinaryBase) obj;
        return Objects.equals(this.typeId, a2.getTypeId())
                && this.varName.equals(a2.getVarName())
                && this.valueVariable.equals(a2.getValueVariable());
    }

    @Override
    public boolean isEquivalent(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        BinaryBase a2 = (BinaryBase) obj;
        return Objects.equals(this.typeId, a2.getTypeId())
                && hasEquivalentPredicatesWith(a2);
    }

    /**
     * @return set of atoms that are (potentially indirectly) linked to this atom via valueVariable
     */
    public Set<Atom> getLinkedAtoms(){
        Set<Atom> atoms = new HashSet<>();
        getParentQuery().getAtoms().stream()
                .filter(Atomic::isAtom).map(atom -> (Atom) atom)
                .filter(Atom::isBinary).map(atom -> (BinaryBase) atom)
                .filter(atom -> atom.getVarName().equals(valueVariable))
                .forEach(atom -> {
                    atoms.add(atom);
                    atoms.addAll(atom.getLinkedAtoms());
                });
        return atoms;
    }

    @Override
    public Set<Var> getVarNames() {
        Set<Var> vars = new HashSet<>();
        if (isUserDefinedName()) vars.add(getVarName());
        if (!valueVariable.getValue().isEmpty()) vars.add(valueVariable);
        return vars;
    }

    @Override
    public Atomic unify (Unifier unifier) {
        super.unify(unifier);
        Var var = valueVariable;
        if (unifier.containsKey(var)) {
            setValueVariable(unifier.get(var).iterator().next());
        } else if (unifier.containsValue(var)) {
            setValueVariable(capture(var));
        }
        return this;
    }

    @Override
    public Unifier getUnifier(Atomic parentAtom) {
        if (!(parentAtom instanceof BinaryBase)) {
            throw new IllegalArgumentException(ErrorMessage.UNIFICATION_ATOM_INCOMPATIBILITY.getMessage());
        }

        Unifier unifier = new UnifierImpl();
        Var childValVarName = this.getValueVariable();
        Var parentValVarName = ((BinaryBase) parentAtom).getValueVariable();

        if (parentAtom.isUserDefinedName()){
            Var childVarName = this.getVarName();
            Var parentVarName = parentAtom.getVarName();
            if (!childVarName.equals(parentVarName)) {
                unifier.addMapping(childVarName, parentVarName);
            }
        }
        if (!childValVarName.getValue().isEmpty()
                && !parentValVarName.getValue().isEmpty()
                && !childValVarName.equals(parentValVarName)) {
            unifier.addMapping(childValVarName, parentValVarName);
        }
        return unifier;
    }
}
