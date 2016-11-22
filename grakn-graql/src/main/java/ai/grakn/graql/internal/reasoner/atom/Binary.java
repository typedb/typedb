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

package ai.grakn.graql.internal.reasoner.atom;

import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import com.google.common.collect.Sets;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.reasoner.query.Query;
import ai.grakn.util.ErrorMessage;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class Binary extends Atom{

    private Predicate predicate = null;
    protected String valueVariable;

    public Binary(VarAdmin pattern) { this(pattern, null);}
    public Binary(VarAdmin pattern, Query par) {
        super(pattern, par);
        this.valueVariable = extractValueVariableName(pattern);
        this.predicate = getPredicate();
    }

    public Binary(Binary a) {
        super(a);
        this.valueVariable = extractValueVariableName(a.getPattern().asVar());
        this.predicate = a.getPredicate();
    }

    protected abstract String extractValueVariableName(VarAdmin var);

    public Predicate getPredicate(){
        if (predicate != null) return predicate;
        else
            return getParentQuery() != null ? getParentQuery().getAtoms().stream()
                .filter(Atomic::isPredicate).map(at -> (Predicate) at)
                .filter(at -> at.getVarName().equals(valueVariable)).findFirst().orElse(null) : null;
    }

    private boolean predicatesEquivalent(Binary atom){
        Predicate pred = getPredicate();
        Predicate objPredicate = atom.getPredicate();
        return (pred  == null && objPredicate == null)
            || ((pred  != null && objPredicate != null) && pred.isEquivalent(objPredicate));
    }

    @Override
    public boolean isBinary(){ return true;}

    @Override
    protected boolean isRuleApplicable(InferenceRule child) {
        //TODO rule applicability for types should be disabled
        Atom ruleAtom = child.getHead().getAtom();
        return (ruleAtom instanceof Binary) && this.getType().equals(ruleAtom.getType());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        Binary a2 = (Binary) obj;
        return this.typeId.equals(a2.getTypeId()) && this.varName.equals(a2.getVarName())
                && this.valueVariable.equals(a2.getValueVariable());
    }

    @Override
    public boolean isEquivalent(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        Binary a2 = (Binary) obj;
        return this.typeId.equals(a2.getTypeId())
                && predicatesEquivalent(a2);
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        hashCode = hashCode * 37 + this.varName.hashCode();
        hashCode = hashCode * 37 + this.valueVariable.hashCode();
        return hashCode;
    }

    @Override
    public int equivalenceHashCode(){
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        hashCode = hashCode * 37 + (predicate != null? predicate.equivalenceHashCode() : 0);
        return hashCode;
    }

    @Override
    public Set<Predicate> getPredicates(){
        return getParentQuery().getAtoms().stream()
                .filter(Atomic::isPredicate).map(atom -> (Predicate) atom)
                .filter(atom -> atom.getVarName().equals(valueVariable)).collect(Collectors.toSet());
    }

    public String getValueVariable(){ return valueVariable;}

    public Set<Atom> getLinkedAtoms(){
        Set<Atom> atoms = new HashSet<>();
        getParentQuery().getAtoms().stream()
                .filter(Atomic::isAtom).map(atom -> (Atom) atom)
                .filter(Atom::isBinary).map(atom -> (Binary) atom)
                .filter(atom -> atom.getVarName().equals(valueVariable))
                .forEach(atom -> {
                    atoms.add(atom);
                    atoms.addAll(atom.getLinkedAtoms());
                });
        return atoms;
    }

    protected abstract void setValueVariable(String var);

    @Override
    public Set<String> getVarNames() {
        Set<String> varNames = Sets.newHashSet(getVarName());
        if (!valueVariable.isEmpty()) varNames.add(valueVariable);
        return varNames;
    }

    @Override
    public void unify(String from, String to) {
        super.unify(from, to);
        String var = valueVariable;
        if (var.equals(from)) {
            setValueVariable(to);
        } else if (var.equals(to)) {
            setValueVariable("captured->" + var);
        }
    }

    @Override
    public void unify (Map<String, String> unifiers) {
        super.unify(unifiers);
        String var = valueVariable;
        if (unifiers.containsKey(var)) {
            setValueVariable(unifiers.get(var));
        }
        else if (unifiers.containsValue(var)) {
            setValueVariable("captured->" + var);
        }
    }

    @Override
    public Map<String, String> getUnifiers(Atomic parentAtom) {
        if (!(parentAtom instanceof Binary))
            throw new IllegalArgumentException(ErrorMessage.UNIFICATION_ATOM_INCOMPATIBILITY.getMessage());

        Map<String, String> unifiers = new HashMap<>();
        String childVarName = this.getVarName();
        String parentVarName = parentAtom.getVarName();
        String childValVarName = this.getValueVariable();
        String parentValVarName = ((Binary) parentAtom).getValueVariable();

        if (!childVarName.equals(parentVarName)) unifiers.put(childVarName, parentVarName);
        if (!childValVarName.equals(parentValVarName)) unifiers.put(childValVarName, parentValVarName);
        return unifiers;
    }
}
