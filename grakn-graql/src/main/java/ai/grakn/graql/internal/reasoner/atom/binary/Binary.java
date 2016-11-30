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

import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.Atomic;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.reasoner.query.Query;
import ai.grakn.util.ErrorMessage;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class Binary extends Atom {

    private Predicate predicate = null;
    private String valueVariable;

    public Binary(VarAdmin pattern, Predicate p, Query par) {
        super(pattern, par);
        this.valueVariable = extractValueVariableName(pattern);
        this.predicate = p;
        this.typeId = extractTypeId(atomPattern.asVar());
    }

    public Binary(Binary a) {
        super(a);
        this.valueVariable = extractValueVariableName(a.getPattern().asVar());
        this.predicate = a.getPredicate() != null ? (Predicate) AtomicFactory.create(a.getPredicate(), getParentQuery()) : null;
        this.typeId = extractTypeId(atomPattern.asVar());
    }

    @Override
    public void setParentQuery(Query q) {
        super.setParentQuery(q);
        if (predicate != null) predicate.setParentQuery(q);
    }

    protected abstract String extractTypeId(VarAdmin var);

    protected abstract String extractValueVariableName(VarAdmin var);

    public Predicate getPredicate() {
        return predicate;
    }

    protected void setPredicate(Predicate p) {
        predicate = p;
    }

    private boolean predicatesEquivalent(Binary atom) {
        Predicate pred = getPredicate();
        Predicate objPredicate = atom.getPredicate();
        return (pred == null && objPredicate == null)
                || ((pred != null && objPredicate != null) && pred.isEquivalent(objPredicate));
    }

    @Override
    public boolean isBinary() {
        return true;
    }

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
    public int equivalenceHashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        hashCode = hashCode * 37 + (predicate != null ? predicate.equivalenceHashCode() : 0);
        return hashCode;
    }

    public String getValueVariable() {
        return valueVariable;
    }
    protected void setValueVariable(String var) {
        valueVariable = var;
    }

    @Override
    public boolean isValueUserDefinedName() {
        return predicate == null;
    }

    @Override
    public Set<Predicate> getIdPredicates() {
        //direct predicates
        return getParentQuery().getIdPredicates().stream()
                .filter(atom -> containsVar(atom.getVarName()))
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Predicate> getValuePredicates(){ return new HashSet<>();}

    @Override
    public Set<Predicate> getPredicates() {
        Set<Predicate> predicates = getValuePredicates();
        predicates.addAll(getIdPredicates());
        return predicates;
    }

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

    @Override
    public Set<String> getVarNames() {
        Set<String> vars = new HashSet<>();
        if (isUserDefinedName()) vars.add(getVarName());
        if (!valueVariable.isEmpty()) vars.add(valueVariable);
        return vars;
    }

    @Override
    public Set<String> getSelectedNames(){
        Set<String> vars = super.getSelectedNames();
        if(isUserDefinedName()) vars.add(getVarName());
        if(isValueUserDefinedName()) vars.add(getValueVariable());
        return vars;
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
        if (predicate != null) predicate.unify(from, to);
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
        if (predicate != null) predicate.unify(unifiers);
    }

    @Override
    public Map<String, String> getUnifiers(Atomic parentAtom) {
        if (!(parentAtom instanceof Binary))
            throw new IllegalArgumentException(ErrorMessage.UNIFICATION_ATOM_INCOMPATIBILITY.getMessage());

        Map<String, String> unifiers = new HashMap<>();
        String childValVarName = this.getValueVariable();
        String parentValVarName = ((Binary) parentAtom).getValueVariable();

        if (parentAtom.isUserDefinedName()){
            String childVarName = this.getVarName();
            String parentVarName = parentAtom.getVarName();
            if (!childVarName.equals(parentVarName))
                unifiers.put(childVarName, parentVarName);
        }
        if (!childValVarName.equals(parentValVarName)) unifiers.put(childValVarName, parentValVarName);
        return unifiers;
    }
}
